"""
Database Proxy Server

Async TCP proxy that intercepts PostgreSQL wire protocol,
parses SQL statements, applies policies, and injects chaos.

This is the core component that sits between the agent and
the actual PostgreSQL database.
"""

import asyncio
import struct
from datetime import datetime
from typing import Callable
from uuid import UUID, uuid4

from structlog import get_logger

from chaostrace.control_plane.models.events import (
    EventType,
    PolicyAction,
    RiskLevel,
    SQLEvent,
    SQLType,
)
from chaostrace.db_proxy.chaos_hooks import ChaosHooks
from chaostrace.db_proxy.risk_scorer import RiskScorer
from chaostrace.db_proxy.sql_interceptor import SQLInterceptor

logger = get_logger(__name__)


class PostgresProtocol:
    """
    PostgreSQL wire protocol constants and parsing.
    
    Reference: https://www.postgresql.org/docs/current/protocol-message-formats.html
    """
    
    # Message type identifiers (first byte of message)
    QUERY = ord('Q')           # Simple query
    PARSE = ord('P')           # Extended query: parse
    BIND = ord('B')            # Extended query: bind
    EXECUTE = ord('E')         # Extended query: execute
    DESCRIBE = ord('D')        # Describe
    SYNC = ord('S')            # Sync
    TERMINATE = ord('X')       # Terminate
    PASSWORD = ord('p')        # Password response
    
    # Backend message types
    AUTHENTICATION = ord('R')
    PARAMETER_STATUS = ord('S')
    BACKEND_KEY = ord('K')
    READY_FOR_QUERY = ord('Z')
    COMMAND_COMPLETE = ord('C')
    DATA_ROW = ord('D')
    ROW_DESCRIPTION = ord('T')
    ERROR_RESPONSE = ord('E')
    NOTICE_RESPONSE = ord('N')
    
    @staticmethod
    def parse_startup_message(data: bytes) -> dict:
        """Parse the startup message from client."""
        if len(data) < 8:
            return {}
        
        length = struct.unpack("!I", data[:4])[0]
        version = struct.unpack("!I", data[4:8])[0]
        
        # Parse key-value pairs
        params = {}
        pos = 8
        while pos < len(data) - 1:
            # Find null terminator for key
            key_end = data.find(b'\x00', pos)
            if key_end == -1:
                break
            key = data[pos:key_end].decode('utf-8')
            pos = key_end + 1
            
            # Find null terminator for value
            val_end = data.find(b'\x00', pos)
            if val_end == -1:
                break
            value = data[pos:val_end].decode('utf-8')
            pos = val_end + 1
            
            if key:
                params[key] = value
        
        return {
            "length": length,
            "version": version,
            "params": params,
        }
    
    @staticmethod
    def parse_query_message(data: bytes) -> str | None:
        """Extract SQL from a Query message."""
        if len(data) < 5:
            return None
        
        if data[0] != PostgresProtocol.QUERY:
            return None
        
        # Length is next 4 bytes (big-endian, includes length field)
        length = struct.unpack("!I", data[1:5])[0]
        
        # SQL is the rest, null-terminated
        sql = data[5:4 + length - 1].decode('utf-8', errors='replace')
        return sql.rstrip('\x00')
    
    @staticmethod
    def create_error_response(
        severity: str = "ERROR",
        code: str = "42000",
        message: str = "Query blocked by policy",
    ) -> bytes:
        """Create a PostgreSQL error response message."""
        parts = [
            b'S' + severity.encode() + b'\x00',  # Severity
            b'C' + code.encode() + b'\x00',       # SQLSTATE code
            b'M' + message.encode() + b'\x00',    # Message
            b'\x00',  # Terminator
        ]
        
        body = b''.join(parts)
        length = len(body) + 4  # Include length field
        
        return struct.pack("!cI", b'E', length) + body
    
    @staticmethod
    def create_ready_for_query(status: str = 'I') -> bytes:
        """Create a ReadyForQuery message."""
        return struct.pack("!cIc", b'Z', 5, status.encode())


class DBProxyConnection:
    """
    Handles a single proxied connection.
    
    Intercepts SQL queries, applies policies, logs events,
    and can inject chaos based on the scheduler.
    """
    
    def __init__(
        self,
        client_reader: asyncio.StreamReader,
        client_writer: asyncio.StreamWriter,
        server_host: str,
        server_port: int,
        run_id: UUID,
        interceptor: SQLInterceptor,
        risk_scorer: RiskScorer,
        chaos_hooks: ChaosHooks | None = None,
        policy_evaluator: Callable | None = None,
        event_callback: Callable | None = None,
    ):
        """
        Initialize a proxy connection.
        
        Args:
            client_reader: Reader for client connection.
            client_writer: Writer for client connection.
            server_host: PostgreSQL server hostname.
            server_port: PostgreSQL server port.
            run_id: ID of the current test run.
            interceptor: SQL interceptor for parsing.
            risk_scorer: Risk scorer for assessment.
            chaos_hooks: Optional chaos hooks for injection.
            policy_evaluator: Optional policy evaluation function.
            event_callback: Callback for logging events.
        """
        self.client_reader = client_reader
        self.client_writer = client_writer
        self.server_host = server_host
        self.server_port = server_port
        self.run_id = run_id
        self.interceptor = interceptor
        self.risk_scorer = risk_scorer
        self.chaos_hooks = chaos_hooks
        self.policy_evaluator = policy_evaluator
        self.event_callback = event_callback
        
        self.server_reader: asyncio.StreamReader | None = None
        self.server_writer: asyncio.StreamWriter | None = None
        self.client_addr = client_writer.get_extra_info('peername')
        
        self._closed = False
        self._sql_count = 0
        
        logger.info(
            "proxy_connection_created",
            run_id=str(run_id),
            client_addr=self.client_addr,
        )
    
    async def handle(self) -> None:
        """Main handler for the proxied connection."""
        try:
            # Connect to PostgreSQL server
            self.server_reader, self.server_writer = await asyncio.open_connection(
                self.server_host,
                self.server_port,
            )
            
            logger.debug(
                "connected_to_postgres",
                server=f"{self.server_host}:{self.server_port}",
            )
            
            # Start bidirectional proxying
            await asyncio.gather(
                self._proxy_client_to_server(),
                self._proxy_server_to_client(),
            )
            
        except asyncio.CancelledError:
            logger.debug("proxy_connection_cancelled")
        except Exception as e:
            logger.exception(
                "proxy_connection_error",
                error=str(e),
            )
        finally:
            await self._close()
    
    async def _proxy_client_to_server(self) -> None:
        """Proxy data from client to PostgreSQL server."""
        try:
            while not self._closed:
                # Read data from client
                data = await self.client_reader.read(65536)
                if not data:
                    break
                
                # Check if this is a query message
                if data and data[0] == PostgresProtocol.QUERY:
                    result = await self._handle_query(data)
                    if result is False:
                        # Query was blocked, don't forward
                        continue
                    if isinstance(result, bytes):
                        # Modified data to forward
                        data = result
                
                # Apply any chaos latency
                if self.chaos_hooks:
                    latency = await self.chaos_hooks.get_current_latency()
                    if latency > 0:
                        await asyncio.sleep(latency / 1000.0)
                
                # Forward to server
                if self.server_writer:
                    self.server_writer.write(data)
                    await self.server_writer.drain()
                    
        except Exception as e:
            if not self._closed:
                logger.error("client_to_server_error", error=str(e))
    
    async def _proxy_server_to_client(self) -> None:
        """Proxy data from PostgreSQL server to client."""
        try:
            while not self._closed:
                if not self.server_reader:
                    break
                    
                data = await self.server_reader.read(65536)
                if not data:
                    break
                
                # Forward to client
                self.client_writer.write(data)
                await self.client_writer.drain()
                
        except Exception as e:
            if not self._closed:
                logger.error("server_to_client_error", error=str(e))
    
    async def _handle_query(self, data: bytes) -> bool | bytes:
        """
        Handle an intercepted SQL query.
        
        Returns:
            - True: Allow query to pass through
            - False: Block query (don't forward)
            - bytes: Modified data to forward
        """
        start_time = datetime.utcnow()
        sql = PostgresProtocol.parse_query_message(data)
        
        if not sql:
            return True
        
        self._sql_count += 1
        
        logger.debug(
            "sql_intercepted",
            sql_preview=sql[:100],
            count=self._sql_count,
        )
        
        # Parse the SQL
        parsed = self.interceptor.parse(sql)
        
        # Score the risk
        risk_assessment = self.risk_scorer.assess(parsed)
        
        # Evaluate policy
        policy_action = PolicyAction.ALLOW
        policy_rule = None
        violation_reason = None
        
        if self.policy_evaluator:
            eval_result = self.policy_evaluator(
                sql=sql,
                sql_type=parsed.sql_type,
                tables=parsed.tables,
                has_where=parsed.has_where_clause,
                estimated_rows=risk_assessment.rows_estimated,
                columns=parsed.columns,
            )
            
            if not eval_result.allowed:
                policy_action = PolicyAction.BLOCK
                policy_rule = ", ".join(eval_result.matched_rules[:3])
                violation_reason = "; ".join(eval_result.violation_reasons[:3])
            elif eval_result.flagged:
                policy_action = PolicyAction.ALLOW_FLAGGED
        
        # Calculate latency
        latency_ms = (datetime.utcnow() - start_time).total_seconds() * 1000
        
        # Create event
        event = SQLEvent(
            event_id=uuid4(),
            run_id=self.run_id,
            event_type=self._get_event_type(policy_action),
            statement=sql,
            statement_hash=parsed.statement_hash,
            sql_type=parsed.sql_type,
            tables=parsed.tables,
            has_where_clause=parsed.has_where_clause,
            risk_level=risk_assessment.risk_level,
            risk_factors=risk_assessment.risk_factors,
            rows_estimated=risk_assessment.rows_estimated,
            policy_action=policy_action,
            policy_rule_matched=policy_rule,
            violation_reason=violation_reason,
            latency_ms=latency_ms,
        )
        
        # Emit event
        if self.event_callback:
            await self.event_callback(event)
        
        # Block if policy says so
        if policy_action == PolicyAction.BLOCK:
            logger.warning(
                "sql_blocked",
                sql_preview=sql[:100],
                reason=violation_reason,
            )
            
            # Send error response to client
            error_msg = f"Query blocked: {violation_reason or 'Policy violation'}"
            error_response = PostgresProtocol.create_error_response(
                message=error_msg,
            )
            ready_response = PostgresProtocol.create_ready_for_query()
            
            self.client_writer.write(error_response + ready_response)
            await self.client_writer.drain()
            
            return False
        
        return True
    
    def _get_event_type(self, action: PolicyAction) -> EventType:
        """Map policy action to event type."""
        if action == PolicyAction.BLOCK:
            return EventType.SQL_BLOCKED
        elif action == PolicyAction.ALLOW_FLAGGED:
            return EventType.SQL_FLAGGED
        return EventType.SQL_ALLOWED
    
    async def _close(self) -> None:
        """Close all connections."""
        if self._closed:
            return
        
        self._closed = True
        
        try:
            self.client_writer.close()
            await self.client_writer.wait_closed()
        except Exception:
            pass
        
        try:
            if self.server_writer:
                self.server_writer.close()
                await self.server_writer.wait_closed()
        except Exception:
            pass
        
        logger.info(
            "proxy_connection_closed",
            sql_count=self._sql_count,
        )


class DBProxyServer:
    """
    TCP server that proxies PostgreSQL connections.
    
    Each connection is handled by a DBProxyConnection
    that intercepts SQL, applies policies, and injects chaos.
    
    Usage:
        server = DBProxyServer(
            listen_host="0.0.0.0",
            listen_port=5433,
            target_host="localhost",
            target_port=5432,
            run_id=uuid4(),
        )
        await server.start()
        # ... server runs ...
        await server.stop()
    """
    
    def __init__(
        self,
        listen_host: str = "0.0.0.0",
        listen_port: int = 5433,
        target_host: str = "localhost",
        target_port: int = 5432,
        run_id: UUID | None = None,
        policy_evaluator: Callable | None = None,
        event_callback: Callable | None = None,
    ):
        """
        Initialize the proxy server.
        
        Args:
            listen_host: Host to listen on.
            listen_port: Port to listen on.
            target_host: PostgreSQL host to forward to.
            target_port: PostgreSQL port to forward to.
            run_id: ID of the test run.
            policy_evaluator: Function to evaluate SQL against policies.
            event_callback: Callback for SQL events.
        """
        self.listen_host = listen_host
        self.listen_port = listen_port
        self.target_host = target_host
        self.target_port = target_port
        self.run_id = run_id or uuid4()
        self.policy_evaluator = policy_evaluator
        self.event_callback = event_callback
        
        self._server: asyncio.Server | None = None
        self._connections: list[DBProxyConnection] = []
        self._interceptor = SQLInterceptor()
        self._risk_scorer = RiskScorer()
        self._chaos_hooks = ChaosHooks()
        
        logger.info(
            "db_proxy_server_initialized",
            listen=f"{listen_host}:{listen_port}",
            target=f"{target_host}:{target_port}",
        )
    
    async def start(self) -> None:
        """Start the proxy server."""
        self._server = await asyncio.start_server(
            self._handle_client,
            self.listen_host,
            self.listen_port,
        )
        
        addr = self._server.sockets[0].getsockname()
        logger.info("db_proxy_server_started", address=addr)
        
        async with self._server:
            await self._server.serve_forever()
    
    async def _handle_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        """Handle a new client connection."""
        connection = DBProxyConnection(
            client_reader=reader,
            client_writer=writer,
            server_host=self.target_host,
            server_port=self.target_port,
            run_id=self.run_id,
            interceptor=self._interceptor,
            risk_scorer=self._risk_scorer,
            chaos_hooks=self._chaos_hooks,
            policy_evaluator=self.policy_evaluator,
            event_callback=self.event_callback,
        )
        
        self._connections.append(connection)
        
        try:
            await connection.handle()
        finally:
            if connection in self._connections:
                self._connections.remove(connection)
    
    async def stop(self) -> None:
        """Stop the proxy server."""
        if self._server:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
        
        # Clean up chaos hooks
        await self._chaos_hooks.cleanup()
        
        logger.info("db_proxy_server_stopped")
    
    @property
    def chaos_hooks(self) -> ChaosHooks:
        """Get the chaos hooks for external control."""
        return self._chaos_hooks


# For running standalone
async def main():
    """Run the proxy server standalone for testing."""
    import os
    
    server = DBProxyServer(
        listen_host="0.0.0.0",
        listen_port=int(os.getenv("PROXY_LISTEN_PORT", "5433")),
        target_host=os.getenv("POSTGRES_HOST", "localhost"),
        target_port=int(os.getenv("POSTGRES_PORT", "5432")),
    )
    
    try:
        await server.start()
    except KeyboardInterrupt:
        await server.stop()


if __name__ == "__main__":
    asyncio.run(main())
