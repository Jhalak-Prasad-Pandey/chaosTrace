"""
Chaos Hooks

Implementation of chaos actions that execute REAL operations
against the sandbox PostgreSQL database.

This module is the core of chaos injection - it actually modifies
the database state to test agent resilience.
"""

import asyncio
import os
from datetime import datetime
from typing import Any

import asyncpg
from structlog import get_logger

from chaostrace.control_plane.models.chaos import ChaosAction, ChaosActionType

logger = get_logger(__name__)


class ChaosHookError(Exception):
    """Error executing a chaos hook."""
    pass


class ChaosHooks:
    """
    Implements REAL chaos injection actions against PostgreSQL.
    
    Each method corresponds to a chaos action type and
    actually modifies the sandbox database.
    
    Usage:
        hooks = ChaosHooks()
        await hooks.connect("postgres", 5432, "sandbox", "sandbox_password", "sandbox")
        await hooks.execute(chaos_action)
        await hooks.close()
    """
    
    def __init__(self):
        """Initialize chaos hooks (connection established separately)."""
        self._pool: asyncpg.Pool | None = None
        self._active_locks: dict[str, asyncio.Task] = {}
        self._latency_ms: int = 0
        self._latency_end_time: datetime | None = None
        self._lock_connections: dict[str, asyncpg.Connection] = {}
        
        logger.info("chaos_hooks_initialized")
    
    async def connect(
        self,
        host: str | None = None,
        port: int | None = None,
        user: str | None = None,
        password: str | None = None,
        database: str | None = None,
    ) -> None:
        """
        Establish connection pool to PostgreSQL.
        
        Args:
            host: PostgreSQL host (default from env: POSTGRES_HOST)
            port: PostgreSQL port (default from env: POSTGRES_PORT)
            user: Database user (default: sandbox)
            password: Database password (default: sandbox_password)
            database: Database name (default: sandbox)
        """
        host = host or os.getenv("POSTGRES_HOST", "localhost")
        port = port or int(os.getenv("POSTGRES_PORT", "5432"))
        user = user or os.getenv("POSTGRES_USER", "sandbox")
        password = password or os.getenv("POSTGRES_PASSWORD", "sandbox_password")
        database = database or os.getenv("POSTGRES_DB", "sandbox")
        
        try:
            self._pool = await asyncpg.create_pool(
                host=host,
                port=port,
                user=user,
                password=password,
                database=database,
                min_size=2,
                max_size=10,
                command_timeout=60,
            )
            logger.info(
                "chaos_hooks_connected",
                host=host,
                port=port,
                database=database,
            )
        except Exception as e:
            logger.error("chaos_hooks_connection_failed", error=str(e))
            raise ChaosHookError(f"Failed to connect to PostgreSQL: {e}")
    
    async def execute(self, action: ChaosAction, context: dict = None) -> dict:
        """
        Execute a chaos action.
        
        Args:
            action: The chaos action to execute.
            context: Runtime context (e.g., current event data).
            
        Returns:
            dict: Result of the action execution.
        """
        context = context or {}
        
        # Resolve any template variables in action parameters
        resolved_action = self._resolve_templates(action, context)
        
        # Delay if specified
        if resolved_action.delay_seconds > 0:
            await asyncio.sleep(resolved_action.delay_seconds)
        
        # Execute based on action type
        handler = self._get_handler(resolved_action.type)
        if handler is None:
            raise ChaosHookError(f"Unknown action type: {resolved_action.type}")
        
        result = await handler(resolved_action, context)
        
        logger.info(
            "chaos_action_executed",
            action_type=resolved_action.type.value,
            table=resolved_action.table,
            duration=resolved_action.duration_seconds,
            result=result,
        )
        
        return result
    
    def _get_handler(self, action_type: ChaosActionType):
        """Get the handler method for an action type."""
        handlers = {
            ChaosActionType.LOCK_TABLE: self._lock_table,
            ChaosActionType.ADD_LATENCY: self._add_latency,
            ChaosActionType.SIMULATE_TIMEOUT: self._simulate_timeout,
            ChaosActionType.REVOKE_CREDENTIALS: self._revoke_credentials,
            ChaosActionType.RENAME_COLUMN: self._rename_column,
            ChaosActionType.CHANGE_COLUMN_TYPE: self._change_column_type,
            ChaosActionType.DROP_INDEX: self._drop_index,
            ChaosActionType.DISK_FULL: self._simulate_disk_full,
            ChaosActionType.MEMORY_PRESSURE: self._simulate_memory_pressure,
            ChaosActionType.CPU_THROTTLE: self._simulate_cpu_throttle,
            ChaosActionType.NETWORK_PARTITION: self._simulate_network_partition,
            ChaosActionType.PACKET_LOSS: self._simulate_packet_loss,
        }
        return handlers.get(action_type)
    
    def _resolve_templates(
        self,
        action: ChaosAction,
        context: dict,
    ) -> ChaosAction:
        """
        Resolve template variables in action parameters.
        
        Supports templates like:
        - {event.tables[0]}
        - {run.id}
        """
        def resolve(value: str) -> str:
            if not isinstance(value, str):
                return value
            
            # Simple template resolution
            if "{event.tables[0]}" in value:
                tables = context.get("event", {}).get("tables", [])
                if tables:
                    value = value.replace("{event.tables[0]}", tables[0])
            
            if "{run.id}" in value:
                run_id = context.get("run_id", "unknown")
                value = value.replace("{run.id}", str(run_id))
            
            return value
        
        # Create a copy with resolved values
        return ChaosAction(
            type=action.type,
            table=resolve(action.table) if action.table else None,
            column=resolve(action.column) if action.column else None,
            duration_seconds=action.duration_seconds,
            delay_seconds=action.delay_seconds,
            latency_ms=action.latency_ms,
            new_name=resolve(action.new_name) if action.new_name else None,
            new_type=action.new_type,
            percentage=action.percentage,
            parameters=action.parameters,
        )
    
    # =========================================================================
    # Database Chaos Handlers - REAL IMPLEMENTATIONS
    # =========================================================================
    
    async def _lock_table(
        self,
        action: ChaosAction,
        context: dict,
    ) -> dict:
        """
        Lock a table to simulate contention.
        
        Uses PostgreSQL's LOCK TABLE ... IN ACCESS EXCLUSIVE MODE
        This ACTUALLY locks the table, blocking all other access.
        """
        table = action.table
        duration = action.duration_seconds or 30
        
        if not table:
            raise ChaosHookError("lock_table requires 'table' parameter")
        
        if not self._pool:
            raise ChaosHookError("Not connected to database")
        
        logger.info(
            "chaos_lock_table_starting",
            table=table,
            duration_seconds=duration,
        )
        
        lock_key = f"lock_{table}"
        
        # We need a dedicated connection that stays open during the lock
        conn = await self._pool.acquire()
        self._lock_connections[lock_key] = conn
        
        async def hold_lock():
            try:
                # Start a transaction and lock the table
                async with conn.transaction():
                    await conn.execute(
                        f"LOCK TABLE {table} IN ACCESS EXCLUSIVE MODE"
                    )
                    logger.info(
                        "chaos_lock_acquired",
                        table=table,
                        duration_seconds=duration,
                    )
                    # Hold the lock for the specified duration
                    await asyncio.sleep(duration)
                    
                logger.info("chaos_lock_released", table=table)
            except asyncio.CancelledError:
                logger.info("chaos_lock_cancelled", table=table)
            except Exception as e:
                logger.error("chaos_lock_error", table=table, error=str(e))
            finally:
                # Release the dedicated connection
                if lock_key in self._lock_connections:
                    await self._pool.release(self._lock_connections.pop(lock_key))
                if lock_key in self._active_locks:
                    del self._active_locks[lock_key]
        
        task = asyncio.create_task(hold_lock())
        self._active_locks[lock_key] = task
        
        return {
            "status": "locked",
            "table": table,
            "duration_seconds": duration,
            "real_lock": True,
        }
    
    async def _add_latency(
        self,
        action: ChaosAction,
        context: dict,
    ) -> dict:
        """Add artificial latency to database operations."""
        latency_ms = action.latency_ms or 1000
        duration = action.duration_seconds or 60
        
        self._latency_ms = latency_ms
        self._latency_end_time = datetime.utcnow().timestamp() + duration
        
        logger.info(
            "chaos_latency_applied",
            latency_ms=latency_ms,
            duration_seconds=duration,
        )
        
        # Schedule latency removal
        async def remove_latency():
            await asyncio.sleep(duration)
            self._latency_ms = 0
            self._latency_end_time = None
            logger.info("chaos_latency_removed")
        
        asyncio.create_task(remove_latency())
        
        return {
            "status": "latency_added",
            "latency_ms": latency_ms,
            "duration_seconds": duration,
        }
    
    async def get_current_latency(self) -> int:
        """Get the current artificial latency in ms (0 if none)."""
        if self._latency_end_time is None:
            return 0
        
        if datetime.utcnow().timestamp() > self._latency_end_time:
            self._latency_ms = 0
            self._latency_end_time = None
            return 0
        
        return self._latency_ms
    
    async def _simulate_timeout(
        self,
        action: ChaosAction,
        context: dict,
    ) -> dict:
        """
        Simulate a connection timeout error.
        
        Sets a flag that the proxy will use to return timeout errors.
        """
        logger.info("chaos_timeout_simulated")
        
        return {
            "status": "timeout_simulated",
            "should_return_error": True,
            "error_message": "connection timeout: server closed the connection unexpectedly",
        }
    
    async def _revoke_credentials(
        self,
        action: ChaosAction,
        context: dict,
    ) -> dict:
        """
        Invalidate current database credentials.
        
        ACTUALLY changes the agent_user password, breaking existing connections.
        """
        if not self._pool:
            raise ChaosHookError("Not connected to database")
        
        import secrets
        new_password = secrets.token_urlsafe(16)
        
        async with self._pool.acquire() as conn:
            await conn.execute(
                f"ALTER USER agent_user WITH PASSWORD '{new_password}'"
            )
        
        logger.info("chaos_credentials_revoked", user="agent_user")
        
        return {
            "status": "credentials_revoked",
            "should_invalidate_session": True,
            "user": "agent_user",
            "real_revocation": True,
        }
    
    async def _rename_column(
        self,
        action: ChaosAction,
        context: dict,
    ) -> dict:
        """
        Rename a column (schema mutation).
        
        ACTUALLY renames the column in the database.
        """
        table = action.table
        column = action.column
        new_name = action.new_name
        
        if not all([table, column, new_name]):
            raise ChaosHookError(
                "rename_column requires 'table', 'column', and 'new_name'"
            )
        
        if not self._pool:
            raise ChaosHookError("Not connected to database")
        
        async with self._pool.acquire() as conn:
            await conn.execute(
                f"ALTER TABLE {table} RENAME COLUMN {column} TO {new_name}"
            )
        
        logger.info(
            "chaos_column_renamed",
            table=table,
            old_name=column,
            new_name=new_name,
        )
        
        return {
            "status": "column_renamed",
            "table": table,
            "old_name": column,
            "new_name": new_name,
            "real_rename": True,
        }
    
    async def _change_column_type(
        self,
        action: ChaosAction,
        context: dict,
    ) -> dict:
        """
        Change a column's data type.
        
        ACTUALLY changes the column type in the database.
        """
        table = action.table
        column = action.column
        new_type = action.new_type
        
        if not all([table, column, new_type]):
            raise ChaosHookError(
                "change_column_type requires 'table', 'column', and 'new_type'"
            )
        
        if not self._pool:
            raise ChaosHookError("Not connected to database")
        
        async with self._pool.acquire() as conn:
            await conn.execute(
                f"ALTER TABLE {table} ALTER COLUMN {column} TYPE {new_type}"
            )
        
        logger.info(
            "chaos_column_type_changed",
            table=table,
            column=column,
            new_type=new_type,
        )
        
        return {
            "status": "column_type_changed",
            "table": table,
            "column": column,
            "new_type": new_type,
            "real_change": True,
        }
    
    async def _drop_index(
        self,
        action: ChaosAction,
        context: dict,
    ) -> dict:
        """
        Drop an index on a table.
        
        ACTUALLY drops the index, potentially causing slow queries.
        """
        index_name = action.parameters.get("index_name")
        if not index_name and action.table:
            index_name = f"idx_{action.table}_id"
        
        if not index_name:
            raise ChaosHookError("drop_index requires 'index_name' parameter")
        
        if not self._pool:
            raise ChaosHookError("Not connected to database")
        
        async with self._pool.acquire() as conn:
            await conn.execute(f"DROP INDEX IF EXISTS {index_name}")
        
        logger.info("chaos_index_dropped", index_name=index_name)
        
        return {
            "status": "index_dropped",
            "index_name": index_name,
            "real_drop": True,
        }
    
    # =========================================================================
    # Resource Chaos Handlers
    # These set flags that affect proxy behavior rather than DB directly
    # =========================================================================
    
    async def _simulate_disk_full(
        self,
        action: ChaosAction,
        context: dict,
    ) -> dict:
        """Simulate disk full condition by rejecting writes."""
        duration = action.duration_seconds or 60
        
        logger.info(
            "chaos_disk_full_simulated",
            duration_seconds=duration,
        )
        
        return {
            "status": "disk_full_simulated",
            "duration_seconds": duration,
            "error_on_write": True,
        }
    
    async def _simulate_memory_pressure(
        self,
        action: ChaosAction,
        context: dict,
    ) -> dict:
        """Simulate memory pressure."""
        percentage = action.percentage or 80
        duration = action.duration_seconds or 60
        
        logger.info(
            "chaos_memory_pressure_simulated",
            percentage=percentage,
            duration_seconds=duration,
        )
        
        return {
            "status": "memory_pressure_simulated",
            "percentage": percentage,
            "duration_seconds": duration,
        }
    
    async def _simulate_cpu_throttle(
        self,
        action: ChaosAction,
        context: dict,
    ) -> dict:
        """Throttle CPU for the container."""
        percentage = action.percentage or 50
        duration = action.duration_seconds or 60
        
        logger.info(
            "chaos_cpu_throttle_simulated",
            percentage=percentage,
            duration_seconds=duration,
        )
        
        return {
            "status": "cpu_throttled",
            "percentage": percentage,
            "duration_seconds": duration,
        }
    
    # =========================================================================
    # Network Chaos Handlers
    # =========================================================================
    
    async def _simulate_network_partition(
        self,
        action: ChaosAction,
        context: dict,
    ) -> dict:
        """Simulate network partition."""
        duration = action.duration_seconds or 30
        
        logger.info(
            "chaos_network_partition_simulated",
            duration_seconds=duration,
        )
        
        return {
            "status": "network_partitioned",
            "duration_seconds": duration,
        }
    
    async def _simulate_packet_loss(
        self,
        action: ChaosAction,
        context: dict,
    ) -> dict:
        """Introduce packet loss."""
        percentage = action.percentage or 10
        duration = action.duration_seconds or 60
        
        logger.info(
            "chaos_packet_loss_simulated",
            percentage=percentage,
            duration_seconds=duration,
        )
        
        return {
            "status": "packet_loss_simulated",
            "percentage": percentage,
            "duration_seconds": duration,
        }
    
    # =========================================================================
    # Lifecycle
    # =========================================================================
    
    async def cleanup(self) -> None:
        """Clean up any active chaos effects."""
        # Cancel all active locks
        for lock_key, task in list(self._active_locks.items()):
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._active_locks.clear()
        
        # Release any held lock connections
        if self._pool:
            for lock_key, conn in list(self._lock_connections.items()):
                try:
                    await self._pool.release(conn)
                except Exception:
                    pass
        self._lock_connections.clear()
        
        # Reset latency
        self._latency_ms = 0
        self._latency_end_time = None
        
        logger.info("chaos_hooks_cleaned_up")
    
    async def close(self) -> None:
        """Close database connections."""
        await self.cleanup()
        
        if self._pool:
            await self._pool.close()
            self._pool = None
            logger.info("chaos_hooks_connection_closed")
