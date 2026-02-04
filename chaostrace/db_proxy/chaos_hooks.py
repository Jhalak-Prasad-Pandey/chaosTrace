"""
Chaos Hooks

Implementation of chaos actions that can be injected
during SQL proxy operations.
"""

import asyncio
from datetime import datetime
from typing import Any

from structlog import get_logger

from chaostrace.control_plane.models.chaos import ChaosAction, ChaosActionType

logger = get_logger(__name__)


class ChaosHookError(Exception):
    """Error executing a chaos hook."""
    pass


class ChaosHooks:
    """
    Implements chaos injection actions.
    
    Each method corresponds to a chaos action type and
    modifies the sandbox environment accordingly.
    
    Usage:
        hooks = ChaosHooks(postgres_connection)
        await hooks.execute(chaos_action)
    """
    
    def __init__(self, db_connection: Any = None):
        """
        Initialize chaos hooks.
        
        Args:
            db_connection: Connection to the sandbox PostgreSQL.
        """
        self.db_connection = db_connection
        self._active_locks: dict[str, asyncio.Task] = {}
        self._latency_ms: int = 0
        self._latency_end_time: datetime | None = None
        
        logger.info("chaos_hooks_initialized")
    
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
    # Database Chaos Handlers
    # =========================================================================
    
    async def _lock_table(
        self,
        action: ChaosAction,
        context: dict,
    ) -> dict:
        """
        Lock a table to simulate contention.
        
        Uses PostgreSQL's LOCK TABLE ... IN ACCESS EXCLUSIVE MODE
        """
        table = action.table
        duration = action.duration_seconds or 30
        
        if not table:
            raise ChaosHookError("lock_table requires 'table' parameter")
        
        # In a real implementation, this would execute:
        # LOCK TABLE {table} IN ACCESS EXCLUSIVE MODE;
        # And hold the lock for the specified duration
        
        logger.info(
            "chaos_lock_table",
            table=table,
            duration_seconds=duration,
        )
        
        # Simulate the lock (actual implementation would use pg connection)
        lock_key = f"lock_{table}"
        
        async def hold_lock():
            await asyncio.sleep(duration)
            if lock_key in self._active_locks:
                del self._active_locks[lock_key]
        
        task = asyncio.create_task(hold_lock())
        self._active_locks[lock_key] = task
        
        return {
            "status": "locked",
            "table": table,
            "duration_seconds": duration,
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
            "chaos_add_latency",
            latency_ms=latency_ms,
            duration_seconds=duration,
        )
        
        # Schedule latency removal
        async def remove_latency():
            await asyncio.sleep(duration)
            self._latency_ms = 0
            self._latency_end_time = None
        
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
        """Simulate a connection timeout error."""
        logger.info("chaos_simulate_timeout")
        
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
        """Invalidate current database credentials."""
        logger.info("chaos_revoke_credentials")
        
        # In reality, this would:
        # ALTER USER agent PASSWORD 'new_random_password';
        
        return {
            "status": "credentials_revoked",
            "should_invalidate_session": True,
        }
    
    async def _rename_column(
        self,
        action: ChaosAction,
        context: dict,
    ) -> dict:
        """Rename a column (schema mutation)."""
        table = action.table
        column = action.column
        new_name = action.new_name
        
        if not all([table, column, new_name]):
            raise ChaosHookError(
                "rename_column requires 'table', 'column', and 'new_name'"
            )
        
        # In reality:
        # ALTER TABLE {table} RENAME COLUMN {column} TO {new_name};
        
        logger.info(
            "chaos_rename_column",
            table=table,
            old_name=column,
            new_name=new_name,
        )
        
        return {
            "status": "column_renamed",
            "table": table,
            "old_name": column,
            "new_name": new_name,
        }
    
    async def _change_column_type(
        self,
        action: ChaosAction,
        context: dict,
    ) -> dict:
        """Change a column's data type."""
        table = action.table
        column = action.column
        new_type = action.new_type
        
        if not all([table, column, new_type]):
            raise ChaosHookError(
                "change_column_type requires 'table', 'column', and 'new_type'"
            )
        
        logger.info(
            "chaos_change_column_type",
            table=table,
            column=column,
            new_type=new_type,
        )
        
        return {
            "status": "column_type_changed",
            "table": table,
            "column": column,
            "new_type": new_type,
        }
    
    async def _drop_index(
        self,
        action: ChaosAction,
        context: dict,
    ) -> dict:
        """Drop an index on a table."""
        # Index name would be in parameters
        index_name = action.parameters.get("index_name", f"idx_{action.table}_id")
        
        logger.info(
            "chaos_drop_index",
            index_name=index_name,
        )
        
        return {
            "status": "index_dropped",
            "index_name": index_name,
        }
    
    # =========================================================================
    # Resource Chaos Handlers
    # =========================================================================
    
    async def _simulate_disk_full(
        self,
        action: ChaosAction,
        context: dict,
    ) -> dict:
        """Simulate disk full condition."""
        duration = action.duration_seconds or 60
        
        logger.info(
            "chaos_disk_full",
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
        percentage = action.percentage or 80  # 80% memory usage
        duration = action.duration_seconds or 60
        
        logger.info(
            "chaos_memory_pressure",
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
        percentage = action.percentage or 50  # 50% throttle
        duration = action.duration_seconds or 60
        
        logger.info(
            "chaos_cpu_throttle",
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
            "chaos_network_partition",
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
        percentage = action.percentage or 10  # 10% packet loss
        duration = action.duration_seconds or 60
        
        logger.info(
            "chaos_packet_loss",
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
            del self._active_locks[lock_key]
        
        # Reset latency
        self._latency_ms = 0
        self._latency_end_time = None
        
        logger.info("chaos_hooks_cleaned_up")
