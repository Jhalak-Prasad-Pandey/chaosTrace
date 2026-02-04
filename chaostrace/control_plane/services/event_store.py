"""
Event Store Service

SQLite-backed persistent storage for run events with per-run isolation.
Supports event ingestion, retrieval, filtering, and cleanup.
"""

import json
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Callable
from uuid import UUID

from structlog import get_logger

from chaostrace.control_plane.models.events import (
    AnyEvent,
    BaseEvent,
    ChaosEvent,
    EventType,
    PolicyAction,
    RiskLevel,
    RunLifecycleEvent,
    SQLEvent,
    SQLType,
)

logger = get_logger(__name__)

# Default database path
DEFAULT_DB_PATH = Path("./data/events.db")


class EventStore:
    """
    SQLite-backed event storage for test runs.
    
    Each run has isolated event storage. Events are persisted to SQLite
    for durability and can survive process restarts.
    
    Usage:
        store = EventStore(db_path="./data/events.db")
        store.add_event(sql_event)
        events = store.get_events(run_id, event_type="sql")
        store.clear_run(run_id)
    """
    
    def __init__(
        self,
        db_path: str | Path = DEFAULT_DB_PATH,
        max_events_per_run: int = 50000,
    ):
        """
        Initialize the event store.
        
        Args:
            db_path: Path to SQLite database file.
            max_events_per_run: Maximum events to store per run (prevents disk bloat).
        """
        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._max_events = max_events_per_run
        self._local = threading.local()
        self._subscribers: list[Callable[[BaseEvent], None]] = []
        
        # Initialize database schema
        self._init_schema()
        
        logger.info(
            "event_store_initialized",
            db_path=str(self._db_path),
            max_events=max_events_per_run,
        )
    
    def _get_connection(self) -> sqlite3.Connection:
        """Get thread-local database connection."""
        if not hasattr(self._local, "conn") or self._local.conn is None:
            self._local.conn = sqlite3.connect(
                str(self._db_path),
                check_same_thread=False,
            )
            self._local.conn.row_factory = sqlite3.Row
            # Enable WAL mode for better concurrent access
            self._local.conn.execute("PRAGMA journal_mode=WAL")
            self._local.conn.execute("PRAGMA synchronous=NORMAL")
        return self._local.conn
    
    @contextmanager
    def _transaction(self):
        """Context manager for database transactions."""
        conn = self._get_connection()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    
    def _init_schema(self) -> None:
        """Initialize database schema."""
        with self._transaction() as conn:
            # Main events table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT NOT NULL UNIQUE,
                    run_id TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    event_class TEXT NOT NULL,
                    data JSON NOT NULL,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Indexes for efficient querying
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_events_run_id 
                ON events(run_id)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_events_run_type 
                ON events(run_id, event_type)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_events_timestamp 
                ON events(run_id, timestamp)
            """)
            
            # Run statistics cache table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS run_stats (
                    run_id TEXT PRIMARY KEY,
                    total_events INTEGER DEFAULT 0,
                    sql_events INTEGER DEFAULT 0,
                    blocked_events INTEGER DEFAULT 0,
                    flagged_events INTEGER DEFAULT 0,
                    chaos_events INTEGER DEFAULT 0,
                    last_updated TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
    
    def add_event(self, event: BaseEvent) -> None:
        """
        Add an event to the store.
        
        Args:
            event: Event to store (must have run_id set).
        """
        # Determine event class for reconstruction
        if isinstance(event, SQLEvent):
            event_class = "SQLEvent"
        elif isinstance(event, ChaosEvent):
            event_class = "ChaosEvent"
        elif isinstance(event, RunLifecycleEvent):
            event_class = "RunLifecycleEvent"
        else:
            event_class = "BaseEvent"
        
        # Serialize event data
        event_data = event.model_dump(mode="json")
        
        with self._transaction() as conn:
            # Check capacity
            count = conn.execute(
                "SELECT COUNT(*) FROM events WHERE run_id = ?",
                (str(event.run_id),)
            ).fetchone()[0]
            
            if count >= self._max_events:
                # Delete oldest 10% to make room
                drop_count = self._max_events // 10
                conn.execute("""
                    DELETE FROM events WHERE id IN (
                        SELECT id FROM events 
                        WHERE run_id = ? 
                        ORDER BY timestamp ASC 
                        LIMIT ?
                    )
                """, (str(event.run_id), drop_count))
                
                logger.warning(
                    "event_store_capacity_reached",
                    run_id=str(event.run_id),
                    dropped=drop_count,
                )
            
            # Insert event
            conn.execute("""
                INSERT INTO events (event_id, run_id, timestamp, event_type, event_class, data)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                str(event.event_id),
                str(event.run_id),
                event.timestamp.isoformat(),
                event.event_type.value,
                event_class,
                json.dumps(event_data),
            ))
            
            # Update stats cache
            self._update_stats(conn, event)
        
        logger.debug(
            "event_stored",
            run_id=str(event.run_id),
            event_type=event.event_type.value,
            event_id=str(event.event_id),
        )
        
        # Notify subscribers
        for subscriber in self._subscribers:
            try:
                subscriber(event)
            except Exception as e:
                logger.error("event_subscriber_error", error=str(e))
    
    def _update_stats(self, conn: sqlite3.Connection, event: BaseEvent) -> None:
        """Update run statistics cache."""
        run_id = str(event.run_id)
        
        # Ensure stats row exists
        conn.execute("""
            INSERT OR IGNORE INTO run_stats (run_id) VALUES (?)
        """, (run_id,))
        
        # Update counters
        updates = ["total_events = total_events + 1"]
        
        if isinstance(event, SQLEvent):
            updates.append("sql_events = sql_events + 1")
            if event.event_type == EventType.SQL_BLOCKED:
                updates.append("blocked_events = blocked_events + 1")
            elif event.event_type == EventType.SQL_FLAGGED:
                updates.append("flagged_events = flagged_events + 1")
        elif isinstance(event, ChaosEvent):
            updates.append("chaos_events = chaos_events + 1")
        
        conn.execute(f"""
            UPDATE run_stats 
            SET {', '.join(updates)}, last_updated = CURRENT_TIMESTAMP
            WHERE run_id = ?
        """, (run_id,))
    
    def add_sql_event(self, event: SQLEvent) -> None:
        """Convenience method for adding SQL events."""
        self.add_event(event)
    
    def add_chaos_event(self, event: ChaosEvent) -> None:
        """Convenience method for adding chaos events."""
        self.add_event(event)
    
    def _reconstruct_event(self, row: sqlite3.Row) -> BaseEvent:
        """Reconstruct event object from database row."""
        data = json.loads(row["data"])
        event_class = row["event_class"]
        
        # Convert string enums back
        if "event_type" in data:
            data["event_type"] = EventType(data["event_type"])
        if "sql_type" in data and data["sql_type"]:
            data["sql_type"] = SQLType(data["sql_type"])
        if "risk_level" in data and data["risk_level"]:
            data["risk_level"] = RiskLevel(data["risk_level"])
        if "policy_action" in data and data["policy_action"]:
            data["policy_action"] = PolicyAction(data["policy_action"])
        
        # Convert UUID strings
        if "event_id" in data:
            data["event_id"] = UUID(data["event_id"])
        if "run_id" in data:
            data["run_id"] = UUID(data["run_id"])
        
        # Convert timestamp
        if "timestamp" in data:
            data["timestamp"] = datetime.fromisoformat(data["timestamp"])
        
        # Reconstruct based on class
        if event_class == "SQLEvent":
            return SQLEvent(**data)
        elif event_class == "ChaosEvent":
            return ChaosEvent(**data)
        elif event_class == "RunLifecycleEvent":
            return RunLifecycleEvent(**data)
        else:
            return BaseEvent(**data)
    
    def get_events(
        self,
        run_id: UUID,
        event_type: str | EventType | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
        limit: int | None = None,
    ) -> list[BaseEvent]:
        """
        Get events for a run with optional filtering.
        
        Args:
            run_id: Run to get events for.
            event_type: Filter by event type (e.g., "sql_blocked", "sql" for all SQL).
            since: Only events after this timestamp.
            until: Only events before this timestamp.
            limit: Maximum number of events to return.
            
        Returns:
            List of events matching the criteria.
        """
        conn = self._get_connection()
        
        query = "SELECT * FROM events WHERE run_id = ?"
        params: list[Any] = [str(run_id)]
        
        # Event type filter
        if event_type is not None:
            if isinstance(event_type, EventType):
                query += " AND event_type = ?"
                params.append(event_type.value)
            elif event_type.lower() in ("sql", "chaos", "run", "agent"):
                # Prefix match
                query += " AND event_type LIKE ?"
                params.append(f"{event_type.lower()}%")
            else:
                query += " AND event_type = ?"
                params.append(event_type)
        
        # Time filters
        if since is not None:
            query += " AND timestamp >= ?"
            params.append(since.isoformat())
        
        if until is not None:
            query += " AND timestamp <= ?"
            params.append(until.isoformat())
        
        # Order and limit
        query += " ORDER BY timestamp ASC"
        if limit is not None:
            query += " LIMIT ?"
            params.append(limit)
        
        rows = conn.execute(query, params).fetchall()
        return [self._reconstruct_event(row) for row in rows]
    
    def get_sql_events(
        self,
        run_id: UUID,
        limit: int | None = None,
    ) -> list[SQLEvent]:
        """Get only SQL events for a run."""
        events = self.get_events(run_id, event_type="sql", limit=limit)
        return [e for e in events if isinstance(e, SQLEvent)]
    
    def get_chaos_events(
        self,
        run_id: UUID,
        limit: int | None = None,
    ) -> list[ChaosEvent]:
        """Get only chaos events for a run."""
        events = self.get_events(run_id, event_type="chaos", limit=limit)
        return [e for e in events if isinstance(e, ChaosEvent)]
    
    def get_blocked_events(self, run_id: UUID) -> list[SQLEvent]:
        """Get all blocked SQL events for a run."""
        events = self.get_events(run_id, event_type=EventType.SQL_BLOCKED)
        return [e for e in events if isinstance(e, SQLEvent)]
    
    def get_violations(self, run_id: UUID) -> list[SQLEvent]:
        """Get all violation events (blocked or flagged) for a run."""
        blocked = self.get_events(run_id, event_type=EventType.SQL_BLOCKED)
        flagged = self.get_events(run_id, event_type=EventType.SQL_FLAGGED)
        events = blocked + flagged
        events.sort(key=lambda e: e.timestamp)
        return [e for e in events if isinstance(e, SQLEvent)]
    
    def get_run_stats(self, run_id: UUID) -> dict:
        """
        Get aggregate statistics for a run.
        
        Returns:
            Dictionary with event counts and summary stats.
        """
        conn = self._get_connection()
        
        # Get cached stats
        row = conn.execute(
            "SELECT * FROM run_stats WHERE run_id = ?",
            (str(run_id),)
        ).fetchone()
        
        if row:
            stats = {
                "total_events": row["total_events"],
                "sql_events": row["sql_events"],
                "chaos_events": row["chaos_events"],
                "blocked_events": row["blocked_events"],
                "flagged_events": row["flagged_events"],
            }
        else:
            stats = {
                "total_events": 0,
                "sql_events": 0,
                "chaos_events": 0,
                "blocked_events": 0,
                "flagged_events": 0,
            }
        
        # Get tables accessed (requires query)
        tables_rows = conn.execute("""
            SELECT DISTINCT json_extract(data, '$.tables') as tables
            FROM events 
            WHERE run_id = ? AND event_class = 'SQLEvent'
        """, (str(run_id),)).fetchall()
        
        tables = set()
        for row in tables_rows:
            if row["tables"]:
                tables.update(json.loads(row["tables"]))
        
        stats["tables_accessed"] = list(tables)
        
        # Get violation reasons
        violations = conn.execute("""
            SELECT json_extract(data, '$.violation_reason') as reason
            FROM events 
            WHERE run_id = ? AND event_type = 'sql_blocked'
        """, (str(run_id),)).fetchall()
        
        stats["violation_reasons"] = [
            row["reason"] for row in violations if row["reason"]
        ]
        
        return stats
    
    def run_exists(self, run_id: UUID) -> bool:
        """Check if a run has any events stored."""
        conn = self._get_connection()
        count = conn.execute(
            "SELECT COUNT(*) FROM events WHERE run_id = ? LIMIT 1",
            (str(run_id),)
        ).fetchone()[0]
        return count > 0
    
    def clear_run(self, run_id: UUID) -> int:
        """
        Clear all events for a run.
        
        Args:
            run_id: Run to clear events for.
            
        Returns:
            Number of events cleared.
        """
        with self._transaction() as conn:
            # Get count first
            count = conn.execute(
                "SELECT COUNT(*) FROM events WHERE run_id = ?",
                (str(run_id),)
            ).fetchone()[0]
            
            # Delete events
            conn.execute("DELETE FROM events WHERE run_id = ?", (str(run_id),))
            
            # Delete stats
            conn.execute("DELETE FROM run_stats WHERE run_id = ?", (str(run_id),))
            
            logger.info("run_events_cleared", run_id=str(run_id), count=count)
            return count
    
    def subscribe(self, callback: Callable[[BaseEvent], None]) -> None:
        """Subscribe to new events."""
        self._subscribers.append(callback)
    
    def unsubscribe(self, callback: Callable[[BaseEvent], None]) -> None:
        """Remove an event subscriber."""
        if callback in self._subscribers:
            self._subscribers.remove(callback)
    
    def export_run(self, run_id: UUID) -> list[dict]:
        """Export all events for a run as JSON-serializable dicts."""
        events = self.get_events(run_id)
        return [e.model_dump(mode="json") for e in events]
    
    def get_active_runs(self) -> list[UUID]:
        """Get list of run IDs with stored events."""
        conn = self._get_connection()
        rows = conn.execute(
            "SELECT DISTINCT run_id FROM events"
        ).fetchall()
        return [UUID(row["run_id"]) for row in rows]
    
    def get_total_event_count(self) -> int:
        """Get total events across all runs."""
        conn = self._get_connection()
        count = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        return count
    
    def vacuum(self) -> None:
        """Compact the database file."""
        conn = self._get_connection()
        conn.execute("VACUUM")
        logger.info("event_store_vacuumed")


# ============================================================================
# Singleton Instance
# ============================================================================

_event_store: EventStore | None = None


def get_event_store(db_path: str | Path | None = None) -> EventStore:
    """
    Get the global event store instance.
    
    Creates the instance on first call (lazy initialization).
    
    Args:
        db_path: Optional custom database path (only used on first call).
    """
    global _event_store
    if _event_store is None:
        _event_store = EventStore(db_path=db_path or DEFAULT_DB_PATH)
    return _event_store


def reset_event_store() -> None:
    """Reset the global event store (useful for testing)."""
    global _event_store
    _event_store = None
