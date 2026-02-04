"""
Event Models

Pydantic models for observability events including SQL interception,
chaos triggers, policy decisions, and agent actions.
"""

from datetime import datetime, UTC
from enum import Enum
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field


class EventType(str, Enum):
    """Types of events that can occur during a run."""
    
    # SQL Events
    SQL_RECEIVED = "sql_received"
    """SQL statement received by the proxy."""
    
    SQL_ALLOWED = "sql_allowed"
    """SQL statement allowed by policy engine."""
    
    SQL_BLOCKED = "sql_blocked"
    """SQL statement blocked by policy engine."""
    
    SQL_FLAGGED = "sql_flagged"
    """SQL statement allowed but flagged for review."""
    
    SQL_ERROR = "sql_error"
    """SQL execution resulted in an error."""
    
    SQL_RESULT = "sql_result"
    """SQL execution completed with result."""
    
    # Chaos Events
    CHAOS_SCHEDULED = "chaos_scheduled"
    """Chaos event has been scheduled."""
    
    CHAOS_TRIGGERED = "chaos_triggered"
    """Chaos event has been triggered."""
    
    CHAOS_COMPLETED = "chaos_completed"
    """Chaos event has completed."""
    
    # Run Lifecycle Events
    RUN_STARTED = "run_started"
    """Test run has started."""
    
    RUN_COMPLETED = "run_completed"
    """Test run completed normally."""
    
    RUN_FAILED = "run_failed"
    """Test run failed with error."""
    
    RUN_TERMINATED = "run_terminated"
    """Test run was terminated."""
    
    # Agent Events
    AGENT_ACTION = "agent_action"
    """Agent performed an action."""
    
    AGENT_ERROR = "agent_error"
    """Agent encountered an error."""


class RiskLevel(str, Enum):
    """Risk classification for SQL operations."""
    
    LOW = "low"
    """Safe read-only or single-row operations."""
    
    MEDIUM = "medium"
    """Modifying operations with proper constraints."""
    
    HIGH = "high"
    """Bulk modifications or sensitive table access."""
    
    CRITICAL = "critical"
    """Destructive DDL or unrestricted bulk operations."""


class SQLType(str, Enum):
    """Classified type of SQL statement."""
    
    SELECT = "select"
    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"
    CREATE = "create"
    ALTER = "alter"
    DROP = "drop"
    TRUNCATE = "truncate"
    GRANT = "grant"
    REVOKE = "revoke"
    BEGIN = "begin"
    COMMIT = "commit"
    ROLLBACK = "rollback"
    OTHER = "other"


class PolicyAction(str, Enum):
    """Action taken by the policy engine."""
    
    ALLOW = "allow"
    """Operation is permitted."""
    
    BLOCK = "block"
    """Operation is blocked."""
    
    ALLOW_FLAGGED = "allow_flagged"
    """Operation is permitted but flagged for review."""


class BaseEvent(BaseModel):
    """
    Base event model with common fields.
    
    All events inherit from this to ensure consistent structure
    for logging and analysis.
    """
    
    event_id: UUID = Field(
        description="Unique identifier for this event"
    )
    
    run_id: UUID = Field(
        description="Run this event belongs to"
    )
    
    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        description="When the event occurred"
    )
    
    event_type: EventType = Field(
        description="Type of event"
    )
    
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        description="Additional event-specific metadata"
    )


class SQLEvent(BaseEvent):
    """
    Event representing an intercepted SQL statement.
    
    This is the primary event type for analyzing agent behavior
    with database operations.
    """
    
    # SQL Details
    statement: str = Field(
        description="The raw SQL statement"
    )
    
    statement_hash: str = Field(
        description="Hash of the statement for deduplication"
    )
    
    sql_type: SQLType = Field(
        description="Classified type of SQL statement"
    )
    
    tables: list[str] = Field(
        default_factory=list,
        description="Tables referenced in the statement"
    )
    
    has_where_clause: bool = Field(
        default=False,
        description="Whether the statement has a WHERE clause"
    )
    
    # Risk Assessment
    risk_level: RiskLevel = Field(
        description="Assessed risk level of this operation"
    )
    
    risk_factors: list[str] = Field(
        default_factory=list,
        description="Factors contributing to risk assessment"
    )
    
    rows_estimated: int | None = Field(
        default=None,
        description="Estimated number of rows affected"
    )
    
    # Policy Decision
    policy_action: PolicyAction = Field(
        description="Action taken by policy engine"
    )
    
    policy_rule_matched: str | None = Field(
        default=None,
        description="Name of the policy rule that matched"
    )
    
    violation_reason: str | None = Field(
        default=None,
        description="Reason for blocking if blocked"
    )
    
    # Performance
    latency_ms: float = Field(
        default=0.0,
        description="Time from receipt to response in milliseconds"
    )
    
    # Result (if allowed and executed)
    rows_affected: int | None = Field(
        default=None,
        description="Actual rows affected after execution"
    )
    
    execution_error: str | None = Field(
        default=None,
        description="Error message if execution failed"
    )


class ChaosEvent(BaseEvent):
    """Event representing a chaos injection."""
    
    chaos_type: str = Field(
        description="Type of chaos (lock_table, add_latency, etc.)"
    )
    
    trigger_type: str = Field(
        description="What triggered this chaos (event, time)"
    )
    
    trigger_condition: str = Field(
        description="Condition that was met"
    )
    
    target: str | None = Field(
        default=None,
        description="Target of the chaos (table name, etc.)"
    )
    
    duration_seconds: int | None = Field(
        default=None,
        description="Duration of the chaos effect"
    )
    
    parameters: dict[str, Any] = Field(
        default_factory=dict,
        description="Additional chaos parameters"
    )


class RunLifecycleEvent(BaseEvent):
    """Event representing a run state change."""
    
    previous_status: str | None = Field(
        default=None,
        description="Previous run status"
    )
    
    new_status: str = Field(
        description="New run status"
    )
    
    message: str = Field(
        description="Description of the state change"
    )
    
    verdict: str | None = Field(
        default=None,
        description="Verdict if run completed"
    )


class AgentEvent(BaseEvent):
    """Event representing an agent action or state."""
    
    action: str = Field(
        description="Description of the agent action"
    )
    
    success: bool = Field(
        default=True,
        description="Whether the action succeeded"
    )
    
    error_message: str | None = Field(
        default=None,
        description="Error message if action failed"
    )
    
    duration_ms: float | None = Field(
        default=None,
        description="Duration of the action"
    )


# Type alias for any event type
AnyEvent = SQLEvent | ChaosEvent | RunLifecycleEvent | AgentEvent
