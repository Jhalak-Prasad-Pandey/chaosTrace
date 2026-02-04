"""
Chaos Models

Pydantic models for chaos scenario definition and scheduling.
Chaos scripts are defined in YAML and loaded at runtime.
"""

from datetime import datetime
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator


class TriggerType(str, Enum):
    """Type of chaos trigger."""
    
    EVENT = "event"
    """Triggered by a specific event (SQL, agent action)."""
    
    TIME = "time"
    """Triggered at a specific time offset."""
    
    COUNT = "count"
    """Triggered after N occurrences of an event."""


class ChaosActionType(str, Enum):
    """Types of chaos actions that can be injected."""
    
    # Database chaos
    LOCK_TABLE = "lock_table"
    """Lock a table to simulate contention."""
    
    ADD_LATENCY = "add_latency"
    """Add latency to database operations."""
    
    SIMULATE_TIMEOUT = "simulate_timeout"
    """Return a connection timeout error."""
    
    REVOKE_CREDENTIALS = "revoke_credentials"
    """Invalidate database credentials."""
    
    RENAME_COLUMN = "rename_column"
    """Rename a column (schema mutation)."""
    
    CHANGE_COLUMN_TYPE = "change_column_type"
    """Change a column's data type."""
    
    DROP_INDEX = "drop_index"
    """Drop an index on a table."""
    
    # Resource chaos
    DISK_FULL = "disk_full"
    """Simulate disk full condition."""
    
    MEMORY_PRESSURE = "memory_pressure"
    """Simulate memory pressure."""
    
    CPU_THROTTLE = "cpu_throttle"
    """Throttle CPU for container."""
    
    # Network chaos
    NETWORK_PARTITION = "network_partition"
    """Simulate network partition."""
    
    PACKET_LOSS = "packet_loss"
    """Introduce packet loss."""


class EventCondition(BaseModel):
    """Condition based on intercepted events."""
    
    event_type: str = Field(
        description="Type of event to match (e.g., SQL_RECEIVED)"
    )
    
    parsed_type: str | None = Field(
        default=None,
        description="SQL type to match (e.g., DELETE, UPDATE)"
    )
    
    table_pattern: str | None = Field(
        default=None,
        description="Table name pattern to match"
    )
    
    occurrence: Literal["first", "every", "last"] | int = Field(
        default="first",
        description="Which occurrence to trigger on"
    )
    
    min_rows: int | None = Field(
        default=None,
        description="Minimum estimated rows for trigger"
    )


class TimeCondition(BaseModel):
    """Condition based on elapsed time."""
    
    elapsed_seconds: int = Field(
        description="Seconds since run start"
    )
    
    jitter_seconds: int = Field(
        default=0,
        description="Random jitter to add (0 to N seconds)"
    )


class CountCondition(BaseModel):
    """Condition based on event count."""
    
    event_type: str = Field(
        description="Type of event to count"
    )
    
    count: int = Field(
        description="Number of events before triggering"
    )
    
    reset_after_trigger: bool = Field(
        default=False,
        description="Reset count after trigger fires"
    )


class ChaosAction(BaseModel):
    """
    Action to take when chaos is triggered.
    
    Different action types require different parameters:
    - lock_table: table, duration_seconds
    - add_latency: latency_ms, duration_seconds
    - simulate_timeout: (no params)
    - rename_column: table, old_name, new_name
    """
    
    type: ChaosActionType = Field(
        description="Type of chaos to inject"
    )
    
    # Target specification
    table: str | None = Field(
        default=None,
        description="Target table (supports {event.tables[0]} template)"
    )
    
    column: str | None = Field(
        default=None,
        description="Target column"
    )
    
    # Timing
    duration_seconds: int | None = Field(
        default=None,
        description="Duration of the chaos effect"
    )
    
    delay_seconds: int = Field(
        default=0,
        description="Delay before starting the chaos"
    )
    
    # Type-specific parameters
    latency_ms: int | None = Field(
        default=None,
        description="Latency in milliseconds (for add_latency)"
    )
    
    new_name: str | None = Field(
        default=None,
        description="New name (for rename operations)"
    )
    
    new_type: str | None = Field(
        default=None,
        description="New type (for type change operations)"
    )
    
    percentage: int | None = Field(
        default=None,
        description="Percentage for probabilistic chaos (0-100)"
    )
    
    # Additional parameters
    parameters: dict[str, Any] = Field(
        default_factory=dict,
        description="Additional type-specific parameters"
    )


class ChaosTrigger(BaseModel):
    """
    A chaos trigger combines a condition with an action.
    
    When the condition is met, the action is executed.
    """
    
    name: str = Field(
        default="",
        description="Optional name for this trigger"
    )
    
    enabled: bool = Field(
        default=True,
        description="Whether this trigger is active"
    )
    
    trigger_type: TriggerType = Field(
        description="Type of trigger condition"
    )
    
    # Conditions (one should be set based on trigger_type)
    event_condition: EventCondition | None = Field(
        default=None,
        description="Condition for event-based triggers"
    )
    
    time_condition: TimeCondition | None = Field(
        default=None,
        description="Condition for time-based triggers"
    )
    
    count_condition: CountCondition | None = Field(
        default=None,
        description="Condition for count-based triggers"
    )
    
    # Action to execute
    action: ChaosAction = Field(
        description="Chaos action to execute"
    )
    
    # Behavior
    max_triggers: int = Field(
        default=1,
        description="Maximum times this trigger can fire"
    )
    
    cooldown_seconds: int = Field(
        default=0,
        description="Minimum time between triggers"
    )
    
    @model_validator(mode="after")
    def validate_condition_matches_type(self) -> "ChaosTrigger":
        """Ensure the appropriate condition is set for the trigger type."""
        if self.trigger_type == TriggerType.EVENT and not self.event_condition:
            raise ValueError("Event trigger requires event_condition")
        if self.trigger_type == TriggerType.TIME and not self.time_condition:
            raise ValueError("Time trigger requires time_condition")
        if self.trigger_type == TriggerType.COUNT and not self.count_condition:
            raise ValueError("Count trigger requires count_condition")
        return self


class ChaosScenario(BaseModel):
    """
    Complete chaos scenario loaded from YAML.
    
    Example YAML:
    ```yaml
    name: db_lock_v1
    version: "1.0"
    description: "Lock table on first DELETE to test retry logic"
    
    triggers:
      - name: first_delete_lock
        trigger_type: event
        event_condition:
          event_type: SQL_RECEIVED
          parsed_type: DELETE
          occurrence: first
        action:
          type: lock_table
          table: "{event.tables[0]}"
          duration_seconds: 30
    
      - name: latency_at_90s
        trigger_type: time
        time_condition:
          elapsed_seconds: 90
        action:
          type: add_latency
          latency_ms: 30000
          duration_seconds: 60
    ```
    """
    
    name: str = Field(
        description="Unique name for this chaos scenario"
    )
    
    version: str = Field(
        default="1.0",
        description="Scenario version"
    )
    
    description: str = Field(
        default="",
        description="Human-readable description"
    )
    
    triggers: list[ChaosTrigger] = Field(
        default_factory=list,
        description="List of chaos triggers"
    )
    
    # Global settings
    enabled: bool = Field(
        default=True,
        description="Whether this scenario is active"
    )
    
    max_total_chaos_events: int = Field(
        default=100,
        description="Maximum total chaos events to allow"
    )


class ChaosState(BaseModel):
    """Runtime state for tracking chaos execution."""
    
    scenario_name: str = Field(
        description="Name of the active scenario"
    )
    
    run_id: str = Field(
        description="Associated run ID"
    )
    
    started_at: datetime = Field(
        default_factory=datetime.utcnow
    )
    
    # Trigger tracking
    trigger_fire_counts: dict[str, int] = Field(
        default_factory=dict,
        description="Count of fires per trigger name"
    )
    
    trigger_last_fired: dict[str, datetime] = Field(
        default_factory=dict,
        description="Last fire time per trigger"
    )
    
    # Event counting
    event_counts: dict[str, int] = Field(
        default_factory=dict,
        description="Count of events by type"
    )
    
    # Active chaos
    active_chaos: list[dict[str, Any]] = Field(
        default_factory=list,
        description="Currently active chaos effects"
    )
    
    total_chaos_events: int = Field(
        default=0,
        description="Total chaos events triggered"
    )
