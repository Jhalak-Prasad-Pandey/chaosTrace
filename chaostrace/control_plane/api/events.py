"""
Events API Routes

Endpoints for event ingestion and retrieval.
This is the critical integration point between the DB Proxy and Control Plane.
"""

from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from chaostrace.control_plane.models.events import (
    ChaosEvent,
    EventType,
    PolicyAction,
    RiskLevel,
    SQLEvent,
    SQLType,
)
from chaostrace.control_plane.services.event_store import get_event_store

router = APIRouter(prefix="/events", tags=["events"])


# ============================================================================
# Request Models
# ============================================================================

class SQLEventCreate(BaseModel):
    """Request model for creating SQL events from proxy."""
    
    run_id: UUID
    statement: str
    statement_hash: str
    sql_type: str  # Will be converted to SQLType enum
    tables: list[str] = Field(default_factory=list)
    has_where_clause: bool = False
    risk_level: str  # Will be converted to RiskLevel enum
    risk_factors: list[str] = Field(default_factory=list)
    rows_estimated: int | None = None
    policy_action: str  # Will be converted to PolicyAction enum
    policy_rule_matched: str | None = None
    violation_reason: str | None = None
    latency_ms: float = 0.0
    rows_affected: int | None = None
    execution_error: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class ChaosEventCreate(BaseModel):
    """Request model for creating chaos events."""
    
    run_id: UUID
    chaos_type: str
    trigger_type: str
    trigger_condition: str
    target: str | None = None
    duration_seconds: int | None = None
    parameters: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)


# ============================================================================
# Event Ingestion Endpoints (Called by Proxy)
# ============================================================================

@router.post("/sql")
async def ingest_sql_event(event_data: SQLEventCreate) -> dict:
    """
    Ingest a SQL event from the DB Proxy.
    
    This is the primary integration point - the proxy calls this
    endpoint for every SQL statement it intercepts.
    """
    # Determine event type based on policy action
    policy_action = PolicyAction(event_data.policy_action)
    if policy_action == PolicyAction.BLOCK:
        event_type = EventType.SQL_BLOCKED
    elif policy_action == PolicyAction.ALLOW_FLAGGED:
        event_type = EventType.SQL_FLAGGED
    else:
        event_type = EventType.SQL_ALLOWED
    
    # Create SQL event
    event = SQLEvent(
        event_id=uuid4(),
        run_id=event_data.run_id,
        timestamp=datetime.utcnow(),
        event_type=event_type,
        statement=event_data.statement,
        statement_hash=event_data.statement_hash,
        sql_type=SQLType(event_data.sql_type),
        tables=event_data.tables,
        has_where_clause=event_data.has_where_clause,
        risk_level=RiskLevel(event_data.risk_level),
        risk_factors=event_data.risk_factors,
        rows_estimated=event_data.rows_estimated,
        policy_action=policy_action,
        policy_rule_matched=event_data.policy_rule_matched,
        violation_reason=event_data.violation_reason,
        latency_ms=event_data.latency_ms,
        rows_affected=event_data.rows_affected,
        execution_error=event_data.execution_error,
        metadata=event_data.metadata,
    )
    
    # Store event
    event_store = get_event_store()
    event_store.add_event(event)
    
    return {
        "status": "ok",
        "event_id": str(event.event_id),
        "event_type": event_type.value,
    }


@router.post("/chaos")
async def ingest_chaos_event(event_data: ChaosEventCreate) -> dict:
    """
    Ingest a chaos event.
    
    Called when a chaos action is triggered by the scheduler.
    """
    event = ChaosEvent(
        event_id=uuid4(),
        run_id=event_data.run_id,
        timestamp=datetime.utcnow(),
        event_type=EventType.CHAOS_TRIGGERED,
        chaos_type=event_data.chaos_type,
        trigger_type=event_data.trigger_type,
        trigger_condition=event_data.trigger_condition,
        target=event_data.target,
        duration_seconds=event_data.duration_seconds,
        parameters=event_data.parameters,
        metadata=event_data.metadata,
    )
    
    event_store = get_event_store()
    event_store.add_event(event)
    
    return {
        "status": "ok",
        "event_id": str(event.event_id),
    }


# ============================================================================
# Event Retrieval Endpoints
# ============================================================================

@router.get("/{run_id}")
async def get_events(
    run_id: UUID,
    event_type: str | None = Query(None, description="Filter by event type prefix (sql, chaos, run)"),
    limit: int = Query(100, ge=1, le=1000),
) -> list[dict]:
    """
    Get events for a specific run.
    
    Returns SQL events, chaos events, and lifecycle events.
    """
    event_store = get_event_store()
    events = event_store.get_events(run_id, event_type=event_type, limit=limit)
    
    return [e.model_dump(mode="json") for e in events]


@router.get("/{run_id}/sql")
async def get_sql_events(
    run_id: UUID,
    limit: int = Query(100, ge=1, le=1000),
) -> list[dict]:
    """Get only SQL events for a run."""
    event_store = get_event_store()
    events = event_store.get_sql_events(run_id, limit=limit)
    return [e.model_dump(mode="json") for e in events]


@router.get("/{run_id}/chaos")
async def get_chaos_events(
    run_id: UUID,
    limit: int = Query(100, ge=1, le=1000),
) -> list[dict]:
    """Get only chaos events for a run."""
    event_store = get_event_store()
    events = event_store.get_chaos_events(run_id, limit=limit)
    return [e.model_dump(mode="json") for e in events]


@router.get("/{run_id}/violations")
async def get_violations(run_id: UUID) -> list[dict]:
    """Get all violation events (blocked or flagged) for a run."""
    event_store = get_event_store()
    events = event_store.get_violations(run_id)
    return [e.model_dump(mode="json") for e in events]


@router.get("/{run_id}/stats")
async def get_event_stats(run_id: UUID) -> dict:
    """Get aggregate statistics for a run's events."""
    event_store = get_event_store()
    return event_store.get_run_stats(run_id)


@router.delete("/{run_id}")
async def clear_run_events(run_id: UUID) -> dict:
    """
    Clear all events for a run.
    
    This is typically called during run cleanup.
    """
    event_store = get_event_store()
    count = event_store.clear_run(run_id)
    return {"status": "ok", "events_cleared": count}
