"""
Run API Routes

Endpoints for managing test runs.
"""

from uuid import UUID

from fastapi import APIRouter, HTTPException, Query

from chaostrace.control_plane.models.run import (
    RunListResponse,
    RunRequest,
    RunResponse,
    RunState,
    RunSummary,
)
from chaostrace.control_plane.services.orchestrator import RunOrchestrator

router = APIRouter(prefix="/runs", tags=["runs"])

# Global orchestrator instance (will be set by main app)
_orchestrator: RunOrchestrator | None = None


def get_orchestrator() -> RunOrchestrator:
    """Get the orchestrator instance."""
    if _orchestrator is None:
        raise HTTPException(
            status_code=503,
            detail="Orchestrator not initialized"
        )
    return _orchestrator


def set_orchestrator(orch: RunOrchestrator) -> None:
    """Set the global orchestrator instance."""
    global _orchestrator
    _orchestrator = orch


@router.post("", response_model=RunResponse)
async def create_run(request: RunRequest) -> RunResponse:
    """
    Create a new test run.
    
    This creates an isolated sandbox environment with:
    - Fresh PostgreSQL instance
    - DB Proxy for SQL interception
    - Agent runner container
    
    The run starts immediately in the background.
    """
    orchestrator = get_orchestrator()
    return await orchestrator.create_run(request)


@router.get("", response_model=RunListResponse)
async def list_runs(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
) -> RunListResponse:
    """
    List all test runs with pagination.
    
    Returns runs sorted by creation time (newest first).
    """
    orchestrator = get_orchestrator()
    runs, total = await orchestrator.list_runs(page=page, page_size=page_size)
    
    summaries = []
    for run in runs:
        duration = None
        if run.started_at and run.ended_at:
            duration = (run.ended_at - run.started_at).total_seconds()
        
        summaries.append(RunSummary(
            run_id=run.run_id,
            status=run.status,
            verdict=run.verdict,
            scenario=run.request.scenario,
            policy_profile=run.request.policy_profile,
            created_at=run.created_at,
            duration_seconds=duration,
            total_sql_events=run.total_sql_events,
            blocked_events=run.blocked_events,
        ))
    
    return RunListResponse(
        runs=summaries,
        total=total,
        page=page,
        page_size=page_size,
    )


@router.get("/{run_id}", response_model=RunState)
async def get_run(run_id: UUID) -> RunState:
    """
    Get details of a specific run.
    
    Returns current status, metrics, and container IDs.
    """
    orchestrator = get_orchestrator()
    state = await orchestrator.get_run_status(run_id)
    
    if state is None:
        raise HTTPException(status_code=404, detail="Run not found")
    
    return state


@router.post("/{run_id}/terminate")
async def terminate_run(run_id: UUID) -> dict:
    """
    Terminate a running test.
    
    Stops all containers and cleans up resources.
    """
    orchestrator = get_orchestrator()
    success = await orchestrator.terminate_run(run_id)
    
    if not success:
        raise HTTPException(
            status_code=400,
            detail="Run not found or already completed"
        )
    
    return {"status": "terminated", "run_id": str(run_id)}


@router.get("/{run_id}/events")
async def get_run_events(
    run_id: UUID,
    event_type: str | None = None,
    limit: int = Query(100, ge=1, le=1000),
) -> list[dict]:
    """
    Get events for a specific run.
    
    Returns SQL events, chaos events, and lifecycle events.
    """
    # TODO: Implement event store retrieval
    return []


@router.get("/{run_id}/report")
async def get_run_report(run_id: UUID) -> dict:
    """
    Get the analysis report for a completed run.
    
    Returns verdict, timeline, violations, and recommendations.
    """
    orchestrator = get_orchestrator()
    state = await orchestrator.get_run_status(run_id)
    
    if state is None:
        raise HTTPException(status_code=404, detail="Run not found")
    
    # Basic report structure
    return {
        "run_id": str(run_id),
        "status": state.status.value,
        "verdict": state.verdict.value if state.verdict else None,
        "summary": {
            "total_sql_events": state.total_sql_events,
            "blocked_events": state.blocked_events,
            "chaos_events_triggered": state.chaos_events_triggered,
            "violations": state.violations,
        },
        "duration_seconds": (
            (state.ended_at - state.started_at).total_seconds()
            if state.ended_at and state.started_at
            else None
        ),
        "created_at": state.created_at.isoformat(),
        "scenario": state.request.scenario,
        "policy_profile": state.request.policy_profile,
    }
