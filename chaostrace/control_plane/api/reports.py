"""
Report API Routes

Endpoints for generating and retrieving run reports.
"""

from pathlib import Path
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import PlainTextResponse

from chaostrace.control_plane.api.runs import get_orchestrator
from chaostrace.control_plane.services.report_generator import (
    ReportFormat,
    ReportGenerator,
)

router = APIRouter(prefix="/reports", tags=["reports"])

# Global report generator
_generator = ReportGenerator()


@router.get("/{run_id}")
async def get_report(
    run_id: UUID,
    format: ReportFormat = Query(ReportFormat.JSON, description="Output format"),
):
    """
    Generate or retrieve a report for a run.
    
    Returns the report in JSON or Markdown format.
    """
    orchestrator = get_orchestrator()
    state = await orchestrator.get_run_status(run_id)
    
    if state is None:
        raise HTTPException(status_code=404, detail="Run not found")
    
    # Get events for the run (placeholder - would be from event store)
    events = []  # TODO: Implement event retrieval
    
    # Generate report
    report = _generator.generate(state, events, format)
    
    if format == ReportFormat.MARKDOWN:
        return PlainTextResponse(report, media_type="text/markdown")
    
    return report


@router.get("/{run_id}/score")
async def get_score(run_id: UUID) -> dict:
    """
    Get the safety score for a run.
    
    Returns a simplified score object suitable for CI/CD.
    """
    orchestrator = get_orchestrator()
    state = await orchestrator.get_run_status(run_id)
    
    if state is None:
        raise HTTPException(status_code=404, detail="Run not found")
    
    # Get events for the run
    events = []  # TODO: Implement event retrieval
    
    # Generate report and extract score
    report = _generator.generate(state, events)
    
    return {
        "run_id": str(run_id),
        "score": report["score"]["final_score"],
        "grade": report["score"]["grade"],
        "pass": report["ci"]["pass"],
        "exit_code": report["ci"]["exit_code"],
    }


@router.get("/{run_id}/ci")
async def get_ci_status(run_id: UUID) -> dict:
    """
    Get CI-friendly status for a run.
    
    Returns minimal data for CI/CD integration.
    """
    orchestrator = get_orchestrator()
    state = await orchestrator.get_run_status(run_id)
    
    if state is None:
        raise HTTPException(status_code=404, detail="Run not found")
    
    events = []
    report = _generator.generate(state, events)
    
    return report["ci"]
