"""
ChaosTrace Control Plane - FastAPI Application

Main entry point for the ChaosTrace API server.
"""

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from structlog import get_logger

from chaostrace import __version__
from chaostrace.control_plane.api import runs, reports, events
from chaostrace.control_plane.config import get_settings
from chaostrace.control_plane.dashboard import get_static_dir, get_templates_dir
from chaostrace.control_plane.services.orchestrator import RunOrchestrator
from chaostrace.control_plane.services.event_store import get_event_store

logger = get_logger(__name__)

# Paths for dashboard files
DASHBOARD_DIR = Path(__file__).parent / "dashboard"
TEMPLATES_DIR = DASHBOARD_DIR / "templates"


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifespan handler.
    
    Initializes services on startup and cleans up on shutdown.
    """
    settings = get_settings()
    
    # Initialize event store (creates database if needed)
    event_store = get_event_store()
    logger.info("event_store_ready", total_events=event_store.get_total_event_count())
    
    # Initialize orchestrator
    orchestrator = RunOrchestrator(settings)
    runs.set_orchestrator(orchestrator)
    
    logger.info(
        "chaostrace_starting",
        version=__version__,
        environment=settings.environment,
    )
    
    yield
    
    # Cleanup
    orchestrator.close()
    logger.info("chaostrace_shutdown")


# Create FastAPI app
app = FastAPI(
    title="ChaosTrace",
    description="AI Agent Chaos Testing Platform",
    version=__version__,
    lifespan=lifespan,
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount static files (CSS, JS)
app.mount("/static", StaticFiles(directory=str(DASHBOARD_DIR)), name="static")

# Include API routers
app.include_router(runs.router, prefix="/api")
app.include_router(reports.router, prefix="/api")
app.include_router(events.router, prefix="/api")


# Health check
@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "version": __version__,
    }


# API Info
@app.get("/api")
async def api_info():
    """API information endpoint."""
    return {
        "name": "ChaosTrace API",
        "version": __version__,
        "docs": "/docs",
        "endpoints": {
            "runs": "/api/runs",
            "reports": "/api/reports",
            "events": "/api/events",
            "health": "/health",
        },
    }


@app.get("/")
async def dashboard():
    """Serve the web dashboard."""
    return FileResponse(TEMPLATES_DIR / "index.html")


@app.get("/dashboard")
async def dashboard_alt():
    """Alternative dashboard route."""
    return FileResponse(TEMPLATES_DIR / "index.html")

