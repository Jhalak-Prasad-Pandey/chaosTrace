"""
ChaosTrace Control Plane - FastAPI Application

Main entry point for the ChaosTrace API server.
"""

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from structlog import get_logger

from chaostrace import __version__
from chaostrace.control_plane.api import runs, reports
from chaostrace.control_plane.config import get_settings
from chaostrace.control_plane.dashboard import ENHANCED_DASHBOARD_HTML
from chaostrace.control_plane.services.orchestrator import RunOrchestrator

logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifespan handler.
    
    Initializes services on startup and cleans up on shutdown.
    """
    settings = get_settings()
    
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

# Include API routers
app.include_router(runs.router, prefix="/api")
app.include_router(reports.router, prefix="/api")


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
            "health": "/health",
        },
    }


@app.get("/", response_class=HTMLResponse)
async def dashboard():
    """Serve the web dashboard."""
    return HTMLResponse(content=ENHANCED_DASHBOARD_HTML)


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard_alt():
    """Alternative dashboard route."""
    return HTMLResponse(content=ENHANCED_DASHBOARD_HTML)
