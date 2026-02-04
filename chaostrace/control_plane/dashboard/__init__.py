"""
Dashboard Module

Serves the ChaosTrace web dashboard using Jinja2 templates and static files.
"""

from pathlib import Path

# Dashboard directory path
DASHBOARD_DIR = Path(__file__).parent

# Template and static file paths
TEMPLATES_DIR = DASHBOARD_DIR / "templates"
STATIC_DIR = DASHBOARD_DIR


def get_templates_dir() -> Path:
    """Get the path to the templates directory."""
    return TEMPLATES_DIR


def get_static_dir() -> Path:
    """Get the path to the static files directory."""
    return STATIC_DIR
