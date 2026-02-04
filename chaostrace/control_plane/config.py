"""
Configuration Management for ChaosTrace

Uses Pydantic Settings for environment-based configuration with
sensible defaults for development and production environments.
"""

from pathlib import Path
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Application configuration loaded from environment variables.
    
    Environment variables should be prefixed with CHAOSTRACE_.
    Example: CHAOSTRACE_DEBUG=true
    """
    
    model_config = SettingsConfigDict(
        env_prefix="CHAOSTRACE_",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )
    
    # =========================================================================
    # General Settings
    # =========================================================================
    
    debug: bool = Field(
        default=False,
        description="Enable debug mode with verbose logging"
    )
    
    environment: Literal["development", "staging", "production"] = Field(
        default="development",
        description="Current runtime environment"
    )
    
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = Field(
        default="INFO",
        description="Logging level for the application"
    )
    
    # =========================================================================
    # API Server Settings
    # =========================================================================
    
    api_host: str = Field(
        default="0.0.0.0",
        description="Host to bind the API server"
    )
    
    api_port: int = Field(
        default=8000,
        description="Port for the API server"
    )
    
    api_workers: int = Field(
        default=1,
        description="Number of uvicorn workers"
    )
    
    # =========================================================================
    # Docker Settings
    # =========================================================================
    
    docker_socket: str = Field(
        default="unix:///var/run/docker.sock",
        description="Docker daemon socket path"
    )
    
    docker_network_prefix: str = Field(
        default="chaostrace_run",
        description="Prefix for isolated Docker networks"
    )
    
    docker_cleanup_on_exit: bool = Field(
        default=True,
        description="Remove containers/networks after run completion"
    )
    
    # =========================================================================
    # Container Settings
    # =========================================================================
    
    postgres_image: str = Field(
        default="postgres:16-alpine",
        description="PostgreSQL Docker image"
    )
    
    postgres_user: str = Field(
        default="sandbox",
        description="PostgreSQL username for sandbox"
    )
    
    postgres_password: str = Field(
        default="sandbox_password",
        description="PostgreSQL password for sandbox (always fake credentials)"
    )
    
    postgres_db: str = Field(
        default="sandbox",
        description="PostgreSQL database name"
    )
    
    # =========================================================================
    # Proxy Settings
    # =========================================================================
    
    proxy_listen_port: int = Field(
        default=5433,
        description="Port the DB proxy listens on"
    )
    
    proxy_target_port: int = Field(
        default=5432,
        description="Port the proxy forwards to (PostgreSQL)"
    )
    
    proxy_buffer_size: int = Field(
        default=65536,
        description="Buffer size for proxy data transfer"
    )
    
    # =========================================================================
    # Run Settings
    # =========================================================================
    
    default_timeout_seconds: int = Field(
        default=300,
        description="Default timeout for a test run in seconds"
    )
    
    max_timeout_seconds: int = Field(
        default=3600,
        description="Maximum allowed timeout for a test run"
    )
    
    # =========================================================================
    # File Paths
    # =========================================================================
    
    base_dir: Path = Field(
        default=Path(__file__).parent.parent.parent,
        description="Base directory of the project"
    )

    host_workspace_path: str | None = Field(
        default=None,
        description="Path to the workspace on the host machine"
    )
    
    @property
    def policies_dir(self) -> Path:
        """Directory containing policy YAML files."""
        return self.base_dir / "policies"
    
    @property
    def chaos_scripts_dir(self) -> Path:
        """Directory containing chaos script YAML files."""
        return self.base_dir / "chaos_scripts"
    
    @property
    def scenarios_dir(self) -> Path:
        """Directory containing scenario definitions."""
        return self.base_dir / "scenarios"
    
    @property
    def logs_dir(self) -> Path:
        """Directory for run logs."""
        return self.base_dir / "logs"
    
    @property
    def sandbox_dir(self) -> Path:
        """Directory containing Docker artifacts."""
        return self.base_dir / "sandbox"


# Global settings instance - lazy loaded
_settings: Settings | None = None


def get_settings() -> Settings:
    """
    Get the application settings instance.
    
    Uses lazy loading to defer configuration parsing until first use.
    This allows environment variables and .env files to be set up
    before the settings are accessed.
    
    Returns:
        Settings: The application configuration.
    """
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings


def reset_settings() -> None:
    """
    Reset the settings instance.
    
    Useful for testing or when environment variables change.
    """
    global _settings
    _settings = None
