"""
Run Models

Pydantic models for test run lifecycle management including
request/response schemas and internal state representation.
"""

from datetime import datetime, UTC
from enum import Enum
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field, field_validator


class AgentType(str, Enum):
    """Supported agent integration types."""
    
    PYTHON = "python"
    """Python script agent mounted as volume."""
    
    OPENAI = "openai"
    """OpenAI-compatible agent with tool calls."""
    
    LANGCHAIN = "langchain"
    """LangChain agent with custom tools."""
    
    CUSTOM = "custom"
    """Custom agent with HTTP callback interface."""


class RunStatus(str, Enum):
    """Current status of a test run."""
    
    PENDING = "pending"
    """Run created but not yet started."""
    
    INITIALIZING = "initializing"
    """Containers being created and configured."""
    
    RUNNING = "running"
    """Agent is executing in the sandbox."""
    
    COMPLETED = "completed"
    """Run finished successfully (agent exited normally)."""
    
    FAILED = "failed"
    """Run failed due to error or violation."""
    
    TERMINATED = "terminated"
    """Run was manually terminated or timed out."""
    
    CLEANUP = "cleanup"
    """Containers being removed and logs archived."""


class Verdict(str, Enum):
    """Final assessment of agent behavior during the run."""
    
    PASS = "pass"
    """Agent handled all chaos events safely."""
    
    FAIL = "fail"
    """Agent exhibited unsafe behavior."""
    
    WARN = "warn"
    """Agent completed but with concerning patterns."""
    
    INCOMPLETE = "incomplete"
    """Run did not complete normally."""


class RunRequest(BaseModel):
    """
    Request schema for creating a new test run.
    
    This defines all parameters needed to set up an isolated
    sandbox environment and execute an agent test.
    """
    
    agent_type: AgentType = Field(
        description="Type of agent integration to use"
    )
    
    agent_entry: str = Field(
        description="Agent entry point (file path, module, or code)",
        min_length=1
    )
    
    scenario: str = Field(
        description="Name of the scenario YAML file (without extension)",
        min_length=1,
        pattern=r"^[a-z0-9_]*$"
    )
    
    policy_profile: str = Field(
        default="strict",
        description="Name of the policy YAML file to use",
        pattern=r"^[a-z0-9_]*$"
    )
    
    chaos_profile: str | None = Field(
        default=None,
        description="Name of the chaos script YAML file (optional)",
        pattern=r"^[a-z0-9_]*$"
    )
    
    timeout_seconds: int = Field(
        default=300,
        description="Maximum run duration in seconds",
        ge=10,
        le=3600
    )
    
    environment: dict[str, str] = Field(
        default_factory=dict,
        description="Additional environment variables for the agent"
    )
    
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        description="Custom metadata to attach to the run"
    )
    
    @field_validator("agent_entry")
    @classmethod
    def validate_agent_entry(cls, v: str, info) -> str:
        """Validate agent entry based on agent type."""
        # Additional validation can be added based on agent_type
        return v.strip()


class RunResponse(BaseModel):
    """Response schema after creating a run."""
    
    run_id: UUID = Field(
        description="Unique identifier for this run"
    )
    
    status: RunStatus = Field(
        description="Current status of the run"
    )
    
    created_at: datetime = Field(
        description="Timestamp when the run was created"
    )
    
    message: str = Field(
        description="Human-readable status message"
    )


class RunState(BaseModel):
    """
    Internal state representation of a test run.
    
    This contains all information needed to track and manage
    a run throughout its lifecycle.
    """
    
    run_id: UUID = Field(
        description="Unique identifier for this run"
    )
    
    request: RunRequest = Field(
        description="Original request that created this run"
    )
    
    status: RunStatus = Field(
        default=RunStatus.PENDING,
        description="Current status of the run"
    )
    
    verdict: Verdict | None = Field(
        default=None,
        description="Final verdict (set when run completes)"
    )
    
    # Timestamps
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        description="When the run was created"
    )
    
    started_at: datetime | None = Field(
        default=None,
        description="When the agent started executing"
    )
    
    ended_at: datetime | None = Field(
        default=None,
        description="When the run completed/failed/terminated"
    )
    
    # Container IDs (set during initialization)
    network_id: str | None = Field(
        default=None,
        description="Docker network ID for this run"
    )
    
    postgres_container_id: str | None = Field(
        default=None,
        description="PostgreSQL container ID"
    )
    
    proxy_container_id: str | None = Field(
        default=None,
        description="DB Proxy container ID"
    )
    
    agent_container_id: str | None = Field(
        default=None,
        description="Agent runner container ID"
    )
    
    # Metrics (updated during run)
    total_sql_events: int = Field(
        default=0,
        description="Total SQL statements intercepted"
    )
    
    blocked_events: int = Field(
        default=0,
        description="Number of blocked SQL statements"
    )
    
    chaos_events_triggered: int = Field(
        default=0,
        description="Number of chaos events injected"
    )
    
    violations: list[str] = Field(
        default_factory=list,
        description="List of policy violations"
    )
    
    # Error handling
    error_message: str | None = Field(
        default=None,
        description="Error message if run failed"
    )
    
    class Config:
        """Pydantic configuration."""
        use_enum_values = True


class RunSummary(BaseModel):
    """Summary of a completed run for listing endpoints."""
    
    run_id: UUID
    status: RunStatus
    verdict: Verdict | None
    scenario: str
    policy_profile: str
    created_at: datetime
    duration_seconds: float | None
    total_sql_events: int
    blocked_events: int


class RunListResponse(BaseModel):
    """Response for listing runs."""
    
    runs: list[RunSummary]
    total: int
    page: int
    page_size: int
