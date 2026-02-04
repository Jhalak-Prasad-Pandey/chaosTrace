"""
File System Operation Models

Pydantic models for file system events, policies, and analysis.
"""

from datetime import datetime, UTC
from enum import Enum
from pathlib import Path
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field


class FSOperationType(str, Enum):
    """Types of file system operations."""
    
    # Read operations
    READ = "read"
    OPEN = "open"
    STAT = "stat"
    LIST_DIR = "list_dir"
    READ_LINK = "read_link"
    
    # Write operations
    WRITE = "write"
    CREATE = "create"
    APPEND = "append"
    TRUNCATE = "truncate"
    
    # Modification operations
    CHMOD = "chmod"
    CHOWN = "chown"
    RENAME = "rename"
    MOVE = "move"
    COPY = "copy"
    
    # Delete operations
    DELETE = "delete"
    UNLINK = "unlink"
    RMDIR = "rmdir"
    RMTREE = "rmtree"
    
    # Directory operations
    MKDIR = "mkdir"
    MAKEDIRS = "makedirs"
    
    # Special operations
    SYMLINK = "symlink"
    HARDLINK = "hardlink"
    EXEC = "exec"


class FSRiskLevel(str, Enum):
    """Risk classification for file system operations."""
    
    LOW = "low"
    """Safe read-only operations."""
    
    MEDIUM = "medium"
    """Write operations to non-sensitive paths."""
    
    HIGH = "high"
    """Delete or modify operations."""
    
    CRITICAL = "critical"
    """Recursive deletes, system files, or honeypots."""


class FSPolicyAction(str, Enum):
    """Action taken by the FS policy engine."""
    
    ALLOW = "allow"
    BLOCK = "block"
    ALLOW_FLAGGED = "allow_flagged"


class FSEvent(BaseModel):
    """
    Event representing a file system operation.
    
    Captures all metadata about an intercepted operation
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
        description="When the operation occurred"
    )
    
    # Operation details
    operation: FSOperationType = Field(
        description="Type of file system operation"
    )
    
    path: str = Field(
        description="Primary path involved in the operation"
    )
    
    target_path: str | None = Field(
        default=None,
        description="Target path for move/copy/rename operations"
    )
    
    # Metadata
    is_directory: bool = Field(
        default=False,
        description="Whether the path is a directory"
    )
    
    file_size: int | None = Field(
        default=None,
        description="Size of the file in bytes"
    )
    
    permissions: str | None = Field(
        default=None,
        description="File permissions (e.g., '0755')"
    )
    
    # Risk assessment
    risk_level: FSRiskLevel = Field(
        description="Assessed risk level"
    )
    
    risk_factors: list[str] = Field(
        default_factory=list,
        description="Factors contributing to risk"
    )
    
    # Policy decision
    policy_action: FSPolicyAction = Field(
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
    
    # Execution result
    success: bool = Field(
        default=True,
        description="Whether the operation succeeded"
    )
    
    error_message: str | None = Field(
        default=None,
        description="Error message if operation failed"
    )
    
    latency_ms: float = Field(
        default=0.0,
        description="Time to complete the operation"
    )
    
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        description="Additional operation-specific metadata"
    )


class FSPathRule(BaseModel):
    """Rule for matching file paths."""
    
    pattern: str = Field(
        description="Glob pattern to match paths"
    )
    
    operations: list[FSOperationType] = Field(
        default_factory=list,
        description="Operations this rule applies to (empty = all)"
    )
    
    action: FSPolicyAction = Field(
        default=FSPolicyAction.BLOCK,
        description="Action to take when matched"
    )
    
    risk_level: FSRiskLevel = Field(
        default=FSRiskLevel.HIGH,
        description="Risk level for matched operations"
    )
    
    message: str = Field(
        default="",
        description="Message to log when matched"
    )


class FSHoneypot(BaseModel):
    """Honeypot file or directory configuration."""
    
    path: str = Field(
        description="Path to the honeypot"
    )
    
    is_directory: bool = Field(
        default=False,
        description="Whether this is a directory honeypot"
    )
    
    recursive: bool = Field(
        default=True,
        description="Whether to match recursively"
    )
    
    alert_on_list: bool = Field(
        default=False,
        description="Alert even on directory listing"
    )


class FSPolicy(BaseModel):
    """
    Complete file system policy definition.
    
    Example YAML:
    ```yaml
    name: strict_fs
    version: "1.0"
    
    protected_paths:
      - pattern: "/etc/**"
        operations: [WRITE, DELETE, CHMOD]
        action: block
        
      - pattern: "/root/**"
        action: block
    
    forbidden_operations:
      - operation: RMTREE
        message: "Recursive delete is not allowed"
    
    honeypots:
      - path: "/secrets/api_keys.txt"
      - path: "/backup/.archive"
        is_directory: true
    ```
    """
    
    name: str = Field(
        description="Unique name for this policy"
    )
    
    version: str = Field(
        default="1.0",
        description="Policy version"
    )
    
    description: str = Field(
        default="",
        description="Human-readable description"
    )
    
    # Path-based rules
    protected_paths: list[FSPathRule] = Field(
        default_factory=list,
        description="Paths with special restrictions"
    )
    
    allowed_paths: list[str] = Field(
        default_factory=list,
        description="Explicitly allowed path patterns"
    )
    
    # Operation restrictions
    forbidden_operations: list[dict] = Field(
        default_factory=list,
        description="Globally forbidden operations"
    )
    
    # Write restrictions
    max_file_size: int = Field(
        default=100 * 1024 * 1024,  # 100MB
        description="Maximum file size for writes"
    )
    
    max_files_per_operation: int = Field(
        default=1000,
        description="Maximum files affected per operation"
    )
    
    # Honeypots
    honeypots: list[FSHoneypot] = Field(
        default_factory=list,
        description="Honeypot files/directories"
    )
    
    # Behavior
    allow_symlinks: bool = Field(
        default=False,
        description="Whether to allow symlink creation"
    )
    
    allow_recursive_delete: bool = Field(
        default=False,
        description="Whether to allow rmtree operations"
    )
    
    sandbox_root: str = Field(
        default="/sandbox",
        description="Root path that agent should stay within"
    )


class FSPolicyResult(BaseModel):
    """Result of evaluating a file operation against policy."""
    
    allowed: bool = Field(
        description="Whether the operation is allowed"
    )
    
    flagged: bool = Field(
        default=False,
        description="Whether to flag for review"
    )
    
    risk_level: FSRiskLevel = Field(
        default=FSRiskLevel.LOW,
        description="Assessed risk level"
    )
    
    matched_rules: list[str] = Field(
        default_factory=list,
        description="Rules that matched"
    )
    
    violation_reasons: list[str] = Field(
        default_factory=list,
        description="Why the operation was blocked"
    )
    
    is_honeypot: bool = Field(
        default=False,
        description="Whether a honeypot was accessed"
    )
