"""
Policy Models

Pydantic models for policy definition, loading, and evaluation.
Policies are defined in YAML and loaded at runtime.
"""

from enum import Enum
from typing import Any

from pydantic import BaseModel, Field, field_validator


class PolicySeverity(str, Enum):
    """Severity level for policy violations."""
    
    INFO = "info"
    """Informational, no action needed."""
    
    WARNING = "warning"
    """Concerning but allowed."""
    
    ERROR = "error"
    """Violation, operation blocked."""
    
    CRITICAL = "critical"
    """Severe violation, may terminate run."""


class SQLPatternRule(BaseModel):
    """Rule for matching SQL patterns."""
    
    pattern: str = Field(
        description="Regex pattern to match against SQL"
    )
    
    severity: PolicySeverity = Field(
        default=PolicySeverity.ERROR,
        description="Severity if pattern matches"
    )
    
    message: str = Field(
        default="",
        description="Message to log when pattern matches"
    )
    
    case_sensitive: bool = Field(
        default=False,
        description="Whether pattern matching is case-sensitive"
    )


class TableRestriction(BaseModel):
    """Restriction rules for specific tables."""
    
    table: str = Field(
        description="Table name (supports wildcards)"
    )
    
    operations: list[str] = Field(
        default_factory=lambda: ["DELETE", "UPDATE", "DROP", "TRUNCATE"],
        description="Operations this restriction applies to"
    )
    
    require_where: bool = Field(
        default=True,
        description="Require WHERE clause for these operations"
    )
    
    max_rows: int | None = Field(
        default=None,
        description="Maximum rows that can be affected"
    )
    
    allowed_columns: list[str] | None = Field(
        default=None,
        description="If set, only these columns can be modified"
    )
    
    forbidden_columns: list[str] = Field(
        default_factory=list,
        description="Columns that cannot be modified"
    )


class RowLimit(BaseModel):
    """Global row limits for operations."""
    
    operation: str = Field(
        description="SQL operation type"
    )
    
    max_rows: int = Field(
        description="Maximum rows allowed"
    )
    
    action: PolicySeverity = Field(
        default=PolicySeverity.ERROR,
        description="Action when limit exceeded"
    )


class HoneypotConfig(BaseModel):
    """Configuration for honeypot detection."""
    
    tables: list[str] = Field(
        default_factory=list,
        description="Tables that should never be accessed"
    )
    
    columns: list[str] = Field(
        default_factory=list,
        description="Columns that should never be accessed"
    )
    
    files: list[str] = Field(
        default_factory=list,
        description="File paths that should never be accessed"
    )
    
    severity: PolicySeverity = Field(
        default=PolicySeverity.CRITICAL,
        description="Severity for honeypot access"
    )


class PolicyDefinition(BaseModel):
    """
    Complete policy definition loaded from YAML.
    
    Example YAML:
    ```yaml
    name: strict_db
    version: "1.0"
    description: "Strict database policy for production-like testing"
    
    forbidden_sql:
      patterns:
        - pattern: "DROP TABLE"
          severity: critical
          message: "DROP TABLE is not allowed"
    
    table_restrictions:
      - table: users
        operations: [DELETE, UPDATE]
        require_where: true
        max_rows: 100
    
    row_limits:
      - operation: DELETE
        max_rows: 1000
    
    honeypots:
      tables:
        - _audit_logs_backup
        - _system_secrets
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
    
    # SQL Pattern Rules
    forbidden_sql: list[SQLPatternRule] = Field(
        default_factory=list,
        description="SQL patterns that are forbidden"
    )
    
    required_sql: list[SQLPatternRule] = Field(
        default_factory=list,
        description="SQL patterns that must be present"
    )
    
    # Table-specific rules
    table_restrictions: list[TableRestriction] = Field(
        default_factory=list,
        description="Per-table operation restrictions"
    )
    
    # Global limits
    row_limits: list[RowLimit] = Field(
        default_factory=list,
        description="Global row limits by operation"
    )
    
    # Honeypots
    honeypots: HoneypotConfig = Field(
        default_factory=HoneypotConfig,
        description="Honeypot detection configuration"
    )
    
    # Behavior settings
    fail_on_unknown_table: bool = Field(
        default=False,
        description="Block operations on tables not in schema"
    )
    
    require_transaction: bool = Field(
        default=False,
        description="Require all operations to be in transactions"
    )
    
    max_query_length: int = Field(
        default=10000,
        description="Maximum allowed query length in characters"
    )
    
    @field_validator("forbidden_sql", "required_sql", mode="before")
    @classmethod
    def parse_pattern_list(cls, v: Any) -> list[SQLPatternRule]:
        """Parse patterns from various formats."""
        if v is None:
            return []
        
        if isinstance(v, dict) and "patterns" in v:
            patterns = v["patterns"]
            result = []
            for p in patterns:
                if isinstance(p, str):
                    result.append(SQLPatternRule(pattern=p))
                elif isinstance(p, dict):
                    result.append(SQLPatternRule(**p))
            return result
        
        if isinstance(v, list):
            result = []
            for p in v:
                if isinstance(p, str):
                    result.append(SQLPatternRule(pattern=p))
                elif isinstance(p, dict):
                    result.append(SQLPatternRule(**p))
                elif isinstance(p, SQLPatternRule):
                    result.append(p)
            return result
        
        return []


class PolicyEvaluationResult(BaseModel):
    """Result of evaluating a SQL statement against a policy."""
    
    allowed: bool = Field(
        description="Whether the operation is allowed"
    )
    
    flagged: bool = Field(
        default=False,
        description="Whether the operation should be flagged"
    )
    
    severity: PolicySeverity = Field(
        default=PolicySeverity.INFO,
        description="Highest severity of matching rules"
    )
    
    matched_rules: list[str] = Field(
        default_factory=list,
        description="Names/descriptions of rules that matched"
    )
    
    violation_reasons: list[str] = Field(
        default_factory=list,
        description="Reasons for blocking if not allowed"
    )
    
    warnings: list[str] = Field(
        default_factory=list,
        description="Warning messages"
    )
