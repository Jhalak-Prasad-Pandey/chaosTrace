"""
Policy Engine Service

YAML-based policy evaluation for SQL statements.
Supports forbidden patterns, table restrictions, row limits,
and honeypot detection.
"""

import re
from pathlib import Path
from typing import Any

import yaml
from structlog import get_logger

from chaostrace.control_plane.models.events import PolicyAction, RiskLevel, SQLType
from chaostrace.control_plane.models.policy import (
    PolicyDefinition,
    PolicyEvaluationResult,
    PolicySeverity,
    SQLPatternRule,
    TableRestriction,
)

logger = get_logger(__name__)


class PolicyEngine:
    """
    Evaluates SQL statements against loaded policies.
    
    The engine supports:
    - Forbidden SQL pattern matching (regex)
    - Table-specific operation restrictions
    - Global row limits by operation type
    - Honeypot detection for sensitive resources
    
    Usage:
        engine = PolicyEngine.from_file("policies/strict.yaml")
        result = engine.evaluate(
            sql="DELETE FROM users WHERE id = 1",
            sql_type=SQLType.DELETE,
            tables=["users"],
            has_where=True,
            estimated_rows=1
        )
        if not result.allowed:
            print(f"Blocked: {result.violation_reasons}")
    """
    
    def __init__(self, policy: PolicyDefinition):
        """
        Initialize the policy engine with a policy definition.
        
        Args:
            policy: The policy definition to enforce.
        """
        self.policy = policy
        self._compiled_patterns: list[tuple[re.Pattern, SQLPatternRule]] = []
        self._compile_patterns()
        
        logger.info(
            "policy_engine_initialized",
            policy_name=policy.name,
            pattern_count=len(self._compiled_patterns),
            table_restriction_count=len(policy.table_restrictions),
        )
    
    def _compile_patterns(self) -> None:
        """Pre-compile regex patterns for performance."""
        for rule in self.policy.forbidden_sql:
            flags = 0 if rule.case_sensitive else re.IGNORECASE
            try:
                pattern = re.compile(rule.pattern, flags)
                self._compiled_patterns.append((pattern, rule))
            except re.error as e:
                logger.error(
                    "invalid_pattern",
                    pattern=rule.pattern,
                    error=str(e)
                )
    
    @classmethod
    def from_file(cls, path: Path | str) -> "PolicyEngine":
        """
        Load a policy from a YAML file.
        
        Args:
            path: Path to the policy YAML file.
            
        Returns:
            PolicyEngine: Configured policy engine.
            
        Raises:
            FileNotFoundError: If the policy file doesn't exist.
            ValueError: If the policy YAML is invalid.
        """
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"Policy file not found: {path}")
        
        with open(path) as f:
            data = yaml.safe_load(f)
        
        policy = PolicyDefinition(**data)
        return cls(policy)
    
    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PolicyEngine":
        """Create a policy engine from a dictionary."""
        policy = PolicyDefinition(**data)
        return cls(policy)
    
    def evaluate(
        self,
        sql: str,
        sql_type: SQLType,
        tables: list[str],
        has_where: bool = False,
        estimated_rows: int | None = None,
        columns: list[str] | None = None,
    ) -> PolicyEvaluationResult:
        """
        Evaluate a SQL statement against the policy.
        
        Args:
            sql: The raw SQL statement.
            sql_type: Classified type of the SQL statement.
            tables: List of tables referenced in the statement.
            has_where: Whether the statement has a WHERE clause.
            estimated_rows: Estimated number of rows affected.
            columns: Columns being modified (for UPDATE/INSERT).
            
        Returns:
            PolicyEvaluationResult: The evaluation result.
        """
        result = PolicyEvaluationResult(allowed=True)
        columns = columns or []
        
        # Check query length
        if len(sql) > self.policy.max_query_length:
            result.allowed = False
            result.severity = PolicySeverity.ERROR
            result.violation_reasons.append(
                f"Query exceeds maximum length ({len(sql)} > {self.policy.max_query_length})"
            )
            return result
        
        # Check forbidden patterns
        self._check_forbidden_patterns(sql, result)
        
        # Check honeypots
        self._check_honeypots(tables, columns, result)
        
        # Check table restrictions
        for table in tables:
            self._check_table_restrictions(
                table=table,
                sql_type=sql_type,
                has_where=has_where,
                estimated_rows=estimated_rows,
                columns=columns,
                result=result,
            )
        
        # Check global row limits
        self._check_row_limits(sql_type, estimated_rows, result)
        
        # Determine final allowed status based on severity
        if result.severity in (PolicySeverity.ERROR, PolicySeverity.CRITICAL):
            result.allowed = False
        elif result.severity == PolicySeverity.WARNING:
            result.flagged = True
        
        logger.debug(
            "policy_evaluation_complete",
            sql_preview=sql[:100],
            allowed=result.allowed,
            severity=result.severity.value,
            matched_rules=result.matched_rules,
        )
        
        return result
    
    def _check_forbidden_patterns(
        self,
        sql: str,
        result: PolicyEvaluationResult
    ) -> None:
        """Check SQL against forbidden patterns."""
        # Severity ordering for comparison
        severity_order = {
            PolicySeverity.INFO: 0,
            PolicySeverity.WARNING: 1,
            PolicySeverity.ERROR: 2,
            PolicySeverity.CRITICAL: 3,
        }
        
        for pattern, rule in self._compiled_patterns:
            if pattern.search(sql):
                result.matched_rules.append(f"forbidden_pattern:{rule.pattern}")
                
                # Update severity if the rule's severity is higher
                if severity_order.get(rule.severity, 0) >= severity_order.get(result.severity, 0):
                    result.severity = rule.severity
                
                message = rule.message or f"Matched forbidden pattern: {rule.pattern}"
                
                if rule.severity in (PolicySeverity.ERROR, PolicySeverity.CRITICAL):
                    result.violation_reasons.append(message)
                else:
                    result.warnings.append(message)
    
    def _check_honeypots(
        self,
        tables: list[str],
        columns: list[str],
        result: PolicyEvaluationResult
    ) -> None:
        """Check for honeypot access."""
        honeypots = self.policy.honeypots
        
        for table in tables:
            if table in honeypots.tables:
                result.severity = honeypots.severity
                result.violation_reasons.append(
                    f"Access to honeypot table: {table}"
                )
                result.matched_rules.append(f"honeypot_table:{table}")
        
        for column in columns:
            if column in honeypots.columns:
                result.severity = honeypots.severity
                result.violation_reasons.append(
                    f"Access to honeypot column: {column}"
                )
                result.matched_rules.append(f"honeypot_column:{column}")
    
    def _check_table_restrictions(
        self,
        table: str,
        sql_type: SQLType,
        has_where: bool,
        estimated_rows: int | None,
        columns: list[str],
        result: PolicyEvaluationResult,
    ) -> None:
        """Check table-specific restrictions."""
        for restriction in self.policy.table_restrictions:
            if not self._table_matches(table, restriction.table):
                continue
            
            operation = sql_type.value.upper()
            if operation not in [op.upper() for op in restriction.operations]:
                continue
            
            # Check WHERE clause requirement
            if restriction.require_where and not has_where:
                if sql_type in (SQLType.DELETE, SQLType.UPDATE):
                    result.severity = PolicySeverity.ERROR
                    result.violation_reasons.append(
                        f"{operation} on {table} requires WHERE clause"
                    )
                    result.matched_rules.append(
                        f"require_where:{table}:{operation}"
                    )
            
            # Check row limit
            if restriction.max_rows is not None and estimated_rows is not None:
                if estimated_rows > restriction.max_rows:
                    result.severity = PolicySeverity.ERROR
                    result.violation_reasons.append(
                        f"{operation} on {table} affects too many rows "
                        f"({estimated_rows} > {restriction.max_rows})"
                    )
                    result.matched_rules.append(
                        f"row_limit:{table}:{restriction.max_rows}"
                    )
            
            # Check forbidden columns
            for col in columns:
                if col in restriction.forbidden_columns:
                    result.severity = PolicySeverity.ERROR
                    result.violation_reasons.append(
                        f"Column {col} is forbidden for modification"
                    )
                    result.matched_rules.append(f"forbidden_column:{col}")
            
            # Check allowed columns
            if restriction.allowed_columns is not None:
                for col in columns:
                    if col not in restriction.allowed_columns:
                        result.severity = PolicySeverity.ERROR
                        result.violation_reasons.append(
                            f"Column {col} is not in allowed list"
                        )
                        result.matched_rules.append(f"not_allowed_column:{col}")
    
    def _check_row_limits(
        self,
        sql_type: SQLType,
        estimated_rows: int | None,
        result: PolicyEvaluationResult,
    ) -> None:
        """Check global row limits."""
        if estimated_rows is None:
            return
        
        for limit in self.policy.row_limits:
            if limit.operation.upper() != sql_type.value.upper():
                continue
            
            if estimated_rows > limit.max_rows:
                if limit.action.value >= result.severity.value:
                    result.severity = limit.action
                
                message = (
                    f"{sql_type.value.upper()} affects too many rows "
                    f"({estimated_rows} > {limit.max_rows})"
                )
                
                if limit.action in (PolicySeverity.ERROR, PolicySeverity.CRITICAL):
                    result.violation_reasons.append(message)
                else:
                    result.warnings.append(message)
                
                result.matched_rules.append(
                    f"global_row_limit:{sql_type.value}:{limit.max_rows}"
                )
    
    def _table_matches(self, table: str, pattern: str) -> bool:
        """Check if a table name matches a pattern (supports wildcards)."""
        if pattern == "*":
            return True
        if "*" in pattern:
            # Convert glob pattern to regex
            regex = pattern.replace("*", ".*")
            return bool(re.match(f"^{regex}$", table, re.IGNORECASE))
        return table.lower() == pattern.lower()
    
    def get_policy_action(self, result: PolicyEvaluationResult) -> PolicyAction:
        """Convert evaluation result to a policy action."""
        if not result.allowed:
            return PolicyAction.BLOCK
        if result.flagged:
            return PolicyAction.ALLOW_FLAGGED
        return PolicyAction.ALLOW
