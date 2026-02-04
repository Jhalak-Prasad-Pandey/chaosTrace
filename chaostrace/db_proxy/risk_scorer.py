"""
Risk Scorer

Assesses the risk level of SQL operations based on:
- Statement type (DDL vs DML)
- Tables involved
- Structural properties (WHERE clause, row limits)
- Historical patterns
"""

from dataclasses import dataclass

from structlog import get_logger

from chaostrace.control_plane.models.events import RiskLevel, SQLType
from chaostrace.db_proxy.sql_interceptor import ParsedSQL

logger = get_logger(__name__)


@dataclass
class RiskAssessment:
    """
    Result of risk assessment for a SQL statement.
    
    Provides a risk level with supporting factors and
    an optional row estimate.
    """
    
    risk_level: RiskLevel
    risk_factors: list[str]
    confidence: float  # 0.0 to 1.0
    rows_estimated: int | None = None
    recommendation: str = ""


class RiskScorer:
    """
    Scores the risk of SQL operations.
    
    Risk is determined by a combination of:
    1. Operation type (DROP > DELETE > UPDATE > INSERT > SELECT)
    2. Table sensitivity (configured per table)
    3. Structural safety (WHERE clause, LIMIT)
    4. Estimated row impact
    
    Usage:
        scorer = RiskScorer()
        assessment = scorer.assess(parsed_sql)
        print(assessment.risk_level)  # RiskLevel.HIGH
    """
    
    # Base risk by operation type
    BASE_RISK: dict[SQLType, RiskLevel] = {
        # DDL - highest risk
        SQLType.DROP: RiskLevel.CRITICAL,
        SQLType.TRUNCATE: RiskLevel.CRITICAL,
        SQLType.ALTER: RiskLevel.HIGH,
        SQLType.CREATE: RiskLevel.MEDIUM,
        
        # DML - varies by constraints
        SQLType.DELETE: RiskLevel.HIGH,
        SQLType.UPDATE: RiskLevel.MEDIUM,
        SQLType.INSERT: RiskLevel.LOW,
        SQLType.SELECT: RiskLevel.LOW,
        
        # Permissions
        SQLType.GRANT: RiskLevel.HIGH,
        SQLType.REVOKE: RiskLevel.HIGH,
        
        # Transaction control
        SQLType.BEGIN: RiskLevel.LOW,
        SQLType.COMMIT: RiskLevel.LOW,
        SQLType.ROLLBACK: RiskLevel.LOW,
        
        # Unknown
        SQLType.OTHER: RiskLevel.MEDIUM,
    }
    
    # Tables considered sensitive by default
    DEFAULT_SENSITIVE_TABLES: set[str] = {
        "users",
        "accounts",
        "passwords",
        "credentials",
        "secrets",
        "api_keys",
        "tokens",
        "sessions",
        "audit_logs",
        "payments",
        "transactions",
    }
    
    # Row thresholds for risk escalation
    ROW_THRESHOLDS = {
        "low_to_medium": 100,
        "medium_to_high": 1000,
        "high_to_critical": 10000,
    }
    
    def __init__(
        self,
        sensitive_tables: set[str] | None = None,
        row_thresholds: dict[str, int] | None = None,
    ):
        """
        Initialize the risk scorer.
        
        Args:
            sensitive_tables: Set of table names considered sensitive.
            row_thresholds: Custom row thresholds for risk escalation.
        """
        self.sensitive_tables = (
            sensitive_tables
            if sensitive_tables is not None
            else self.DEFAULT_SENSITIVE_TABLES
        )
        self.row_thresholds = row_thresholds or self.ROW_THRESHOLDS
        
        logger.info(
            "risk_scorer_initialized",
            sensitive_table_count=len(self.sensitive_tables),
        )
    
    def assess(self, parsed: ParsedSQL) -> RiskAssessment:
        """
        Assess the risk of a parsed SQL statement.
        
        Args:
            parsed: Parsed SQL statement.
            
        Returns:
            RiskAssessment: The risk assessment result.
        """
        factors: list[str] = []
        confidence = 1.0
        rows_estimated = None
        
        # Handle parse errors
        if not parsed.is_valid:
            factors.append("Parse error - treating as potentially risky")
            confidence = 0.5
        
        # Start with base risk for the operation type
        risk = self.BASE_RISK.get(parsed.sql_type, RiskLevel.MEDIUM)
        
        # Check for sensitive tables
        sensitive_tables_hit = [
            t for t in parsed.tables
            if t.lower() in {s.lower() for s in self.sensitive_tables}
        ]
        if sensitive_tables_hit:
            risk = self._escalate_risk(risk)
            factors.append(f"Sensitive table(s): {', '.join(sensitive_tables_hit)}")
        
        # Check for missing WHERE clause on DML
        if parsed.sql_type in (SQLType.DELETE, SQLType.UPDATE):
            if not parsed.has_where_clause:
                risk = self._escalate_risk(risk)
                factors.append(f"{parsed.sql_type.value.upper()} without WHERE clause")
        
        # Check for SELECT * (potential data exfiltration)
        if parsed.is_select_star:
            if sensitive_tables_hit:
                risk = max(risk, RiskLevel.MEDIUM)
                factors.append("SELECT * on sensitive table")
        
        # Analyze query complexity
        if parsed.estimated_complexity >= 7:
            factors.append(f"High query complexity ({parsed.estimated_complexity}/10)")
            if risk == RiskLevel.LOW:
                risk = RiskLevel.MEDIUM
        
        # Subquery analysis
        if parsed.subquery_count > 2:
            factors.append(f"Multiple subqueries ({parsed.subquery_count})")
            if risk == RiskLevel.LOW:
                risk = RiskLevel.MEDIUM
        
        # Estimate rows affected (if possible)
        rows_estimated = self._estimate_rows(parsed)
        if rows_estimated is not None:
            risk = self._adjust_risk_by_rows(risk, rows_estimated, factors)
        
        # Generate recommendation
        recommendation = self._generate_recommendation(parsed, risk, factors)
        
        logger.debug(
            "risk_assessment_complete",
            sql_type=parsed.sql_type.value,
            risk_level=risk.value,
            factor_count=len(factors),
            rows_estimated=rows_estimated,
        )
        
        return RiskAssessment(
            risk_level=risk,
            risk_factors=factors,
            confidence=confidence,
            rows_estimated=rows_estimated,
            recommendation=recommendation,
        )
    
    def _escalate_risk(self, risk: RiskLevel) -> RiskLevel:
        """Escalate risk to the next level."""
        levels = [RiskLevel.LOW, RiskLevel.MEDIUM, RiskLevel.HIGH, RiskLevel.CRITICAL]
        current_idx = levels.index(risk)
        new_idx = min(current_idx + 1, len(levels) - 1)
        return levels[new_idx]
    
    def _estimate_rows(self, parsed: ParsedSQL) -> int | None:
        """
        Estimate rows affected by the query.
        
        This is a placeholder for more sophisticated estimation.
        In production, this could:
        - Query table statistics
        - Use EXPLAIN to get row estimates
        - Apply heuristics based on WHERE clause structure
        """
        # For now, return None (unknown)
        # Future: integrate with actual table statistics
        
        # Heuristic: no WHERE = potentially many rows
        if parsed.sql_type in (SQLType.DELETE, SQLType.UPDATE):
            if not parsed.has_where_clause:
                return 1000000  # Assume worst case
        
        return None
    
    def _adjust_risk_by_rows(
        self,
        risk: RiskLevel,
        rows: int,
        factors: list[str],
    ) -> RiskLevel:
        """Adjust risk level based on row count."""
        levels = [RiskLevel.LOW, RiskLevel.MEDIUM, RiskLevel.HIGH, RiskLevel.CRITICAL]
        current_idx = levels.index(risk)
        
        if rows >= self.row_thresholds.get("high_to_critical", 10000):
            factors.append(f"Very high row impact ({rows:,} rows)")
            return RiskLevel.CRITICAL
        elif rows >= self.row_thresholds.get("medium_to_high", 1000):
            factors.append(f"High row impact ({rows:,} rows)")
            return levels[max(current_idx, 2)]  # At least HIGH
        elif rows >= self.row_thresholds.get("low_to_medium", 100):
            factors.append(f"Moderate row impact ({rows:,} rows)")
            return levels[max(current_idx, 1)]  # At least MEDIUM
        
        return risk
    
    def _generate_recommendation(
        self,
        parsed: ParsedSQL,
        risk: RiskLevel,
        factors: list[str],
    ) -> str:
        """Generate a human-readable recommendation."""
        if risk == RiskLevel.CRITICAL:
            if parsed.sql_type == SQLType.DROP:
                return "BLOCK: DROP statements are not allowed"
            elif parsed.sql_type == SQLType.TRUNCATE:
                return "BLOCK: TRUNCATE statements are not allowed"
            else:
                return "BLOCK: Operation has critical risk level"
        
        if risk == RiskLevel.HIGH:
            if not parsed.has_where_clause:
                return "BLOCK: Add WHERE clause to limit scope"
            return "FLAG: Review before allowing"
        
        if risk == RiskLevel.MEDIUM:
            return "ALLOW: Monitor for anomalies"
        
        return "ALLOW: Low risk operation"


# Convenience function for quick risk assessment
def assess_risk(sql: str, interceptor=None) -> RiskAssessment:
    """
    Convenience function to assess SQL risk.
    
    Args:
        sql: Raw SQL statement.
        interceptor: Optional SQLInterceptor instance.
        
    Returns:
        RiskAssessment: The risk assessment.
    """
    from chaostrace.db_proxy.sql_interceptor import SQLInterceptor
    
    interceptor = interceptor or SQLInterceptor()
    parsed = interceptor.parse(sql)
    scorer = RiskScorer()
    return scorer.assess(parsed)
