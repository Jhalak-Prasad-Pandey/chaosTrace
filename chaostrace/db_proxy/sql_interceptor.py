"""
SQL Interceptor

Parses and classifies SQL statements using SQLGlot.
Extracts tables, columns, statement types, and estimates row impact.
"""

import hashlib
from dataclasses import dataclass, field
from typing import Any

import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError
from structlog import get_logger

from chaostrace.control_plane.models.events import SQLType

logger = get_logger(__name__)


@dataclass
class ParsedSQL:
    """
    Result of parsing a SQL statement.
    
    Contains extracted metadata useful for policy evaluation
    and risk assessment.
    """
    
    # Original statement
    raw_sql: str
    
    # Hash for deduplication/caching
    statement_hash: str
    
    # Classification
    sql_type: SQLType
    
    # Extracted metadata
    tables: list[str] = field(default_factory=list)
    columns: list[str] = field(default_factory=list)
    
    # Structural analysis
    has_where_clause: bool = False
    has_limit_clause: bool = False
    has_order_by: bool = False
    is_select_star: bool = False
    
    # Subqueries
    has_subquery: bool = False
    subquery_count: int = 0
    
    # Joins
    join_count: int = 0
    
    # Aggregations
    has_aggregation: bool = False
    
    # Transaction control
    is_transaction_control: bool = False
    
    # Parsing status
    parse_error: str | None = None
    is_valid: bool = True
    
    # For complex queries
    estimated_complexity: int = 1  # 1-10 scale


class SQLInterceptor:
    """
    Intercepts and parses SQL statements.
    
    Uses SQLGlot for dialect-aware parsing of SQL.
    Supports PostgreSQL-specific syntax.
    
    Usage:
        interceptor = SQLInterceptor()
        parsed = interceptor.parse("SELECT * FROM users WHERE id = 1")
        print(parsed.sql_type)  # SQLType.SELECT
        print(parsed.tables)    # ["users"]
    """
    
    # Map SQLGlot expression types to our SQLType enum
    TYPE_MAP: dict[type, SQLType] = {
        exp.Select: SQLType.SELECT,
        exp.Insert: SQLType.INSERT,
        exp.Update: SQLType.UPDATE,
        exp.Delete: SQLType.DELETE,
        exp.Create: SQLType.CREATE,
        exp.Alter: SQLType.ALTER,
        exp.Drop: SQLType.DROP,
        exp.Grant: SQLType.GRANT,
        exp.Transaction: SQLType.BEGIN,
        exp.Commit: SQLType.COMMIT,
        exp.Rollback: SQLType.ROLLBACK,
    }
    
    def __init__(self, dialect: str = "postgres"):
        """
        Initialize the SQL interceptor.
        
        Args:
            dialect: SQL dialect to use for parsing.
        """
        self.dialect = dialect
        logger.info("sql_interceptor_initialized", dialect=dialect)
    
    def parse(self, sql: str) -> ParsedSQL:
        """
        Parse a SQL statement and extract metadata.
        
        Args:
            sql: The raw SQL statement.
            
        Returns:
            ParsedSQL: Parsed SQL with extracted metadata.
        """
        sql = sql.strip()
        statement_hash = self._compute_hash(sql)
        
        # Handle empty statements
        if not sql:
            return ParsedSQL(
                raw_sql=sql,
                statement_hash=statement_hash,
                sql_type=SQLType.OTHER,
                is_valid=False,
                parse_error="Empty statement",
            )
        
        try:
            # Parse the SQL
            expressions = sqlglot.parse(sql, dialect=self.dialect)
            
            if not expressions:
                return ParsedSQL(
                    raw_sql=sql,
                    statement_hash=statement_hash,
                    sql_type=SQLType.OTHER,
                    is_valid=False,
                    parse_error="No expressions parsed",
                )
            
            # Analyze the first expression (main statement)
            main_expr = expressions[0]
            
            return self._analyze_expression(sql, statement_hash, main_expr)
            
        except ParseError as e:
            logger.warning(
                "sql_parse_error",
                sql_preview=sql[:100],
                error=str(e),
            )
            
            # Try to classify even if parsing failed
            sql_type = self._classify_by_prefix(sql)
            
            return ParsedSQL(
                raw_sql=sql,
                statement_hash=statement_hash,
                sql_type=sql_type,
                is_valid=False,
                parse_error=str(e),
            )
    
    def _analyze_expression(
        self,
        sql: str,
        statement_hash: str,
        expr: exp.Expression,
    ) -> ParsedSQL:
        """Analyze a parsed SQL expression."""
        # Determine SQL type
        sql_type = self._get_sql_type(expr)
        
        # Extract tables
        tables = self._extract_tables(expr)
        
        # Extract columns
        columns = self._extract_columns(expr)
        
        # Analyze structure
        has_where = self._has_where_clause(expr)
        has_limit = self._has_limit_clause(expr)
        has_order = self._has_order_by(expr)
        is_select_star = self._is_select_star(expr)
        
        # Count subqueries
        subqueries = list(expr.find_all(exp.Subquery))
        has_subquery = len(subqueries) > 0
        
        # Count joins
        joins = list(expr.find_all(exp.Join))
        
        # Check for aggregations
        aggregations = list(expr.find_all(
            exp.Count, exp.Sum, exp.Avg, exp.Min, exp.Max
        ))
        
        # Check for transaction control
        is_transaction = sql_type in (
            SQLType.BEGIN, SQLType.COMMIT, SQLType.ROLLBACK
        )
        
        # Estimate complexity
        complexity = self._estimate_complexity(
            expr, len(tables), len(joins), len(subqueries)
        )
        
        return ParsedSQL(
            raw_sql=sql,
            statement_hash=statement_hash,
            sql_type=sql_type,
            tables=tables,
            columns=columns,
            has_where_clause=has_where,
            has_limit_clause=has_limit,
            has_order_by=has_order,
            is_select_star=is_select_star,
            has_subquery=has_subquery,
            subquery_count=len(subqueries),
            join_count=len(joins),
            has_aggregation=len(aggregations) > 0,
            is_transaction_control=is_transaction,
            estimated_complexity=complexity,
        )
    
    def _get_sql_type(self, expr: exp.Expression) -> SQLType:
        """Determine the SQL type from an expression."""
        for expr_type, sql_type in self.TYPE_MAP.items():
            if isinstance(expr, expr_type):
                return sql_type
        
        # Check for TRUNCATE (not a standard SQLGlot type)
        sql_text = expr.sql().upper()
        if sql_text.startswith("TRUNCATE"):
            return SQLType.TRUNCATE
        
        return SQLType.OTHER
    
    def _extract_tables(self, expr: exp.Expression) -> list[str]:
        """Extract all referenced table names."""
        tables = set()
        
        # Find all table references
        for table in expr.find_all(exp.Table):
            if table.name:
                tables.add(table.name)
        
        return sorted(tables)
    
    def _extract_columns(self, expr: exp.Expression) -> list[str]:
        """Extract all referenced column names."""
        columns = set()
        
        # Find all column references
        for column in expr.find_all(exp.Column):
            if column.name:
                columns.add(column.name)
        
        return sorted(columns)
    
    def _has_where_clause(self, expr: exp.Expression) -> bool:
        """Check if the expression has a WHERE clause."""
        return len(list(expr.find_all(exp.Where))) > 0
    
    def _has_limit_clause(self, expr: exp.Expression) -> bool:
        """Check if the expression has a LIMIT clause."""
        return len(list(expr.find_all(exp.Limit))) > 0
    
    def _has_order_by(self, expr: exp.Expression) -> bool:
        """Check if the expression has an ORDER BY clause."""
        return len(list(expr.find_all(exp.Order))) > 0
    
    def _is_select_star(self, expr: exp.Expression) -> bool:
        """Check if this is a SELECT * query."""
        if not isinstance(expr, exp.Select):
            return False
        
        for star in expr.find_all(exp.Star):
            return True
        
        return False
    
    def _estimate_complexity(
        self,
        expr: exp.Expression,
        table_count: int,
        join_count: int,
        subquery_count: int,
    ) -> int:
        """
        Estimate query complexity on a 1-10 scale.
        
        Factors:
        - Number of tables
        - Number of joins
        - Number of subqueries
        - Presence of aggregations
        - Presence of window functions
        """
        complexity = 1
        
        # Tables
        complexity += min(table_count - 1, 2)  # +0 to +2
        
        # Joins
        complexity += min(join_count, 3)  # +0 to +3
        
        # Subqueries
        complexity += min(subquery_count * 2, 4)  # +0 to +4
        
        # Window functions
        if list(expr.find_all(exp.Window)):
            complexity += 1
        
        # CTEs
        if list(expr.find_all(exp.CTE)):
            complexity += 1
        
        return min(complexity, 10)
    
    def _classify_by_prefix(self, sql: str) -> SQLType:
        """Classify SQL by its first keyword (fallback)."""
        sql_upper = sql.upper().strip()
        
        prefixes = {
            "SELECT": SQLType.SELECT,
            "INSERT": SQLType.INSERT,
            "UPDATE": SQLType.UPDATE,
            "DELETE": SQLType.DELETE,
            "CREATE": SQLType.CREATE,
            "ALTER": SQLType.ALTER,
            "DROP": SQLType.DROP,
            "TRUNCATE": SQLType.TRUNCATE,
            "GRANT": SQLType.GRANT,
            "REVOKE": SQLType.REVOKE,
            "BEGIN": SQLType.BEGIN,
            "START": SQLType.BEGIN,
            "COMMIT": SQLType.COMMIT,
            "ROLLBACK": SQLType.ROLLBACK,
        }
        
        for prefix, sql_type in prefixes.items():
            if sql_upper.startswith(prefix):
                return sql_type
        
        return SQLType.OTHER
    
    def _compute_hash(self, sql: str) -> str:
        """Compute a hash of the SQL statement."""
        # Normalize whitespace for consistent hashing
        normalized = " ".join(sql.split())
        return hashlib.sha256(normalized.encode()).hexdigest()[:16]
    
    def normalize(self, sql: str) -> str:
        """
        Normalize SQL for comparison/caching.
        
        - Collapses whitespace
        - Lowercases keywords
        - Removes comments
        """
        try:
            parsed = sqlglot.parse_one(sql, dialect=self.dialect)
            return parsed.sql(dialect=self.dialect, normalize=True)
        except ParseError:
            # Fallback: basic normalization
            return " ".join(sql.split())
