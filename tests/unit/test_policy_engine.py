"""
Tests for Policy Engine
"""

import pytest

from chaostrace.control_plane.models.events import SQLType
from chaostrace.control_plane.models.policy import PolicyDefinition, PolicySeverity
from chaostrace.control_plane.services.policy_engine import PolicyEngine


class TestPolicyEngine:
    """Test suite for policy evaluation."""
    
    @pytest.fixture
    def strict_policy(self):
        """Create a strict policy for testing."""
        policy_data = {
            "name": "test_strict",
            "version": "1.0",
            "forbidden_sql": {
                "patterns": [
                    {"pattern": "DROP TABLE", "severity": "critical"},
                    {"pattern": "TRUNCATE", "severity": "critical"},
                    {"pattern": "DELETE.*FROM.*users.*$", "severity": "error"},
                ]
            },
            "table_restrictions": [
                {
                    "table": "users",
                    "operations": ["DELETE", "UPDATE"],
                    "require_where": True,
                    "max_rows": 100,
                }
            ],
            "row_limits": [
                {"operation": "DELETE", "max_rows": 500, "action": "error"},
            ],
            "honeypots": {
                "tables": ["_secrets", "_admin"],
                "severity": "critical",
            },
        }
        return PolicyEngine.from_dict(policy_data)
    
    # =========================================================================
    # Basic Pattern Tests
    # =========================================================================
    
    def test_allow_safe_select(self, strict_policy):
        """Test that safe SELECT queries are allowed."""
        result = strict_policy.evaluate(
            sql="SELECT * FROM users WHERE id = 1",
            sql_type=SQLType.SELECT,
            tables=["users"],
            has_where=True,
        )
        
        assert result.allowed is True
        assert len(result.violation_reasons) == 0
    
    def test_block_drop_table(self, strict_policy):
        """Test that DROP TABLE is blocked."""
        result = strict_policy.evaluate(
            sql="DROP TABLE users",
            sql_type=SQLType.DROP,
            tables=["users"],
        )
        
        assert result.allowed is False
        assert result.severity == PolicySeverity.CRITICAL
        assert any("DROP TABLE" in r for r in result.matched_rules)
    
    def test_block_truncate(self, strict_policy):
        """Test that TRUNCATE is blocked."""
        result = strict_policy.evaluate(
            sql="TRUNCATE TABLE users",
            sql_type=SQLType.TRUNCATE,
            tables=["users"],
        )
        
        assert result.allowed is False
        assert result.severity == PolicySeverity.CRITICAL
    
    # =========================================================================
    # Table Restriction Tests
    # =========================================================================
    
    def test_require_where_clause(self, strict_policy):
        """Test that WHERE clause is required for restricted tables."""
        result = strict_policy.evaluate(
            sql="DELETE FROM users",
            sql_type=SQLType.DELETE,
            tables=["users"],
            has_where=False,
        )
        
        assert result.allowed is False
        assert any("WHERE" in r for r in result.violation_reasons)
    
    def test_allow_delete_with_where(self, strict_policy):
        """Test that DELETE with WHERE clause is evaluated properly."""
        result = strict_policy.evaluate(
            sql="DELETE FROM users WHERE id = 1",
            sql_type=SQLType.DELETE,
            tables=["users"],
            has_where=True,
            estimated_rows=1,
        )
        
        # May be blocked by pattern, but WHERE should not be a violation
        where_violations = [r for r in result.violation_reasons if "WHERE" in r]
        assert len(where_violations) == 0
    
    def test_row_limit_exceeded(self, strict_policy):
        """Test that row limits are enforced."""
        result = strict_policy.evaluate(
            sql="DELETE FROM users WHERE is_active = false",
            sql_type=SQLType.DELETE,
            tables=["users"],
            has_where=True,
            estimated_rows=1000,  # Exceeds limit
        )
        
        assert result.allowed is False
        assert any("row" in r.lower() for r in result.violation_reasons)
    
    # =========================================================================
    # Honeypot Tests
    # =========================================================================
    
    def test_block_honeypot_access(self, strict_policy):
        """Test that honeypot table access is blocked."""
        result = strict_policy.evaluate(
            sql="SELECT * FROM _secrets",
            sql_type=SQLType.SELECT,
            tables=["_secrets"],
        )
        
        assert result.allowed is False
        assert result.severity == PolicySeverity.CRITICAL
        assert any("honeypot" in r.lower() for r in result.violation_reasons)
    
    # =========================================================================
    # Edge Cases
    # =========================================================================
    
    def test_query_length_limit(self, strict_policy):
        """Test that excessively long queries are blocked."""
        # Create a query longer than max_query_length
        long_sql = "SELECT * FROM users WHERE " + " OR ".join(
            [f"id = {i}" for i in range(2000)]
        )
        
        result = strict_policy.evaluate(
            sql=long_sql,
            sql_type=SQLType.SELECT,
            tables=["users"],
            has_where=True,
        )
        
        assert result.allowed is False
        assert any("length" in r.lower() for r in result.violation_reasons)
    
    def test_policy_action_conversion(self, strict_policy):
        """Test conversion of evaluation result to policy action."""
        from chaostrace.control_plane.models.events import PolicyAction
        
        # Allowed query
        result = strict_policy.evaluate(
            sql="SELECT 1",
            sql_type=SQLType.SELECT,
            tables=[],
        )
        action = strict_policy.get_policy_action(result)
        assert action == PolicyAction.ALLOW
        
        # Blocked query
        result = strict_policy.evaluate(
            sql="DROP TABLE users",
            sql_type=SQLType.DROP,
            tables=["users"],
        )
        action = strict_policy.get_policy_action(result)
        assert action == PolicyAction.BLOCK
