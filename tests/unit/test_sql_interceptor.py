"""
Tests for SQL Interceptor
"""

import pytest

from chaostrace.control_plane.models.events import SQLType
from chaostrace.db_proxy.sql_interceptor import SQLInterceptor


class TestSQLInterceptor:
    """Test suite for SQL statement parsing and classification."""
    
    @pytest.fixture
    def interceptor(self):
        """Create a SQL interceptor instance."""
        return SQLInterceptor()
    
    # =========================================================================
    # Statement Type Classification
    # =========================================================================
    
    def test_parse_select(self, interceptor):
        """Test SELECT statement parsing."""
        result = interceptor.parse("SELECT * FROM users WHERE id = 1")
        
        assert result.sql_type == SQLType.SELECT
        assert "users" in result.tables
        assert result.has_where_clause is True
        assert result.is_select_star is True
        assert result.is_valid is True
    
    def test_parse_select_columns(self, interceptor):
        """Test SELECT with specific columns."""
        result = interceptor.parse("SELECT id, name FROM users")
        
        assert result.sql_type == SQLType.SELECT
        assert result.is_select_star is False
        assert result.has_where_clause is False
    
    def test_parse_insert(self, interceptor):
        """Test INSERT statement parsing."""
        result = interceptor.parse(
            "INSERT INTO users (name, email) VALUES ('Test', 'test@example.com')"
        )
        
        assert result.sql_type == SQLType.INSERT
        assert "users" in result.tables
    
    def test_parse_update(self, interceptor):
        """Test UPDATE statement parsing."""
        result = interceptor.parse(
            "UPDATE users SET name = 'New Name' WHERE id = 1"
        )
        
        assert result.sql_type == SQLType.UPDATE
        assert "users" in result.tables
        assert result.has_where_clause is True
    
    def test_parse_update_without_where(self, interceptor):
        """Test UPDATE without WHERE clause."""
        result = interceptor.parse("UPDATE users SET is_active = false")
        
        assert result.sql_type == SQLType.UPDATE
        assert result.has_where_clause is False
    
    def test_parse_delete(self, interceptor):
        """Test DELETE statement parsing."""
        result = interceptor.parse("DELETE FROM users WHERE id = 1")
        
        assert result.sql_type == SQLType.DELETE
        assert "users" in result.tables
        assert result.has_where_clause is True
    
    def test_parse_delete_without_where(self, interceptor):
        """Test DELETE without WHERE clause."""
        result = interceptor.parse("DELETE FROM users")
        
        assert result.sql_type == SQLType.DELETE
        assert result.has_where_clause is False
    
    def test_parse_drop_table(self, interceptor):
        """Test DROP TABLE parsing."""
        result = interceptor.parse("DROP TABLE users")
        
        assert result.sql_type == SQLType.DROP
        assert "users" in result.tables
    
    def test_parse_truncate(self, interceptor):
        """Test TRUNCATE parsing."""
        result = interceptor.parse("TRUNCATE TABLE users")
        
        assert result.sql_type == SQLType.TRUNCATE
    
    # =========================================================================
    # Complex Queries
    # =========================================================================
    
    def test_parse_join(self, interceptor):
        """Test JOIN query parsing."""
        result = interceptor.parse("""
            SELECT u.name, a.balance 
            FROM users u 
            JOIN accounts a ON u.id = a.user_id 
            WHERE u.is_active = true
        """)
        
        assert result.sql_type == SQLType.SELECT
        assert "users" in result.tables
        assert "accounts" in result.tables
        assert result.join_count >= 1
        assert result.has_where_clause is True
    
    def test_parse_subquery(self, interceptor):
        """Test subquery parsing."""
        result = interceptor.parse("""
            SELECT * FROM users 
            WHERE id IN (SELECT user_id FROM accounts WHERE balance > 1000)
        """)
        
        assert result.sql_type == SQLType.SELECT
        assert result.has_subquery is True
        assert result.subquery_count >= 1
    
    def test_parse_complex_query(self, interceptor):
        """Test complex query with multiple features."""
        result = interceptor.parse("""
            SELECT 
                u.name,
                COUNT(t.id) as transaction_count,
                SUM(t.amount) as total_amount
            FROM users u
            JOIN accounts a ON u.id = a.user_id
            JOIN transactions t ON a.id = t.account_id
            WHERE u.is_active = true
            GROUP BY u.id, u.name
            HAVING SUM(t.amount) > 1000
            ORDER BY total_amount DESC
            LIMIT 10
        """)
        
        assert result.sql_type == SQLType.SELECT
        assert result.has_where_clause is True
        assert result.has_limit_clause is True
        assert result.has_order_by is True
        assert result.has_aggregation is True
        assert result.join_count >= 2
        assert result.estimated_complexity >= 5
    
    # =========================================================================
    # Edge Cases
    # =========================================================================
    
    def test_parse_empty_statement(self, interceptor):
        """Test empty statement handling."""
        result = interceptor.parse("")
        
        assert result.is_valid is False
        assert result.sql_type == SQLType.OTHER
    
    def test_parse_whitespace_only(self, interceptor):
        """Test whitespace-only statement handling."""
        result = interceptor.parse("   \n\t   ")
        
        assert result.is_valid is False
    
    def test_parse_invalid_sql(self, interceptor):
        """Test invalid SQL handling."""
        result = interceptor.parse("THIS IS NOT SQL AT ALL")
        
        assert result.is_valid is False
        assert result.parse_error is not None
    
    def test_statement_hash_consistency(self, interceptor):
        """Test that equivalent statements have same hash."""
        sql1 = "SELECT * FROM users"
        sql2 = "SELECT  *  FROM  users"  # Extra whitespace
        
        result1 = interceptor.parse(sql1)
        result2 = interceptor.parse(sql2)
        
        # Hashes should be the same after normalization
        assert result1.statement_hash == result2.statement_hash
    
    def test_statement_hash_uniqueness(self, interceptor):
        """Test that different statements have different hashes."""
        result1 = interceptor.parse("SELECT * FROM users")
        result2 = interceptor.parse("SELECT * FROM accounts")
        
        assert result1.statement_hash != result2.statement_hash
