"""
Tests for File System Interceptor
"""

import os
import tempfile
from pathlib import Path
from uuid import uuid4

import pytest

from chaostrace.fs_proxy.interceptor import FSInterceptor
from chaostrace.fs_proxy.models import (
    FSOperationType,
    FSPolicyAction,
    FSPolicyResult,
    FSRiskLevel,
)


class TestFSInterceptor:
    """Test suite for filesystem interceptor."""
    
    @pytest.fixture
    def temp_sandbox(self):
        """Create a temporary sandbox directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)
    
    @pytest.fixture
    def interceptor(self, temp_sandbox):
        """Create an interceptor for the temp sandbox."""
        return FSInterceptor(
            run_id=uuid4(),
            sandbox_root=str(temp_sandbox),
        )
    
    def test_read_file(self, interceptor, temp_sandbox):
        """Test reading a file."""
        test_file = temp_sandbox / "test.txt"
        test_file.write_text("Hello, World!")
        
        content = interceptor.read_file(test_file)
        assert content == "Hello, World!"
    
    def test_write_file(self, interceptor, temp_sandbox):
        """Test writing a file."""
        test_file = temp_sandbox / "output.txt"
        
        bytes_written = interceptor.write_file(test_file, "Test content")
        assert bytes_written > 0
        assert test_file.read_text() == "Test content"
    
    def test_create_file(self, interceptor, temp_sandbox):
        """Test creating a file."""
        new_file = temp_sandbox / "new_file.txt"
        
        result = interceptor.create_file(new_file, "Initial content")
        assert result.exists()
        assert result.read_text() == "Initial content"
    
    def test_delete_file(self, interceptor, temp_sandbox):
        """Test deleting a file."""
        test_file = temp_sandbox / "to_delete.txt"
        test_file.write_text("Delete me")
        
        interceptor.delete_file(test_file)
        assert not test_file.exists()
    
    def test_list_dir(self, interceptor, temp_sandbox):
        """Test listing directory contents."""
        (temp_sandbox / "file1.txt").write_text("1")
        (temp_sandbox / "file2.txt").write_text("2")
        
        entries = interceptor.list_dir(temp_sandbox)
        assert "file1.txt" in entries
        assert "file2.txt" in entries
    
    def test_make_directory(self, interceptor, temp_sandbox):
        """Test creating a directory."""
        new_dir = temp_sandbox / "new_dir"
        
        result = interceptor.make_directory(new_dir)
        assert result.is_dir()
    
    def test_make_directory_with_parents(self, interceptor, temp_sandbox):
        """Test creating nested directories."""
        nested_dir = temp_sandbox / "a" / "b" / "c"
        
        result = interceptor.make_directory(nested_dir, parents=True)
        assert result.is_dir()
    
    def test_rename_file(self, interceptor, temp_sandbox):
        """Test renaming a file."""
        old_path = temp_sandbox / "old_name.txt"
        new_path = temp_sandbox / "new_name.txt"
        old_path.write_text("Content")
        
        result = interceptor.rename(old_path, new_path)
        assert result == new_path.resolve()
        assert not old_path.exists()
        assert new_path.exists()
    
    def test_copy_file(self, interceptor, temp_sandbox):
        """Test copying a file."""
        src = temp_sandbox / "source.txt"
        dst = temp_sandbox / "destination.txt"
        src.write_text("Copy me")
        
        result = interceptor.copy(src, dst)
        assert result == dst.resolve()
        assert src.exists()  # Original still exists
        assert dst.exists()
        assert dst.read_text() == "Copy me"
    
    def test_stats(self, interceptor, temp_sandbox):
        """Test operation statistics."""
        test_file = temp_sandbox / "test.txt"
        test_file.write_text("Stats test")
        
        interceptor.read_file(test_file)
        interceptor.stat_file(test_file)
        interceptor.list_dir(temp_sandbox)
        
        stats = interceptor.stats
        assert stats["operation_count"] == 3
        assert stats["blocked_count"] == 0


class TestFSInterceptorWithPolicy:
    """Test filesystem interceptor with policy enforcement."""
    
    @pytest.fixture
    def temp_sandbox(self):
        """Create a temporary sandbox directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)
    
    @pytest.fixture
    def blocking_policy(self):
        """Create a policy that blocks all deletes."""
        def evaluator(operation, path, target_path=None):
            if operation in (FSOperationType.DELETE, FSOperationType.RMDIR, FSOperationType.RMTREE):
                return FSPolicyResult(
                    allowed=False,
                    risk_level=FSRiskLevel.HIGH,
                    violation_reasons=["Delete operations are blocked"],
                )
            return FSPolicyResult(allowed=True, risk_level=FSRiskLevel.LOW)
        return evaluator
    
    @pytest.fixture
    def interceptor_with_policy(self, temp_sandbox, blocking_policy):
        """Create an interceptor with blocking policy."""
        return FSInterceptor(
            run_id=uuid4(),
            sandbox_root=str(temp_sandbox),
            policy_evaluator=blocking_policy,
        )
    
    def test_policy_blocks_delete(self, interceptor_with_policy, temp_sandbox):
        """Test that policy blocks delete operations."""
        test_file = temp_sandbox / "protected.txt"
        test_file.write_text("Protected content")
        
        with pytest.raises(PermissionError) as exc:
            interceptor_with_policy.delete_file(test_file)
        
        assert "blocked" in str(exc.value).lower()
        assert test_file.exists()  # File should still exist
    
    def test_policy_allows_read(self, interceptor_with_policy, temp_sandbox):
        """Test that policy allows read operations."""
        test_file = temp_sandbox / "test.txt"
        test_file.write_text("Read me")
        
        content = interceptor_with_policy.read_file(test_file)
        assert content == "Read me"
