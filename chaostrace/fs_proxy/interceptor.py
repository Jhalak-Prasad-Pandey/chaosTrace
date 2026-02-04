"""
File System Interceptor

Intercepts and logs file system operations, wrapping Python's
standard file operations with monitoring and policy enforcement.
"""

import fnmatch
import os
import shutil
import stat
from datetime import datetime, UTC
from pathlib import Path
from typing import Any, Callable
from uuid import UUID, uuid4

from structlog import get_logger

from chaostrace.fs_proxy.models import (
    FSEvent,
    FSOperationType,
    FSPolicyAction,
    FSPolicyResult,
    FSRiskLevel,
)

logger = get_logger(__name__)


class FSInterceptor:
    """
    Intercepts file system operations and logs them.
    
    Provides wrapped versions of common file operations that:
    - Log all operations with metadata
    - Check against policies before execution
    - Support chaos injection hooks
    - Track operation patterns for analysis
    
    Usage:
        interceptor = FSInterceptor(run_id)
        
        # Instead of: open('/path/to/file', 'r')
        content = interceptor.read_file('/path/to/file')
        
        # Instead of: os.remove('/path/to/file')
        interceptor.delete_file('/path/to/file')
    """
    
    def __init__(
        self,
        run_id: UUID,
        sandbox_root: str = "/sandbox",
        policy_evaluator: Callable | None = None,
        event_callback: Callable | None = None,
    ):
        """
        Initialize the file system interceptor.
        
        Args:
            run_id: ID of the current test run.
            sandbox_root: Root directory for the sandbox.
            policy_evaluator: Function to evaluate operations against policy.
            event_callback: Callback for logging events.
        """
        self.run_id = run_id
        self.sandbox_root = Path(sandbox_root).resolve()
        self.policy_evaluator = policy_evaluator
        self.event_callback = event_callback
        
        self._operation_count = 0
        self._blocked_count = 0
        
        logger.info(
            "fs_interceptor_initialized",
            run_id=str(run_id),
            sandbox_root=str(self.sandbox_root),
        )
    
    # =========================================================================
    # Read Operations
    # =========================================================================
    
    def read_file(self, path: str | Path, binary: bool = False) -> str | bytes:
        """
        Read a file's contents.
        
        Args:
            path: Path to the file.
            binary: Whether to read as binary.
            
        Returns:
            File contents as string or bytes.
        """
        path = Path(path).resolve()
        start_time = datetime.now(UTC)
        
        # Check policy
        result = self._check_policy(FSOperationType.READ, path)
        if not result.allowed:
            self._log_event(
                FSOperationType.READ, path, result,
                success=False, error_message=result.violation_reasons[0] if result.violation_reasons else "Blocked"
            )
            raise PermissionError(f"Read blocked: {result.violation_reasons}")
        
        try:
            mode = "rb" if binary else "r"
            with open(path, mode) as f:
                content = f.read()
            
            latency = (datetime.now(UTC) - start_time).total_seconds() * 1000
            self._log_event(
                FSOperationType.READ, path, result,
                success=True, latency_ms=latency,
                metadata={"size": len(content), "binary": binary}
            )
            
            return content
            
        except Exception as e:
            self._log_event(
                FSOperationType.READ, path, result,
                success=False, error_message=str(e)
            )
            raise
    
    def list_dir(self, path: str | Path) -> list[str]:
        """
        List contents of a directory.
        
        Args:
            path: Path to the directory.
            
        Returns:
            List of file/directory names.
        """
        path = Path(path).resolve()
        start_time = datetime.now(UTC)
        
        result = self._check_policy(FSOperationType.LIST_DIR, path)
        if not result.allowed:
            self._log_event(
                FSOperationType.LIST_DIR, path, result,
                success=False, error_message="Blocked"
            )
            raise PermissionError(f"List blocked: {result.violation_reasons}")
        
        try:
            entries = os.listdir(path)
            
            latency = (datetime.now(UTC) - start_time).total_seconds() * 1000
            self._log_event(
                FSOperationType.LIST_DIR, path, result,
                success=True, latency_ms=latency,
                metadata={"entry_count": len(entries)}
            )
            
            return entries
            
        except Exception as e:
            self._log_event(
                FSOperationType.LIST_DIR, path, result,
                success=False, error_message=str(e)
            )
            raise
    
    def stat_file(self, path: str | Path) -> os.stat_result:
        """
        Get file statistics.
        
        Args:
            path: Path to the file.
            
        Returns:
            os.stat_result object.
        """
        path = Path(path).resolve()
        
        result = self._check_policy(FSOperationType.STAT, path)
        if not result.allowed:
            self._log_event(
                FSOperationType.STAT, path, result,
                success=False, error_message="Blocked"
            )
            raise PermissionError(f"Stat blocked: {result.violation_reasons}")
        
        try:
            stats = os.stat(path)
            self._log_event(
                FSOperationType.STAT, path, result,
                success=True,
                metadata={"size": stats.st_size, "mode": oct(stats.st_mode)}
            )
            return stats
            
        except Exception as e:
            self._log_event(
                FSOperationType.STAT, path, result,
                success=False, error_message=str(e)
            )
            raise
    
    # =========================================================================
    # Write Operations
    # =========================================================================
    
    def write_file(
        self,
        path: str | Path,
        content: str | bytes,
        append: bool = False,
    ) -> int:
        """
        Write content to a file.
        
        Args:
            path: Path to the file.
            content: Content to write.
            append: Whether to append instead of overwrite.
            
        Returns:
            Number of bytes written.
        """
        path = Path(path).resolve()
        operation = FSOperationType.APPEND if append else FSOperationType.WRITE
        start_time = datetime.now(UTC)
        
        result = self._check_policy(operation, path)
        if not result.allowed:
            self._log_event(
                operation, path, result,
                success=False, error_message="Blocked"
            )
            raise PermissionError(f"Write blocked: {result.violation_reasons}")
        
        try:
            is_binary = isinstance(content, bytes)
            mode = "ab" if append else "wb"
            if not is_binary:
                mode = "a" if append else "w"
            
            with open(path, mode) as f:
                bytes_written = f.write(content)
            
            latency = (datetime.now(UTC) - start_time).total_seconds() * 1000
            self._log_event(
                operation, path, result,
                success=True, latency_ms=latency,
                metadata={"bytes_written": bytes_written, "append": append}
            )
            
            return bytes_written
            
        except Exception as e:
            self._log_event(
                operation, path, result,
                success=False, error_message=str(e)
            )
            raise
    
    def create_file(self, path: str | Path, content: str = "") -> Path:
        """
        Create a new file.
        
        Args:
            path: Path for the new file.
            content: Optional initial content.
            
        Returns:
            Path to the created file.
        """
        path = Path(path).resolve()
        
        result = self._check_policy(FSOperationType.CREATE, path)
        if not result.allowed:
            self._log_event(
                FSOperationType.CREATE, path, result,
                success=False, error_message="Blocked"
            )
            raise PermissionError(f"Create blocked: {result.violation_reasons}")
        
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content)
            
            self._log_event(
                FSOperationType.CREATE, path, result,
                success=True,
                metadata={"content_length": len(content)}
            )
            
            return path
            
        except Exception as e:
            self._log_event(
                FSOperationType.CREATE, path, result,
                success=False, error_message=str(e)
            )
            raise
    
    # =========================================================================
    # Delete Operations
    # =========================================================================
    
    def delete_file(self, path: str | Path) -> None:
        """
        Delete a file.
        
        Args:
            path: Path to the file to delete.
        """
        path = Path(path).resolve()
        
        result = self._check_policy(FSOperationType.DELETE, path)
        if not result.allowed:
            self._log_event(
                FSOperationType.DELETE, path, result,
                success=False, error_message="Blocked"
            )
            raise PermissionError(f"Delete blocked: {result.violation_reasons}")
        
        try:
            file_size = path.stat().st_size if path.exists() else 0
            os.remove(path)
            
            self._log_event(
                FSOperationType.DELETE, path, result,
                success=True,
                metadata={"deleted_size": file_size}
            )
            
        except Exception as e:
            self._log_event(
                FSOperationType.DELETE, path, result,
                success=False, error_message=str(e)
            )
            raise
    
    def delete_directory(self, path: str | Path, recursive: bool = False) -> None:
        """
        Delete a directory.
        
        Args:
            path: Path to the directory.
            recursive: Whether to delete recursively (dangerous!).
        """
        path = Path(path).resolve()
        operation = FSOperationType.RMTREE if recursive else FSOperationType.RMDIR
        
        result = self._check_policy(operation, path)
        if not result.allowed:
            self._log_event(
                operation, path, result,
                success=False, error_message="Blocked"
            )
            raise PermissionError(f"Delete blocked: {result.violation_reasons}")
        
        try:
            if recursive:
                # Count files before deletion
                file_count = sum(1 for _ in path.rglob("*"))
                shutil.rmtree(path)
                metadata = {"recursive": True, "files_deleted": file_count}
            else:
                os.rmdir(path)
                metadata = {"recursive": False}
            
            self._log_event(
                operation, path, result,
                success=True, metadata=metadata
            )
            
        except Exception as e:
            self._log_event(
                operation, path, result,
                success=False, error_message=str(e)
            )
            raise
    
    # =========================================================================
    # Directory Operations
    # =========================================================================
    
    def make_directory(self, path: str | Path, parents: bool = False) -> Path:
        """
        Create a directory.
        
        Args:
            path: Path for the new directory.
            parents: Whether to create parent directories.
            
        Returns:
            Path to the created directory.
        """
        path = Path(path).resolve()
        operation = FSOperationType.MAKEDIRS if parents else FSOperationType.MKDIR
        
        result = self._check_policy(operation, path)
        if not result.allowed:
            self._log_event(
                operation, path, result,
                success=False, error_message="Blocked"
            )
            raise PermissionError(f"Mkdir blocked: {result.violation_reasons}")
        
        try:
            if parents:
                path.mkdir(parents=True, exist_ok=True)
            else:
                path.mkdir()
            
            self._log_event(
                operation, path, result,
                success=True, metadata={"parents": parents}
            )
            
            return path
            
        except Exception as e:
            self._log_event(
                operation, path, result,
                success=False, error_message=str(e)
            )
            raise
    
    # =========================================================================
    # Move/Copy Operations
    # =========================================================================
    
    def rename(self, src: str | Path, dst: str | Path) -> Path:
        """
        Rename/move a file or directory.
        
        Args:
            src: Source path.
            dst: Destination path.
            
        Returns:
            New path.
        """
        src = Path(src).resolve()
        dst = Path(dst).resolve()
        
        result = self._check_policy(FSOperationType.RENAME, src, dst)
        if not result.allowed:
            self._log_event(
                FSOperationType.RENAME, src, result,
                target_path=dst, success=False, error_message="Blocked"
            )
            raise PermissionError(f"Rename blocked: {result.violation_reasons}")
        
        try:
            shutil.move(str(src), str(dst))
            
            self._log_event(
                FSOperationType.RENAME, src, result,
                target_path=dst, success=True
            )
            
            return dst
            
        except Exception as e:
            self._log_event(
                FSOperationType.RENAME, src, result,
                target_path=dst, success=False, error_message=str(e)
            )
            raise
    
    def copy(self, src: str | Path, dst: str | Path) -> Path:
        """
        Copy a file.
        
        Args:
            src: Source path.
            dst: Destination path.
            
        Returns:
            Destination path.
        """
        src = Path(src).resolve()
        dst = Path(dst).resolve()
        
        result = self._check_policy(FSOperationType.COPY, src, dst)
        if not result.allowed:
            self._log_event(
                FSOperationType.COPY, src, result,
                target_path=dst, success=False, error_message="Blocked"
            )
            raise PermissionError(f"Copy blocked: {result.violation_reasons}")
        
        try:
            shutil.copy2(str(src), str(dst))
            
            self._log_event(
                FSOperationType.COPY, src, result,
                target_path=dst, success=True,
                metadata={"size": src.stat().st_size}
            )
            
            return dst
            
        except Exception as e:
            self._log_event(
                FSOperationType.COPY, src, result,
                target_path=dst, success=False, error_message=str(e)
            )
            raise
    
    # =========================================================================
    # Permission Operations
    # =========================================================================
    
    def chmod(self, path: str | Path, mode: int) -> None:
        """
        Change file permissions.
        
        Args:
            path: Path to the file.
            mode: New permission mode (e.g., 0o755).
        """
        path = Path(path).resolve()
        
        result = self._check_policy(FSOperationType.CHMOD, path)
        if not result.allowed:
            self._log_event(
                FSOperationType.CHMOD, path, result,
                success=False, error_message="Blocked"
            )
            raise PermissionError(f"Chmod blocked: {result.violation_reasons}")
        
        try:
            old_mode = path.stat().st_mode
            os.chmod(path, mode)
            
            self._log_event(
                FSOperationType.CHMOD, path, result,
                success=True,
                metadata={"old_mode": oct(old_mode), "new_mode": oct(mode)}
            )
            
        except Exception as e:
            self._log_event(
                FSOperationType.CHMOD, path, result,
                success=False, error_message=str(e)
            )
            raise
    
    # =========================================================================
    # Helper Methods
    # =========================================================================
    
    def _check_policy(
        self,
        operation: FSOperationType,
        path: Path,
        target_path: Path | None = None,
    ) -> FSPolicyResult:
        """Check if an operation is allowed by policy."""
        self._operation_count += 1
        
        if self.policy_evaluator:
            return self.policy_evaluator(
                operation=operation,
                path=str(path),
                target_path=str(target_path) if target_path else None,
            )
        
        # Default: allow everything
        return FSPolicyResult(
            allowed=True,
            risk_level=FSRiskLevel.LOW,
        )
    
    def _log_event(
        self,
        operation: FSOperationType,
        path: Path,
        policy_result: FSPolicyResult,
        target_path: Path | None = None,
        success: bool = True,
        error_message: str | None = None,
        latency_ms: float = 0.0,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Log a file system event."""
        if not policy_result.allowed:
            self._blocked_count += 1
        
        event = FSEvent(
            event_id=uuid4(),
            run_id=self.run_id,
            operation=operation,
            path=str(path),
            target_path=str(target_path) if target_path else None,
            is_directory=path.is_dir() if path.exists() else False,
            file_size=path.stat().st_size if path.exists() and path.is_file() else None,
            risk_level=policy_result.risk_level,
            risk_factors=policy_result.matched_rules,
            policy_action=(
                FSPolicyAction.BLOCK if not policy_result.allowed
                else FSPolicyAction.ALLOW_FLAGGED if policy_result.flagged
                else FSPolicyAction.ALLOW
            ),
            policy_rule_matched=policy_result.matched_rules[0] if policy_result.matched_rules else None,
            violation_reason=policy_result.violation_reasons[0] if policy_result.violation_reasons else None,
            success=success,
            error_message=error_message,
            latency_ms=latency_ms,
            metadata=metadata or {},
        )
        
        if self.event_callback:
            self.event_callback(event)
        
        log_method = logger.debug if success else logger.warning
        log_method(
            "fs_operation",
            operation=operation.value,
            path=str(path),
            allowed=policy_result.allowed,
            success=success,
        )
    
    @property
    def stats(self) -> dict:
        """Get interceptor statistics."""
        return {
            "operation_count": self._operation_count,
            "blocked_count": self._blocked_count,
        }
