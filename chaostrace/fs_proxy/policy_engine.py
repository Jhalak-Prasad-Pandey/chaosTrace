"""
File System Policy Engine

Evaluates file system operations against policies.
Supports path patterns, operation restrictions, and honeypot detection.
"""

import fnmatch
from pathlib import Path
from typing import Any

import yaml
from structlog import get_logger

from chaostrace.fs_proxy.models import (
    FSHoneypot,
    FSOperationType,
    FSPathRule,
    FSPolicy,
    FSPolicyAction,
    FSPolicyResult,
    FSRiskLevel,
)

logger = get_logger(__name__)


class FSPolicyEngine:
    """
    Evaluates file system operations against loaded policies.
    
    Supports:
    - Path pattern matching (glob patterns)
    - Operation-specific restrictions
    - Honeypot file/directory detection
    - Sandbox boundary enforcement
    
    Usage:
        engine = FSPolicyEngine.from_file("policies/strict_fs.yaml")
        result = engine.evaluate(
            operation=FSOperationType.DELETE,
            path="/sandbox/data/users.db"
        )
        if not result.allowed:
            print(f"Blocked: {result.violation_reasons}")
    """
    
    # Base risk by operation type
    BASE_RISK: dict[FSOperationType, FSRiskLevel] = {
        # Read operations - low risk
        FSOperationType.READ: FSRiskLevel.LOW,
        FSOperationType.OPEN: FSRiskLevel.LOW,
        FSOperationType.STAT: FSRiskLevel.LOW,
        FSOperationType.LIST_DIR: FSRiskLevel.LOW,
        FSOperationType.READ_LINK: FSRiskLevel.LOW,
        
        # Write operations - medium risk
        FSOperationType.WRITE: FSRiskLevel.MEDIUM,
        FSOperationType.CREATE: FSRiskLevel.MEDIUM,
        FSOperationType.APPEND: FSRiskLevel.MEDIUM,
        FSOperationType.MKDIR: FSRiskLevel.MEDIUM,
        FSOperationType.MAKEDIRS: FSRiskLevel.MEDIUM,
        
        # Modification - high risk
        FSOperationType.TRUNCATE: FSRiskLevel.HIGH,
        FSOperationType.CHMOD: FSRiskLevel.HIGH,
        FSOperationType.CHOWN: FSRiskLevel.HIGH,
        FSOperationType.RENAME: FSRiskLevel.MEDIUM,
        FSOperationType.MOVE: FSRiskLevel.MEDIUM,
        FSOperationType.COPY: FSRiskLevel.LOW,
        
        # Delete operations - high risk
        FSOperationType.DELETE: FSRiskLevel.HIGH,
        FSOperationType.UNLINK: FSRiskLevel.HIGH,
        FSOperationType.RMDIR: FSRiskLevel.HIGH,
        FSOperationType.RMTREE: FSRiskLevel.CRITICAL,
        
        # Special - high risk
        FSOperationType.SYMLINK: FSRiskLevel.HIGH,
        FSOperationType.HARDLINK: FSRiskLevel.HIGH,
        FSOperationType.EXEC: FSRiskLevel.CRITICAL,
    }
    
    def __init__(self, policy: FSPolicy):
        """
        Initialize the policy engine.
        
        Args:
            policy: The file system policy to enforce.
        """
        self.policy = policy
        self._honeypot_patterns: list[tuple[str, FSHoneypot]] = []
        self._compile_honeypots()
        
        logger.info(
            "fs_policy_engine_initialized",
            policy_name=policy.name,
            protected_path_count=len(policy.protected_paths),
            honeypot_count=len(policy.honeypots),
        )
    
    def _compile_honeypots(self) -> None:
        """Prepare honeypot patterns for matching."""
        for hp in self.policy.honeypots:
            if hp.recursive and hp.is_directory:
                # Match directory and all contents
                self._honeypot_patterns.append((f"{hp.path}/**", hp))
            self._honeypot_patterns.append((hp.path, hp))
    
    @classmethod
    def from_file(cls, path: Path | str) -> "FSPolicyEngine":
        """Load a policy from a YAML file."""
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"Policy file not found: {path}")
        
        with open(path) as f:
            data = yaml.safe_load(f)
        
        policy = FSPolicy(**data)
        return cls(policy)
    
    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "FSPolicyEngine":
        """Create a policy engine from a dictionary."""
        policy = FSPolicy(**data)
        return cls(policy)
    
    def evaluate(
        self,
        operation: FSOperationType,
        path: str,
        target_path: str | None = None,
    ) -> FSPolicyResult:
        """
        Evaluate a file system operation against the policy.
        
        Args:
            operation: Type of operation.
            path: Primary path involved.
            target_path: Target path for move/copy/rename.
            
        Returns:
            FSPolicyResult: Evaluation result.
        """
        result = FSPolicyResult(
            allowed=True,
            risk_level=self.BASE_RISK.get(operation, FSRiskLevel.MEDIUM),
        )
        
        path_obj = Path(path)
        
        # Check sandbox boundary
        self._check_sandbox_boundary(path_obj, result)
        if target_path:
            self._check_sandbox_boundary(Path(target_path), result)
        
        # Check globally forbidden operations
        self._check_forbidden_operations(operation, result)
        
        # Check honeypots
        self._check_honeypots(path, operation, result)
        if target_path:
            self._check_honeypots(target_path, operation, result)
        
        # Check protected paths
        self._check_protected_paths(path, operation, result)
        if target_path:
            self._check_protected_paths(target_path, operation, result)
        
        # Check operation-specific restrictions
        self._check_operation_restrictions(operation, result)
        
        logger.debug(
            "fs_policy_evaluation",
            operation=operation.value,
            path=path,
            allowed=result.allowed,
            risk_level=result.risk_level.value,
        )
        
        return result
    
    def _check_sandbox_boundary(
        self,
        path: Path,
        result: FSPolicyResult,
    ) -> None:
        """Ensure operations stay within sandbox."""
        try:
            resolved = path.resolve()
            sandbox = Path(self.policy.sandbox_root).resolve()
            
            # Check if path is within sandbox
            try:
                resolved.relative_to(sandbox)
            except ValueError:
                result.allowed = False
                result.risk_level = FSRiskLevel.CRITICAL
                result.violation_reasons.append(
                    f"Path escapes sandbox: {path} (sandbox: {sandbox})"
                )
                result.matched_rules.append("sandbox_boundary")
        except Exception:
            # Path doesn't exist yet, check parent
            pass
    
    def _check_forbidden_operations(
        self,
        operation: FSOperationType,
        result: FSPolicyResult,
    ) -> None:
        """Check globally forbidden operations."""
        for forbidden in self.policy.forbidden_operations:
            op_name = forbidden.get("operation", "")
            if op_name.upper() == operation.value.upper():
                result.allowed = False
                result.risk_level = FSRiskLevel.CRITICAL
                result.violation_reasons.append(
                    forbidden.get("message", f"{operation.value} is forbidden")
                )
                result.matched_rules.append(f"forbidden_operation:{operation.value}")
        
        # Check specific restrictions
        if operation == FSOperationType.RMTREE and not self.policy.allow_recursive_delete:
            result.allowed = False
            result.risk_level = FSRiskLevel.CRITICAL
            result.violation_reasons.append("Recursive delete is not allowed")
            result.matched_rules.append("no_recursive_delete")
        
        if operation in (FSOperationType.SYMLINK, FSOperationType.HARDLINK):
            if not self.policy.allow_symlinks:
                result.allowed = False
                result.risk_level = FSRiskLevel.HIGH
                result.violation_reasons.append("Symlinks are not allowed")
                result.matched_rules.append("no_symlinks")
    
    def _check_honeypots(
        self,
        path: str,
        operation: FSOperationType,
        result: FSPolicyResult,
    ) -> None:
        """Check if path matches any honeypots."""
        for pattern, honeypot in self._honeypot_patterns:
            if fnmatch.fnmatch(path, pattern):
                # Skip if this is just a directory listing and alert_on_list is False
                if operation == FSOperationType.LIST_DIR and not honeypot.alert_on_list:
                    continue
                
                result.allowed = False
                result.is_honeypot = True
                result.risk_level = FSRiskLevel.CRITICAL
                result.violation_reasons.append(
                    f"Access to honeypot: {honeypot.path}"
                )
                result.matched_rules.append(f"honeypot:{honeypot.path}")
    
    def _check_protected_paths(
        self,
        path: str,
        operation: FSOperationType,
        result: FSPolicyResult,
    ) -> None:
        """Check if path matches protected path rules."""
        for rule in self.policy.protected_paths:
            if not fnmatch.fnmatch(path, rule.pattern):
                continue
            
            # Check if operation is in the restricted list
            if rule.operations and operation not in rule.operations:
                continue
            
            # Match found
            result.matched_rules.append(f"protected_path:{rule.pattern}")
            
            if rule.action == FSPolicyAction.BLOCK:
                result.allowed = False
                result.risk_level = rule.risk_level
                result.violation_reasons.append(
                    rule.message or f"Access to protected path: {rule.pattern}"
                )
            elif rule.action == FSPolicyAction.ALLOW_FLAGGED:
                result.flagged = True
    
    def _check_operation_restrictions(
        self,
        operation: FSOperationType,
        result: FSPolicyResult,
    ) -> None:
        """Check operation-specific restrictions."""
        # Additional checks can be added here based on operation type
        pass
    
    def is_path_allowed(self, path: str) -> bool:
        """Quick check if a path is accessible."""
        for pattern in self.policy.allowed_paths:
            if fnmatch.fnmatch(path, pattern):
                return True
        return False
