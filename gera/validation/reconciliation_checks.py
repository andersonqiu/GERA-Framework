"""
Deterministic Reconciliation Checks

Provides count matching, amount balancing, key completeness,
and hash integrity verification for cross-system reconciliation.

These checks form the deterministic layer of GERA's validation
pipeline, complementing the statistical Z-Score gate.
"""

import hashlib
import json
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Set


class CheckStatus(Enum):
    """Reconciliation check outcome."""
    PASS = "PASS"
    FAIL = "FAIL"
    WARN = "WARN"


@dataclass
class ReconciliationResult:
    """Result of a single reconciliation check."""
    check_name: str
    status: CheckStatus
    message: str
    source_value: Any = None
    target_value: Any = None
    difference: Any = None
    tolerance_used: Optional[float] = None


class ReconciliationReport:
    """Aggregated report of multiple reconciliation checks."""

    def __init__(self):
        self.results: List[ReconciliationResult] = []

    def add(self, result: ReconciliationResult):
        self.results.append(result)

    @property
    def overall_status(self) -> CheckStatus:
        if any(r.status == CheckStatus.FAIL for r in self.results):
            return CheckStatus.FAIL
        if any(r.status == CheckStatus.WARN for r in self.results):
            return CheckStatus.WARN
        return CheckStatus.PASS

    def to_audit_record(self) -> Dict[str, Any]:
        return {
            "overall_status": self.overall_status.value,
            "check_count": len(self.results),
            "checks": [
                {
                    "name": r.check_name,
                    "status": r.status.value,
                    "message": r.message,
                }
                for r in self.results
            ],
        }


class ReconciliationCheck:
    """
    Deterministic reconciliation checks for cross-system validation.

    Provides count matching, amount balancing, key completeness,
    and hash integrity verification.

    Args:
        tolerance: Amount tolerance as a fraction (default: 0.01 = 1%)
    """

    def __init__(self, tolerance: float = 0.01):
        self.tolerance = tolerance

    def check_count(
        self, source_count: int, target_count: int
    ) -> ReconciliationResult:
        """Verify record counts match between source and target."""
        diff = target_count - source_count
        if diff == 0:
            return ReconciliationResult(
                check_name="record_count",
                status=CheckStatus.PASS,
                message=f"Counts match: {source_count} records",
                source_value=source_count,
                target_value=target_count,
                difference=0,
            )
        return ReconciliationResult(
            check_name="record_count",
            status=CheckStatus.FAIL,
            message=f"Count mismatch: {diff:+d} records",
            source_value=source_count,
            target_value=target_count,
            difference=diff,
        )

    def check_amount(
        self,
        source_amount: float,
        target_amount: float,
        label: str = "amount",
    ) -> ReconciliationResult:
        """Verify amounts balance within tolerance."""
        diff = target_amount - source_amount
        if source_amount == 0:
            pct_diff = 0.0 if target_amount == 0 else float('inf')
        else:
            pct_diff = abs(diff) / abs(source_amount)

        if pct_diff <= self.tolerance:
            return ReconciliationResult(
                check_name=f"amount_balance_{label}",
                status=CheckStatus.PASS,
                message=f"{label} balanced within {self.tolerance:.2%} tolerance",
                source_value=source_amount,
                target_value=target_amount,
                difference=diff,
                tolerance_used=self.tolerance,
            )
        return ReconciliationResult(
            check_name=f"amount_balance_{label}",
            status=CheckStatus.FAIL,
            message=f"{label} mismatch: {diff:,.2f} ({pct_diff:.4%} of source)",
            source_value=source_amount,
            target_value=target_amount,
            difference=diff,
            tolerance_used=self.tolerance,
        )

    def check_completeness(
        self,
        source_keys: Set[str],
        target_keys: Set[str],
    ) -> ReconciliationResult:
        """Verify all source keys exist in target."""
        missing = source_keys - target_keys
        if not missing:
            return ReconciliationResult(
                check_name="key_completeness",
                status=CheckStatus.PASS,
                message=f"All {len(source_keys)} source keys present in target",
                source_value=len(source_keys),
                target_value=len(target_keys),
            )
        return ReconciliationResult(
            check_name="key_completeness",
            status=CheckStatus.FAIL,
            message=f"{len(missing)} source keys missing in target",
            source_value=len(source_keys),
            target_value=len(target_keys),
            difference=sorted(missing),
        )

    def check_hash_integrity(
        self, data: Any, expected_hash: str
    ) -> ReconciliationResult:
        """Verify data integrity via SHA-256 hash comparison."""
        actual_hash = self.compute_hash(data)
        if actual_hash == expected_hash:
            return ReconciliationResult(
                check_name="hash_integrity",
                status=CheckStatus.PASS,
                message="Hash integrity verified",
                source_value=expected_hash[:16] + "...",
                target_value=actual_hash[:16] + "...",
            )
        return ReconciliationResult(
            check_name="hash_integrity",
            status=CheckStatus.FAIL,
            message="Hash mismatch — possible data tampering",
            source_value=expected_hash[:16] + "...",
            target_value=actual_hash[:16] + "...",
        )

    @staticmethod
    def compute_hash(data: Any) -> str:
        """Compute SHA-256 hash of JSON-serialized data."""
        serialized = json.dumps(data, sort_keys=True, default=str)
        return hashlib.sha256(serialized.encode()).hexdigest()

    def run_all(
        self,
        source_count: int,
        target_count: int,
        source_amount: float,
        target_amount: float,
        source_keys: Set[str],
        target_keys: Set[str],
    ) -> ReconciliationReport:
        """Run all standard reconciliation checks."""
        report = ReconciliationReport()
        report.add(self.check_count(source_count, target_count))
        report.add(self.check_amount(source_amount, target_amount))
        report.add(self.check_completeness(source_keys, target_keys))
        return report
