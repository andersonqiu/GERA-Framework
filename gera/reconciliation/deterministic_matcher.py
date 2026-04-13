"""
Deterministic Cross-System Matcher

Implements key-based record matching between source and target
systems with composite key support, key normalization, and
value conflict detection.

This is Layer 1 of the GERA Framework, providing the foundation
for cross-system reconciliation in regulated enterprises.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple


class MatchStatus(Enum):
    """Record match status."""
    MATCHED = "matched"
    UNMATCHED_SOURCE = "unmatched_source"
    UNMATCHED_TARGET = "unmatched_target"
    DUPLICATE = "duplicate"
    CONFLICT = "conflict"


@dataclass
class MatchResult:
    """Result for a single record match attempt."""
    source_key: Optional[tuple]
    target_key: Optional[tuple]
    status: MatchStatus
    source_record: Optional[Dict[str, Any]] = None
    target_record: Optional[Dict[str, Any]] = None
    conflicts: List[Tuple[str, Any, Any]] = field(default_factory=list)


class MatchReport:
    """Aggregated matching report."""

    def __init__(self):
        self.results: List[MatchResult] = []

    def add(self, result: MatchResult):
        self.results.append(result)

    @property
    def matched_count(self) -> int:
        return sum(
            1 for r in self.results
            if r.status in (MatchStatus.MATCHED, MatchStatus.CONFLICT)
        )

    @property
    def unmatched_source_count(self) -> int:
        return sum(
            1 for r in self.results if r.status == MatchStatus.UNMATCHED_SOURCE
        )

    @property
    def unmatched_target_count(self) -> int:
        return sum(
            1 for r in self.results if r.status == MatchStatus.UNMATCHED_TARGET
        )

    @property
    def conflict_count(self) -> int:
        return sum(
            1 for r in self.results if r.status == MatchStatus.CONFLICT
        )

    @property
    def match_rate(self) -> float:
        total_source = sum(
            1 for r in self.results
            if r.status != MatchStatus.UNMATCHED_TARGET
        )
        if total_source == 0:
            return 0.0
        return self.matched_count / total_source

    @property
    def is_fully_reconciled(self) -> bool:
        return all(r.status == MatchStatus.MATCHED for r in self.results)


class DeterministicMatcher:
    """
    Cross-system record matcher using deterministic key matching.

    Supports composite keys, key normalization, and value field
    conflict detection.

    Args:
        key_fields: List of field names to use as match keys
        value_fields: Optional list of fields to check for conflicts
        normalize_keys: Whether to normalize keys (strip/lower)
    """

    def __init__(
        self,
        key_fields: List[str],
        value_fields: Optional[List[str]] = None,
        normalize_keys: bool = True,
    ):
        self.key_fields = key_fields
        self.value_fields = value_fields or []
        self.normalize_keys = normalize_keys

    def _extract_key(self, record: Dict[str, Any]) -> tuple:
        """Extract composite key from record."""
        parts = []
        for f in self.key_fields:
            val = record.get(f, "")
            if self.normalize_keys and isinstance(val, str):
                val = val.strip().lower()
            parts.append(val)
        return tuple(parts)

    def match(
        self,
        source_records: List[Dict[str, Any]],
        target_records: List[Dict[str, Any]],
    ) -> MatchReport:
        """
        Match source records against target records.

        Returns a MatchReport with results for every record
        in both source and target.
        """
        report = MatchReport()

        # Build target index
        target_index: Dict[tuple, List[Dict[str, Any]]] = {}
        for rec in target_records:
            key = self._extract_key(rec)
            target_index.setdefault(key, []).append(rec)

        matched_target_keys = set()

        # Match each source record
        for src in source_records:
            src_key = self._extract_key(src)
            targets = target_index.get(src_key, [])

            if not targets:
                report.add(MatchResult(
                    source_key=src_key,
                    target_key=None,
                    status=MatchStatus.UNMATCHED_SOURCE,
                    source_record=src,
                ))
                continue

            if len(targets) > 1:
                report.add(MatchResult(
                    source_key=src_key,
                    target_key=src_key,
                    status=MatchStatus.DUPLICATE,
                    source_record=src,
                    target_record=targets[0],
                ))
                matched_target_keys.add(src_key)
                continue

            tgt = targets[0]
            matched_target_keys.add(src_key)

            # Check for value conflicts
            conflicts = []
            for vf in self.value_fields:
                sv = src.get(vf)
                tv = tgt.get(vf)
                if sv != tv:
                    conflicts.append((vf, sv, tv))

            if conflicts:
                report.add(MatchResult(
                    source_key=src_key,
                    target_key=src_key,
                    status=MatchStatus.CONFLICT,
                    source_record=src,
                    target_record=tgt,
                    conflicts=conflicts,
                ))
            else:
                report.add(MatchResult(
                    source_key=src_key,
                    target_key=src_key,
                    status=MatchStatus.MATCHED,
                    source_record=src,
                    target_record=tgt,
                ))

        # Report unmatched targets
        for key, targets in target_index.items():
            if key not in matched_target_keys:
                for tgt in targets:
                    report.add(MatchResult(
                        source_key=None,
                        target_key=key,
                        status=MatchStatus.UNMATCHED_TARGET,
                        target_record=tgt,
                    ))

        return report
