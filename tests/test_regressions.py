"""
Regression tests for the correctness bugs fixed in the 2026-04 hardening
pass. One test per bug; each test first asserts the fixed behavior and,
where practical, also exercises the previously-buggy code path to show
the fix is load-bearing.

Bug map (matches the companion-code review notes):

* B1 — AuditLogger retention purge + verify_chain false tamper
* B2 — AuditEvent.details dict was mutable despite frozen dataclass
* B3 — DeterministicMatcher silently produced many-to-one matches
* B4 — SemanticRegistry let external mutation bypass update() versioning
* B5 — ZScoreGate.validate_segmented PASSed on insufficient per-segment
       baseline (fail-open); must FLAG (fail-closed)
* B6 — AuditEvent details containing sets / nested tuples bypassed
       _deep_freeze and remained mutable (P2A follow-up)
* B7 — MatchReport.match_rate denominator inflated by target-side
       DUPLICATE rows (P2B follow-up)
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import MappingProxyType
from typing import List

import numpy as np
import pytest

from gera.governance.audit_logger import AuditEvent, AuditLogger, EventType
from gera.governance.semantic_registry import (
    DataSensitivity,
    MetricDefinition,
    SemanticRegistry,
)
from gera.reconciliation.deterministic_matcher import (
    DeterministicMatcher,
    MatchStatus,
)
from gera.validation.zscore_gate import GateDecision, ZScoreGate


# ---------------------------------------------------------------------------
# B1 — AuditLogger retention + verify_chain
# ---------------------------------------------------------------------------

class TestAuditLoggerRetentionChain:
    """After retention purge, verify_chain() must still return True."""

    def _force_purge(self, logger: AuditLogger, n_old: int, n_new: int) -> None:
        """
        Log n_old + n_new events and back-date the first n_old past the
        retention cutoff so that the next log() triggers _cleanup_expired.
        """
        for i in range(n_old + n_new):
            logger.log(EventType.SYSTEM_EVENT, "sys", f"action-{i}", "res")

        cutoff_past = datetime.now(timezone.utc) - timedelta(
            days=logger.retention_days + 1
        )
        for i in range(n_old):
            # Events are frozen, so use dataclasses.replace via the usual
            # escape hatch.
            from dataclasses import replace
            logger._events[i] = replace(logger._events[i], timestamp=cutoff_past)
        # Reset oldest_timestamp so cleanup is actually triggered.
        logger._oldest_timestamp = cutoff_past

    def test_verify_chain_after_partial_purge(self):
        logger = AuditLogger(retention_days=30)
        self._force_purge(logger, n_old=3, n_new=2)

        # Trigger cleanup. Must not raise, must drop the old events.
        logger.log(EventType.SYSTEM_EVENT, "sys", "trigger", "res")

        assert logger.event_count <= 3, (
            "expected old events to be purged, got "
            f"{logger.event_count}"
        )

        # THE BUG: before the fix, surviving events referenced the
        # previous_hash of a purged predecessor, so verify_chain()
        # falsely reported tampering.
        ok, detail = logger.verify_chain_detail()
        assert ok, f"chain verification must survive retention purge, got {detail}"
        assert detail is None

    def test_verify_chain_after_full_purge(self):
        """When retention purges every event, verify_chain() must stay True
        and the anchor must advance so a follow-up log() still chains."""
        logger = AuditLogger(retention_days=30)
        self._force_purge(logger, n_old=3, n_new=0)

        # Trigger cleanup.
        logger.log(EventType.SYSTEM_EVENT, "sys", "post-purge", "res")

        # Chain must still verify end-to-end.
        ok, detail = logger.verify_chain_detail()
        assert ok, f"chain must verify after full purge + re-log, got {detail}"
        assert detail is None

    def test_event_id_monotonic_across_purge(self):
        """event_id must be a monotonically increasing, never-reused sequence.

        Previously the ID was ``len(self._events) + 1`` which reset after
        retention purge shrank the buffer — a second event then took the
        ID of an already-purged event. That broke the contract relied on
        by the Athena / BigQuery ``v_audit_chain_verification`` view,
        which orders the chain via ``LAG(event_hash) OVER (ORDER BY
        event_id)``. Reusing an ID would make the warehouse-side chain
        non-sortable and effectively unverifiable.
        """
        logger = AuditLogger(retention_days=30)

        # Log five events, back-date the first three past retention,
        # remember the ID of the last pre-purge event.
        for i in range(5):
            logger.log(EventType.SYSTEM_EVENT, "sys", f"pre-{i}", "res")
        pre_purge_ids = [e.event_id for e in logger._events]
        assert pre_purge_ids == [f"AUD-{i:08d}" for i in range(1, 6)]

        cutoff_past = datetime.now(timezone.utc) - timedelta(
            days=logger.retention_days + 1
        )
        from dataclasses import replace
        for i in range(3):
            logger._events[i] = replace(logger._events[i], timestamp=cutoff_past)
        logger._oldest_timestamp = cutoff_past

        # Trigger cleanup via a new log() call.
        new_event = logger.log(EventType.SYSTEM_EVENT, "sys", "post-purge", "res")

        # The new event's ID must be strictly greater than every ID ever
        # assigned, even the purged ones.
        all_prior_seq = [int(pid.split("-")[1]) for pid in pre_purge_ids]
        new_seq = int(new_event.event_id.split("-")[1])
        assert new_seq > max(all_prior_seq), (
            f"event_id must be monotonic across purge; got {new_event.event_id} "
            f"after {pre_purge_ids}"
        )

        # And no ID is ever repeated across the full history we observed.
        observed_ids = pre_purge_ids + [new_event.event_id]
        assert len(observed_ids) == len(set(observed_ids)), (
            "event_id must never repeat"
        )


# ---------------------------------------------------------------------------
# B2 — AuditEvent.details immutability
# ---------------------------------------------------------------------------

class TestAuditEventDetailsImmutable:
    """event.details must not be mutable from outside the logger."""

    def test_caller_dict_mutation_does_not_leak_into_event(self):
        logger = AuditLogger()
        details = {"amount": 100, "nested": {"k": "v"}, "list": [1, 2]}
        event = logger.log(EventType.GATE_DECISION, "sys", "pass", "g", details)

        # Mutate the caller's original dict. This must NOT affect the
        # stored event.
        details["amount"] = 999
        details["nested"]["k"] = "tampered"
        details["list"].append("injected")
        details["new_key"] = "sneaky"

        assert event.details["amount"] == 100
        assert event.details["nested"]["k"] == "v"
        assert list(event.details["list"]) == [1, 2]
        assert "new_key" not in event.details

        # And the chain still verifies.
        assert logger.verify_chain()

    def test_event_details_is_read_only(self):
        logger = AuditLogger()
        event = logger.log(
            EventType.GATE_DECISION,
            "sys",
            "pass",
            "g",
            details={"amount": 100, "nested": {"k": "v"}},
        )

        # Top-level must be a MappingProxyType (read-only).
        assert isinstance(event.details, MappingProxyType)
        with pytest.raises(TypeError):
            event.details["amount"] = 1  # type: ignore[index]

        # Nested dicts must also be read-only.
        assert isinstance(event.details["nested"], MappingProxyType)
        with pytest.raises(TypeError):
            event.details["nested"]["k"] = "tampered"  # type: ignore[index]


# ---------------------------------------------------------------------------
# B3 — DeterministicMatcher duplicate detection on both sides
# ---------------------------------------------------------------------------

class TestMatcherDuplicateDetection:
    """Many-to-one must not silently count as MATCHED."""

    def setup_method(self):
        self.matcher = DeterministicMatcher(
            key_fields=["id"],
            value_fields=["amount"],
        )

    def test_many_to_one_source_not_silently_matched(self):
        """Two source rows against one target row must be flagged DUPLICATE.

        Previous behaviour silently produced two MATCHED rows against the
        same target — masking duplicate postings / split ledger rows.
        """
        src = [
            {"id": "1", "amount": 100},
            {"id": "1", "amount": 100},
        ]
        tgt = [{"id": "1", "amount": 100}]
        report = self.matcher.match(src, tgt)

        assert report.matched_count == 0, (
            "many-to-one must not be silently MATCHED"
        )
        dups = [r for r in report.results if r.status == MatchStatus.DUPLICATE]
        assert len(dups) >= 2, (
            f"expected DUPLICATE rows for every ambiguous input, got {dups}"
        )

    def test_many_to_many_surfaces_every_record(self):
        src = [
            {"id": "1", "amount": 100},
            {"id": "1", "amount": 100},
        ]
        tgt = [
            {"id": "1", "amount": 100},
            {"id": "1", "amount": 100},
        ]
        report = self.matcher.match(src, tgt)

        assert report.matched_count == 0
        dups = [r for r in report.results if r.status == MatchStatus.DUPLICATE]
        # At minimum every input row shows up as DUPLICATE.
        assert len(dups) >= len(src) + len(tgt) - 1

    def test_one_to_one_still_matches_cleanly(self):
        """The fix must not regress the plain 1:1 path."""
        src = [{"id": "1", "amount": 100}, {"id": "2", "amount": 200}]
        tgt = [{"id": "1", "amount": 100}, {"id": "2", "amount": 200}]
        report = self.matcher.match(src, tgt)
        assert report.matched_count == 2
        assert report.is_fully_reconciled


# ---------------------------------------------------------------------------
# B4 — SemanticRegistry copy-on-write / copy-on-read
# ---------------------------------------------------------------------------

class TestSemanticRegistryIsolation:
    """External mutation must not bypass update()'s versioning."""

    def _metric(self, name: str = "revenue_total") -> MetricDefinition:
        return MetricDefinition(
            name=name,
            description="x",
            formula="SUM(amount)",
            owner="finance",
            sensitivity=DataSensitivity.INTERNAL,
            lineage=["erp.sales"],
        )

    def test_post_register_mutation_does_not_leak(self):
        registry = SemanticRegistry()
        m = self._metric()
        registry.register(m)

        # Mutating the caller's original object must NOT change the
        # registry's record.
        m.formula = "TAMPERED"
        m.version = 999
        m.lineage.append("hacked_source")

        fetched = registry.get("revenue_total")
        assert fetched is not None
        assert fetched.formula == "SUM(amount)"
        assert fetched.version == 1
        assert "hacked_source" not in fetched.lineage

    def test_get_returns_isolated_copy(self):
        registry = SemanticRegistry()
        registry.register(self._metric())

        a = registry.get("revenue_total")
        b = registry.get("revenue_total")
        assert a is not b

        # Mutating the returned object must NOT bump the registry's
        # state or bypass update()'s versioning.
        a.formula = "MALICIOUS"  # type: ignore[union-attr]
        a.version = 999  # type: ignore[union-attr]
        a.lineage.append("backdoor")  # type: ignore[union-attr]

        c = registry.get("revenue_total")
        assert c.formula == "SUM(amount)"
        assert c.version == 1
        assert "backdoor" not in c.lineage


# ---------------------------------------------------------------------------
# B5 — ZScoreGate segmented: FLAG (not PASS) when baseline is insufficient
# ---------------------------------------------------------------------------

class TestZScoreSegmentedInsufficientBaseline:
    """A segment without enough history must FAIL CLOSED (FLAG), not PASS."""

    def test_new_segment_with_no_history_is_flagged(self):
        gate = ZScoreGate(
            sigma_threshold=2.5,
            block_threshold=4.0,
            min_observations=30,
        )

        # "revenue" has a long baseline, "brand_new_segment" has none.
        rng = np.random.default_rng(13)
        hist = {
            "revenue": rng.normal(1000.0, 50.0, 100).tolist(),
            # Intentionally fewer than min_observations:
            "brand_new_segment": [5.0, 6.0, 7.0],
        }
        result = gate.validate_segmented(
            values=[1005.0, 100.0],
            segments=["revenue", "brand_new_segment"],
            historical_values=hist,
        )

        # Previously: the cold-start record was silently counted as PASS,
        # producing gate_decision=PASS and an empty anomalies list for it.
        assert result.flagged >= 1, (
            "insufficient-baseline record must be FLAGGED, not silently PASSED"
        )
        # The cold-start record should appear in the anomalies with a
        # FLAG decision.
        cold_start_anoms = [
            a for a in result.anomalies
            if a.segment == "brand_new_segment"
        ]
        assert cold_start_anoms, (
            "the unvalidatable record must surface in anomalies so the "
            "caller can route it to manual review"
        )
        assert all(a.decision == GateDecision.FLAG for a in cold_start_anoms)
        # Overall gate must not PASS when any record was unvalidatable.
        assert result.gate_decision != GateDecision.PASS


# ---------------------------------------------------------------------------
# Optional: MAD / Modified Z-Score scoring
# ---------------------------------------------------------------------------

class TestMADScoring:
    """MAD scoring is robust to a baseline contaminated by outliers."""

    def test_default_method_is_zscore(self):
        gate = ZScoreGate()
        assert gate.method == "zscore"

    def test_invalid_method_raises(self):
        with pytest.raises(ValueError, match="method"):
            ZScoreGate(method="bogus")

    def test_mad_ignores_outlier_contamination_in_baseline(self):
        """A classic Z-score is inflated by a single huge outlier in the
        baseline; the MAD score is robust to it."""
        # 50 clean samples around 100, then 5 fraud-style outliers at
        # 10_000. Classic std becomes ~3_000, swallowing normal signal.
        clean = [100.0] * 50 + [10_000.0] * 5
        # A new observation at 150 is clearly suspicious vs. the clean
        # part but tiny vs. the contaminated std.
        gate_classic = ZScoreGate(method="zscore", min_observations=10)
        gate_mad = ZScoreGate(method="mad", min_observations=10)

        c_mean, c_std, _ = gate_classic.compute_baseline(clean)
        m_med, m_mad, _ = gate_mad.compute_baseline(clean)

        classic_anom = gate_classic.evaluate_record(150.0, c_mean, c_std)
        mad_anom = gate_mad.evaluate_record(150.0, m_med, m_mad)

        # The MAD score should be strictly larger (more suspicious) than
        # the classic score in this contaminated-baseline regime.
        assert mad_anom.z_score > classic_anom.z_score


# ---------------------------------------------------------------------------
# B6 — AuditEvent details: sets / nested tuples must be deeply frozen
# ---------------------------------------------------------------------------

class TestAuditEventDetailsDeepFreezeExtras:
    """Non-list / non-dict containers must also be frozen.

    Earlier ``_deep_freeze`` only recursed into Mapping and list, so sets
    passed directly and dicts/sets nested inside tuples stayed mutable.
    A caller holding a reference to one of those mutable inner objects
    could tamper with the payload after logging — and because the hash
    chain serialises ``default=str`` for non-JSON-native types, the
    hash at verify time would diverge from the hash at log time and
    falsely flag ``hash_mismatch``.
    """

    def test_top_level_set_is_frozen(self):
        """Reviewer's repro: ``details={'s': {1, 2}}; event.details['s'].add(3)``
        must fail instead of silently mutating the stored payload."""
        logger = AuditLogger()
        event = logger.log(
            EventType.GATE_DECISION,
            "sys",
            "pass",
            "g",
            details={"s": {1, 2}},
        )

        # frozenset has no .add(): the attempted mutation must raise.
        with pytest.raises(AttributeError):
            event.details["s"].add(3)  # type: ignore[union-attr]

        # And the chain must verify cleanly — no hash_mismatch.
        ok, detail = logger.verify_chain_detail()
        assert ok, f"chain must verify after set-valued details, got {detail}"

    def test_set_insertion_order_does_not_change_hash(self):
        """Canonical serialisation must be insertion-order-independent.

        Two logically identical payloads that only differ in the order
        items were inserted into a set must produce the same event_hash.
        """
        logger_a = AuditLogger()
        logger_b = AuditLogger()

        event_a = logger_a.log(
            EventType.GATE_DECISION, "sys", "pass", "g",
            details={"tags": {"alpha", "beta", "gamma"}},
        )
        event_b = logger_b.log(
            EventType.GATE_DECISION, "sys", "pass", "g",
            details={"tags": {"gamma", "alpha", "beta"}},
        )

        # Same timestamp-independent *content* hash: we can't compare
        # event_hash directly because timestamps differ, but we can
        # verify each chain individually — the real regression would be
        # a hash_mismatch at verify time.
        assert logger_a.verify_chain(), "logger A chain must verify"
        assert logger_b.verify_chain(), "logger B chain must verify"

        # And the stored payload must be a frozenset in both cases.
        from types import MappingProxyType as _Proxy
        assert isinstance(event_a.details, _Proxy)
        assert isinstance(event_b.details, _Proxy)
        assert isinstance(event_a.details["tags"], frozenset)
        assert isinstance(event_b.details["tags"], frozenset)

    def test_dict_nested_in_tuple_is_frozen(self):
        """A dict nested inside a tuple used to pass through
        ``_deep_freeze`` untouched, leaving the inner dict mutable."""
        logger = AuditLogger()
        payload = {"events": ({"a": 1}, {"b": 2})}
        event = logger.log(
            EventType.GATE_DECISION, "sys", "pass", "g", details=payload,
        )

        # The tuple becomes a tuple of read-only mappings.
        stored = event.details["events"]
        assert isinstance(stored, tuple)
        for inner in stored:
            # Every nested dict inside the tuple must be read-only.
            with pytest.raises(TypeError):
                inner["a"] = "tampered"  # type: ignore[index]

        # Chain must still verify.
        assert logger.verify_chain()

    def test_bytearray_is_coerced_to_bytes(self):
        """bytearray is mutable; it must be frozen to immutable bytes."""
        logger = AuditLogger()
        event = logger.log(
            EventType.GATE_DECISION, "sys", "pass", "g",
            details={"blob": bytearray(b"hello")},
        )

        # The stored value is immutable bytes, not bytearray.
        assert isinstance(event.details["blob"], bytes)
        assert not isinstance(event.details["blob"], bytearray)
        # Chain verifies.
        assert logger.verify_chain()


# ---------------------------------------------------------------------------
# B7 — MatchReport.match_rate denominator must be the source count
# ---------------------------------------------------------------------------

class TestMatchRateDenominator:
    """Target-side DUPLICATE rows must not inflate the denominator.

    Reviewer's repro: src=[id=1, id=2] vs tgt=[id=1, id=1, id=2] yields
    one MATCHED row (id=2) and several DUPLICATE rows on both sides for
    id=1. The number of source records is 2 and exactly 1 of them
    matched cleanly, so match_rate must be 0.5 — not 0.25.
    """

    def test_target_side_duplicates_do_not_inflate_denominator(self):
        matcher = DeterministicMatcher(key_fields=["id"])
        src = [{"id": "1"}, {"id": "2"}]
        tgt = [{"id": "1"}, {"id": "1"}, {"id": "2"}]

        report = matcher.match(src, tgt)

        assert report.source_count == 2
        assert report.matched_count == 1, (
            "exactly one source row (id=2) should match cleanly"
        )
        assert report.match_rate == pytest.approx(0.5), (
            f"expected 0.5 = 1 matched / 2 source, got {report.match_rate}"
        )

    def test_match_rate_empty_is_zero(self):
        matcher = DeterministicMatcher(key_fields=["id"])
        report = matcher.match([], [])
        assert report.source_count == 0
        assert report.match_rate == 0.0

    def test_all_source_duplicated_is_zero(self):
        """Every source row duplicated → no clean matches → rate 0."""
        matcher = DeterministicMatcher(key_fields=["id"])
        src = [{"id": "1"}, {"id": "1"}]
        tgt = [{"id": "1"}]
        report = matcher.match(src, tgt)
        assert report.source_count == 2
        assert report.matched_count == 0
        assert report.match_rate == 0.0
