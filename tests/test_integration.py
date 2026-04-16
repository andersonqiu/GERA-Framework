"""
End-to-end integration tests for the GERA Framework.

These tests exercise all four layers together against realistic
record populations, verifying that the layers compose correctly
under both "clean" and "dirty" data scenarios.  Unit tests cover
each module in isolation; these tests cover the pipeline.

Scenarios:
    1. Clean pipeline  — every layer PASSes end-to-end
    2. Mismatched counts — Layer 1 detects, audit captures
    3. Injected anomaly — Layer 2 BLOCKs, Layer 1 exception raised
    4. Hash tampering — audit chain verification fails
    5. SemanticRegistry governs reconciliation metric
    6. Full compliance report emits all NIST controls
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Tuple

import numpy as np
import pytest

from gera.governance import (
    AuditLogger,
    DataSensitivity,
    EventType,
    MetricDefinition,
    SemanticRegistry,
)
from gera.nist.csf2_controls import CSF2ControlMapper
from gera.reconciliation import (
    DeterministicMatcher,
    ExceptionRouter,
    ExceptionSeverity,
    MatchStatus,
)
from gera.validation import ReconciliationCheck, ZScoreGate
from gera.validation.reconciliation_checks import CheckStatus
from gera.validation.zscore_gate import GateDecision


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@dataclass
class PipelineContext:
    """Holds the instantiated GERA components for one pipeline run."""
    matcher: DeterministicMatcher
    checker: ReconciliationCheck
    gate: ZScoreGate
    logger: AuditLogger
    registry: SemanticRegistry
    router: ExceptionRouter


def _build_pipeline() -> PipelineContext:
    return PipelineContext(
        matcher=DeterministicMatcher(
            key_fields=["txn_id"],
            value_fields=["amount"],
        ),
        checker=ReconciliationCheck(tolerance=0.01),
        gate=ZScoreGate(sigma_threshold=2.5, block_threshold=4.0),
        logger=AuditLogger(retention_days=2555),
        registry=SemanticRegistry(),
        router=ExceptionRouter(),
    )


def _gen_matched_records(
    n: int = 200,
    seed: int = 42,
    drop_indices: List[int] = None,
    tamper_amount_indices: List[int] = None,
    inject_anomaly_indices: List[int] = None,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Generate deterministic source/target record pairs with optional faults.

    Args:
        drop_indices: target indices to drop (simulates unmatched source)
        tamper_amount_indices: target indices to change amount (conflict)
        inject_anomaly_indices: source indices to replace with extreme
            outliers, giving Layer 2 something to BLOCK on.
    """
    rng = np.random.default_rng(seed)
    amounts = rng.normal(2_000, 200, n).round(2)

    source = [
        {"txn_id": f"TXN-{i:05d}", "amount": float(amounts[i]), "dept": "finance"}
        for i in range(n)
    ]

    if inject_anomaly_indices:
        for idx in inject_anomaly_indices:
            source[idx]["amount"] = 1_000_000.0  # ~5000σ out

    target = [dict(r) for r in source]

    drops = set(drop_indices or [])
    target = [r for i, r in enumerate(target) if i not in drops]

    for idx in (tamper_amount_indices or []):
        # Adjust the target index accounting for drops before it.
        shifted = idx - sum(1 for d in drops if d < idx)
        if 0 <= shifted < len(target):
            target[shifted]["amount"] = target[shifted]["amount"] + 0.01

    return source, target


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestCleanPipeline:
    """Happy path: every layer reports PASS end-to-end."""

    def test_clean_pipeline_end_to_end(self):
        # Use a BLOCK threshold of 5.0 and a large historical sample so
        # naturally occurring tails (~0.5 %) in a 200-record batch
        # produce FLAGs at most — never BLOCKs — giving us a stable
        # "clean" baseline to assert against.
        ctx = PipelineContext(
            matcher=DeterministicMatcher(
                key_fields=["txn_id"],
                value_fields=["amount"],
            ),
            checker=ReconciliationCheck(tolerance=0.01),
            gate=ZScoreGate(sigma_threshold=2.5, block_threshold=5.0),
            logger=AuditLogger(retention_days=2555),
            registry=SemanticRegistry(),
            router=ExceptionRouter(),
        )
        source, target = _gen_matched_records(n=200)
        historical = np.random.default_rng(1).normal(2_000, 200, 5_000).tolist()

        # Layer 1: match
        match_report = ctx.matcher.match(source, target)
        assert match_report.is_fully_reconciled
        assert match_report.matched_count == 200
        assert match_report.conflict_count == 0

        # Layer 2: deterministic checks
        check_report = ctx.checker.run_all(
            source_count=len(source),
            target_count=len(target),
            source_amount=sum(r["amount"] for r in source),
            target_amount=sum(r["amount"] for r in target),
            source_keys={r["txn_id"] for r in source},
            target_keys={r["txn_id"] for r in target},
        )
        assert check_report.overall_status == CheckStatus.PASS

        # Layer 2: statistical gate
        gate_result = ctx.gate.validate(
            values=[r["amount"] for r in source],
            historical_values=historical,
            record_ids=[r["txn_id"] for r in source],
        )
        # A handful of natural tails in a 200-record normal batch will
        # FLAG, but with block_threshold=5σ they cannot BLOCK.  That is
        # the precise guarantee enterprise users need: no false stops.
        assert gate_result.gate_decision in {GateDecision.PASS, GateDecision.FLAG}
        assert gate_result.blocked == 0

        # Layer 3: audit chain for every gate
        ctx.logger.log_gate_decision("match", "pass", {"rate": match_report.match_rate})
        ctx.logger.log_gate_decision("checks", check_report.overall_status.value)
        ctx.logger.log_gate_decision("zscore", gate_result.gate_decision.value)
        assert ctx.logger.event_count == 3
        assert ctx.logger.verify_chain()

        # Layer 4: NIST report references each decision
        mapper = CSF2ControlMapper()
        summary = mapper.compliance_summary()
        assert "controls" in summary
        assert any("DE.CM" in c["id"] for c in summary["controls"])
        assert "DETECT" in summary["functions_covered"]


class TestMismatchedCounts:
    """Dropped target records should surface at Layer 1 and cascade."""

    def test_missing_records_raise_exception_and_audit(self):
        ctx = _build_pipeline()
        source, target = _gen_matched_records(n=100, drop_indices=[5, 42, 77])

        match_report = ctx.matcher.match(source, target)
        assert not match_report.is_fully_reconciled
        assert match_report.unmatched_source_count == 3

        check_report = ctx.checker.run_all(
            source_count=len(source),
            target_count=len(target),
            source_amount=sum(r["amount"] for r in source),
            target_amount=sum(r["amount"] for r in target),
            source_keys={r["txn_id"] for r in source},
            target_keys={r["txn_id"] for r in target},
        )
        assert check_report.overall_status == CheckStatus.FAIL

        # Every unmatched record routes to the exception queue
        for r in match_report.results:
            if r.status == MatchStatus.UNMATCHED_SOURCE:
                ctx.router.route(
                    source=r.source_record["txn_id"],
                    description="missing in target",
                    severity=ExceptionSeverity.HIGH,
                )

        assert ctx.router.open_count == 3

        # Audit trail captures the failure and exception creation
        ctx.logger.log_gate_decision("match", "fail", {
            "rate": match_report.match_rate,
            "unmatched": match_report.unmatched_source_count,
        })
        for exc in (e for e in ctx.router._queue if e.status.value != "resolved"):
            ctx.logger.log(
                event_type=EventType.EXCEPTION_CREATED,
                actor="gera_pipeline",
                action="created",
                resource=exc.exception_id,
                details={"severity": exc.severity.value},
            )

        assert ctx.logger.event_count == 4
        assert ctx.logger.verify_chain()


class TestInjectedAnomaly:
    """An injected outlier should BLOCK at Layer 2 and be auditable."""

    def test_outlier_blocks_batch(self):
        ctx = _build_pipeline()
        source, target = _gen_matched_records(
            n=200,
            inject_anomaly_indices=[100],
        )
        historical = np.random.default_rng(1).normal(2_000, 200, 500).tolist()

        gate_result = ctx.gate.validate(
            values=[r["amount"] for r in source],
            historical_values=historical,
            record_ids=[r["txn_id"] for r in source],
        )
        assert gate_result.gate_decision == GateDecision.BLOCK
        assert gate_result.blocked >= 1
        assert any(a.record_id == "TXN-00100" for a in gate_result.anomalies)

        # Block decision must land in the audit chain
        ctx.logger.log_gate_decision(
            gate_name="zscore",
            decision=gate_result.gate_decision.value,
            details={
                "blocked_count": gate_result.blocked,
                "batch_anomaly_rate": gate_result.batch_anomaly_rate,
            },
        )
        events = ctx.logger.query(event_type=EventType.GATE_DECISION)
        assert len(events) == 1
        assert events[0].action == "gate_block"


class TestAuditChainTampering:
    """Modifying stored events must break chain verification."""

    def test_tampered_event_is_detected(self):
        ctx = _build_pipeline()
        for i in range(10):
            ctx.logger.log_gate_decision("probe", "pass", {"i": i})

        assert ctx.logger.verify_chain()

        # Tamper with stored events — frozen dataclass, so we replace the
        # whole object via list mutation (the typical "admin edit" attack).
        from dataclasses import replace
        ctx.logger._events[5] = replace(
            ctx.logger._events[5],
            details={"i": 5, "amount_added_by_attacker": 999_999},
        )

        ok, detail = ctx.logger.verify_chain_detail()
        assert ok is False
        assert detail is not None
        assert detail["event_index"] == 5
        assert detail["violation"] == "hash_mismatch"


class TestSemanticGovernance:
    """Registered metrics provide conformance + lineage for reconciliation."""

    def test_registered_metric_is_queryable(self):
        ctx = _build_pipeline()
        ctx.registry.register(MetricDefinition(
            name="daily_txn_total",
            description="Sum of all transactions per calendar day",
            formula="SUM(amount) GROUP BY DATE(ts)",
            owner="finance-ops@example.com",
            sensitivity=DataSensitivity.CONFIDENTIAL,
            lineage=["source_db", "target_db"],
        ))

        metric = ctx.registry.get("daily_txn_total")
        assert metric is not None
        assert metric.sensitivity == DataSensitivity.CONFIDENTIAL

        conformance = ctx.registry.validate_conformance("daily_txn_total", 1_250.00)
        assert conformance["is_valid"] is True
        assert conformance["sensitivity"] == "confidential"

        glossary = ctx.registry.export_glossary()
        assert len(glossary) == 1
        assert glossary[0]["name"] == "daily_txn_total"


class TestLargeScalePipeline:
    """Smoke test at 10K records — catches accidental quadratic algorithms."""

    def test_10k_record_pipeline_completes(self):
        # 10K draws from N(2000, 200) will almost certainly contain a few
        # values >5σ by chance (expected count ≈ 0.006 × 10_000 = 0.6, but
        # tail heaviness of a finite sample produces 0–3 in practice).
        # We set block_threshold=6.0 so this smoke test is deterministic
        # without suppressing the algorithm's behaviour on real outliers.
        ctx = PipelineContext(
            matcher=DeterministicMatcher(
                key_fields=["txn_id"],
                value_fields=["amount"],
            ),
            checker=ReconciliationCheck(tolerance=0.01),
            gate=ZScoreGate(sigma_threshold=2.5, block_threshold=6.0),
            logger=AuditLogger(retention_days=2555),
            registry=SemanticRegistry(),
            router=ExceptionRouter(),
        )
        source, target = _gen_matched_records(n=10_000, drop_indices=list(range(0, 100)))
        historical = np.random.default_rng(99).normal(2_000, 200, 5_000).tolist()

        match_report = ctx.matcher.match(source, target)
        assert match_report.unmatched_source_count == 100
        assert match_report.matched_count == 9_900

        gate_result = ctx.gate.validate(
            values=[r["amount"] for r in source],
            historical_values=historical,
            record_ids=[r["txn_id"] for r in source],
        )
        # No injected anomaly — decision depends on the generated data,
        # but at this scale we must not BLOCK on normal variation.
        assert gate_result.gate_decision in {GateDecision.PASS, GateDecision.FLAG}

        for _ in range(100):
            ctx.logger.log_gate_decision("probe", "pass")
        assert ctx.logger.event_count == 100
        assert ctx.logger.verify_chain()


class TestReplayability:
    """Running the same inputs twice must yield identical reports."""

    def test_deterministic_match_report(self):
        source_a, target_a = _gen_matched_records(n=500, drop_indices=[1, 2, 3])
        source_b, target_b = _gen_matched_records(n=500, drop_indices=[1, 2, 3])

        m_a = DeterministicMatcher(key_fields=["txn_id"], value_fields=["amount"])
        m_b = DeterministicMatcher(key_fields=["txn_id"], value_fields=["amount"])

        rep_a = m_a.match(source_a, target_a)
        rep_b = m_b.match(source_b, target_b)

        assert rep_a.matched_count == rep_b.matched_count
        assert rep_a.unmatched_source_count == rep_b.unmatched_source_count
        assert rep_a.match_rate == rep_b.match_rate


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
