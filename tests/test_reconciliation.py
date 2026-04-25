"""Tests for Layer 1 (Reconciliation), Layer 3 (Governance), and Layer 4 (NIST)."""

import json
import pytest
from dataclasses import FrozenInstanceError
from pathlib import Path

from gera.reconciliation.deterministic_matcher import (
    DeterministicMatcher,
    MatchStatus,
)
from gera.reconciliation.exception_router import (
    ExceptionRouter,
    ExceptionSeverity,
    ExceptionStatus,
)
from gera.governance.audit_logger import AuditLogger, EventType
from gera.governance.semantic_registry import (
    SemanticRegistry,
    MetricDefinition,
    DataSensitivity,
)
from gera.nist.csf2_controls import CSF2ControlMapper


# ---------------------------------------------------------------------------
# Layer 1 — DeterministicMatcher
# ---------------------------------------------------------------------------

class TestDeterministicMatcher:
    """Tests for cross-system record matching."""

    def setup_method(self):
        self.matcher = DeterministicMatcher(
            key_fields=["id"],
            value_fields=["amount"],
        )

    def test_perfect_match(self):
        src = [{"id": "1", "amount": 100}]
        tgt = [{"id": "1", "amount": 100}]
        report = self.matcher.match(src, tgt)
        assert report.matched_count == 1
        assert report.is_fully_reconciled

    def test_missing_target(self):
        src = [{"id": "1", "amount": 100}, {"id": "2", "amount": 200}]
        tgt = [{"id": "1", "amount": 100}]
        report = self.matcher.match(src, tgt)
        assert report.unmatched_source_count == 1

    def test_extra_target(self):
        src = [{"id": "1", "amount": 100}]
        tgt = [{"id": "1", "amount": 100}, {"id": "2", "amount": 200}]
        report = self.matcher.match(src, tgt)
        assert report.unmatched_target_count == 1

    def test_value_conflict(self):
        src = [{"id": "1", "amount": 100}]
        tgt = [{"id": "1", "amount": 999}]
        report = self.matcher.match(src, tgt)
        assert report.conflict_count == 1
        assert report.results[0].conflicts[0] == ("amount", 100, 999)

    def test_composite_key(self):
        m = DeterministicMatcher(key_fields=["dept", "id"])
        src = [{"dept": "fin", "id": "1"}]
        tgt = [{"dept": "fin", "id": "1"}]
        report = m.match(src, tgt)
        assert report.is_fully_reconciled

    def test_key_normalization_enabled(self):
        matcher = DeterministicMatcher(
            key_fields=["id"],
            value_fields=["amount"],
            normalize_keys=True,
        )
        src = [{"id": " ABC ", "amount": 100}]
        tgt = [{"id": "abc", "amount": 100}]
        report = matcher.match(src, tgt)
        assert report.matched_count == 1

    def test_key_normalization_disabled(self):
        matcher = DeterministicMatcher(
            key_fields=["id"],
            value_fields=["amount"],
            normalize_keys=False,
        )
        src = [{"id": " ABC ", "amount": 100}]
        tgt = [{"id": "abc", "amount": 100}]
        report = matcher.match(src, tgt)
        assert report.matched_count == 0
        assert report.unmatched_source_count == 1
        assert report.unmatched_target_count == 1

    def test_duplicate_target_keys(self):
        src = [{"id": "1", "amount": 100}]
        tgt = [{"id": "1", "amount": 100}, {"id": "1", "amount": 200}]
        report = self.matcher.match(src, tgt)
        assert any(r.status == MatchStatus.DUPLICATE for r in report.results)

    def test_match_rate(self):
        src = [
            {"id": "1", "amount": 100},
            {"id": "2", "amount": 200},
            {"id": "3", "amount": 300},
        ]
        tgt = [
            {"id": "1", "amount": 100},
            {"id": "2", "amount": 200},
        ]
        report = self.matcher.match(src, tgt)
        assert abs(report.match_rate - 2 / 3) < 0.01

    def test_negative_amounts_match(self):
        """Debits (negative amounts) should reconcile correctly."""
        src = [{"id": "1", "amount": 1000.0}, {"id": "2", "amount": -500.0}]
        tgt = [{"id": "1", "amount": 1000.0}, {"id": "2", "amount": -500.0}]
        report = self.matcher.match(src, tgt)
        assert report.matched_count == 2
        assert report.is_fully_reconciled

    def test_negative_amount_conflict_detected(self):
        """A sign flip (debit vs credit) should be a conflict."""
        src = [{"id": "1", "amount": -500.0}]
        tgt = [{"id": "1", "amount": 500.0}]
        report = self.matcher.match(src, tgt)
        assert report.conflict_count == 1

    def test_empty_source_and_target(self):
        """Both empty datasets should produce an empty, fully-reconciled report."""
        report = self.matcher.match([], [])
        assert report.matched_count == 0
        assert report.is_fully_reconciled

    def test_empty_source_nonempty_target(self):
        tgt = [{"id": "1", "amount": 100}]
        report = self.matcher.match([], tgt)
        assert report.unmatched_target_count == 1

    def test_empty_key_fields_raises(self):
        """Empty key_fields list should raise ValueError at construction."""
        with pytest.raises(ValueError, match="key_fields must not be empty"):
            DeterministicMatcher(key_fields=[])


# ---------------------------------------------------------------------------
# Layer 1 — ExceptionRouter
# ---------------------------------------------------------------------------

class TestExceptionRouter:
    """Tests for FIFO exception queue."""

    def setup_method(self):
        self.router = ExceptionRouter()

    def test_route_exception(self):
        exc = self.router.route(
            source="recon",
            description="Missing record",
            severity=ExceptionSeverity.HIGH,
        )
        assert exc.exception_id == "EXC-000001"
        assert exc.status == ExceptionStatus.OPEN

    def test_resolve_exception_updates_internal_queue(self):
        """resolve() must update the object stored inside the queue, not just the returned ref."""
        exc = self.router.route("recon", "test", ExceptionSeverity.LOW)
        exc_id = exc.exception_id

        assert self.router.resolve(exc_id, "Fixed")

        # Retrieve directly from the internal queue to confirm the state changed.
        queued = next(e for e in self.router._queue if e.exception_id == exc_id)
        assert queued.status == ExceptionStatus.RESOLVED
        assert queued.resolution_notes == "Fixed"
        assert queued.resolved_at is not None

    def test_resolve_nonexistent(self):
        assert not self.router.resolve("EXC-999999")

    def test_open_count(self):
        exc1 = self.router.route("a", "x", ExceptionSeverity.LOW)
        exc2 = self.router.route("b", "y", ExceptionSeverity.HIGH)
        exc3 = self.router.route("c", "z", ExceptionSeverity.MEDIUM)

        assert self.router.open_count == 3

        self.router.resolve(exc3.exception_id)
        assert self.router.open_count == 2

        # exc1 and exc2 must still be open.
        still_open = [
            e for e in self.router._queue
            if e.status != ExceptionStatus.RESOLVED
        ]
        assert {e.exception_id for e in still_open} == {
            exc1.exception_id, exc2.exception_id
        }

    def test_queue_summary(self):
        self.router.route("a", "x", ExceptionSeverity.LOW)
        self.router.route("b", "y", ExceptionSeverity.HIGH)
        summary = self.router.get_queue_summary()
        assert summary["total"] == 2
        assert summary["open"] == 2
        assert "low" in summary["by_severity"]
        assert "high" in summary["by_severity"]

    def test_escalate_overdue_is_idempotent(self):
        """Calling escalate_overdue() twice should not double-escalate."""
        exc = self.router.route("src", "desc", ExceptionSeverity.CRITICAL)
        # Force breach: back-date creation by 2 hours (> 1-hour SLA).
        from datetime import timedelta, timezone
        from datetime import datetime
        exc.created_at = datetime.now(timezone.utc) - timedelta(hours=2)

        first = self.router.escalate_overdue()
        second = self.router.escalate_overdue()

        assert len(first) == 1
        assert len(second) == 0  # Already escalated — no double-escalation.
        assert exc.status == ExceptionStatus.ESCALATED

    def test_warehouse_sla_views_match_python_enum_values(self):
        repo_root = Path(__file__).resolve().parents[1]
        for sql_path in (
            repo_root / "sql" / "bigquery" / "04_exceptions_queue.sql",
            repo_root / "sql" / "athena" / "04_exceptions_queue.sql",
        ):
            sql = sql_path.read_text()
            assert sql.count("CASE UPPER(e.severity)") == 2
            assert "WHEN 'CRITICAL' THEN 1" in sql


# ---------------------------------------------------------------------------
# Layer 3 — AuditLogger
# ---------------------------------------------------------------------------

class TestAuditLogger:
    """Tests for append-only audit logging."""

    def setup_method(self):
        self.logger = AuditLogger()

    def test_log_and_verify_chain(self):
        self.logger.log(EventType.GATE_DECISION, "system", "pass", "gate1")
        self.logger.log(EventType.DATA_ACCESS, "user1", "read", "table_x")
        self.logger.log(EventType.RECONCILIATION, "system", "complete", "batch_1")
        assert self.logger.event_count == 3
        assert self.logger.verify_chain()

    def test_query_by_type(self):
        self.logger.log(EventType.GATE_DECISION, "sys", "pass", "g1")
        self.logger.log(EventType.DATA_ACCESS, "usr", "read", "t1")
        self.logger.log(EventType.GATE_DECISION, "sys", "fail", "g2")
        results = self.logger.query(event_type=EventType.GATE_DECISION)
        assert len(results) == 2

    def test_query_compound_filters(self):
        """query() must support multiple simultaneous filters."""
        self.logger.log(EventType.GATE_DECISION, "sys", "pass", "g1")
        self.logger.log(EventType.GATE_DECISION, "user1", "pass", "g1")
        self.logger.log(EventType.DATA_ACCESS, "sys", "read", "t1")

        results = self.logger.query(
            event_type=EventType.GATE_DECISION,
            actor="sys",
        )
        assert len(results) == 1
        assert results[0].action == "pass"
        assert results[0].actor == "sys"

    def test_query_returns_empty_list_when_no_match(self):
        self.logger.log(EventType.SYSTEM_EVENT, "sys", "start", "pipeline")
        results = self.logger.query(event_type=EventType.POLICY_VIOLATION)
        assert results == []

    def test_log_access_convenience(self):
        event = self.logger.log_access(actor="alice", resource="table_x")
        assert event.event_type == EventType.DATA_ACCESS
        assert event.actor == "alice"
        assert event.action == "read"

    def test_log_gate_decision_convenience(self):
        event = self.logger.log_gate_decision("zscore_gate", "pass")
        assert event.event_type == EventType.GATE_DECISION
        assert event.actor == "gera_pipeline"
        assert "pass" in event.action

    def test_immutable_events(self):
        event = self.logger.log(EventType.SYSTEM_EVENT, "sys", "start", "pipeline")
        with pytest.raises((FrozenInstanceError, AttributeError)):
            event.action = "tampered"

    def test_verify_chain_detail_on_intact_chain(self):
        self.logger.log(EventType.SYSTEM_EVENT, "sys", "start", "pipeline")
        self.logger.log(EventType.SYSTEM_EVENT, "sys", "end", "pipeline")
        valid, detail = self.logger.verify_chain_detail()
        assert valid is True
        assert detail is None

    def test_verify_chain_detail_on_empty_logger(self):
        valid, detail = self.logger.verify_chain_detail()
        assert valid is True
        assert detail is None

    def test_export_produces_valid_json(self):
        """export() must produce RFC-7159-compliant JSON (no bare Infinity tokens)."""
        self.logger.log(EventType.SYSTEM_EVENT, "sys", "start", "pipeline")
        self.logger.log(
            EventType.GATE_DECISION,
            "sys",
            "flag",
            "zscore",
            details={"z_score": float("inf"), "label": "overflow"},
        )
        exported = self.logger.export()
        # Must parse without error.
        data = json.loads(exported)
        assert len(data) == 2
        assert all("event_id" in e for e in data)
        assert all("event_hash" in e for e in data)
        # Infinity must have been replaced with a string sentinel.
        gate_event = next(e for e in data if e["event_type"] == "gate_decision")
        assert gate_event["details"]["z_score"] == "Infinity"


# ---------------------------------------------------------------------------
# Layer 3 — SemanticRegistry
# ---------------------------------------------------------------------------

class TestSemanticRegistry:
    """Tests for the governed metric registry."""

    def _make_metric(self, name="revenue_total", **kwargs) -> MetricDefinition:
        defaults = dict(
            name=name,
            description=f"Governed metric: {name}",
            formula="SUM(amount)",
            owner="finance",
            lineage=["erp.sales", "crm.orders"],
        )
        defaults.update(kwargs)
        return MetricDefinition(**defaults)

    def setup_method(self):
        self.registry = SemanticRegistry()

    def test_register_and_get(self):
        m = self._make_metric()
        self.registry.register(m)
        retrieved = self.registry.get("revenue_total")
        # Registry deep-copies on register and on get so that external
        # callers cannot bypass update()'s versioning. The returned object
        # must therefore be a structural equal but a distinct instance.
        assert retrieved is not m
        assert retrieved == m
        assert retrieved.name == "revenue_total"

    def test_get_nonexistent_returns_none(self):
        assert self.registry.get("no_such_metric") is None

    def test_register_duplicate_raises(self):
        self.registry.register(self._make_metric())
        with pytest.raises(ValueError, match="already registered"):
            self.registry.register(self._make_metric())

    def test_update_bumps_version(self):
        self.registry.register(self._make_metric())
        updated = self.registry.update("revenue_total", description="Updated description")
        assert updated.version == 2
        assert updated.description == "Updated description"

    def test_update_rejects_immutable_fields(self):
        self.registry.register(self._make_metric())
        with pytest.raises(ValueError, match="cannot be updated"):
            self.registry.update("revenue_total", name="new_name")
        with pytest.raises(ValueError, match="cannot be updated"):
            self.registry.update("revenue_total", version=99)

    def test_update_rejects_wrong_sensitivity_type(self):
        self.registry.register(self._make_metric())
        with pytest.raises(TypeError, match="DataSensitivity"):
            self.registry.update("revenue_total", sensitivity="RESTRICTED")

    def test_update_accepts_valid_sensitivity(self):
        self.registry.register(self._make_metric())
        updated = self.registry.update(
            "revenue_total", sensitivity=DataSensitivity.RESTRICTED
        )
        assert updated.sensitivity == DataSensitivity.RESTRICTED

    def test_update_nonexistent_raises(self):
        with pytest.raises(KeyError):
            self.registry.update("no_such_metric", description="x")

    def test_search_by_name(self):
        self.registry.register(self._make_metric("revenue_total"))
        self.registry.register(self._make_metric("revenue_net"))
        self.registry.register(self._make_metric("cost_total"))
        results = self.registry.search("revenue")
        assert len(results) == 2
        assert all("revenue" in m.name for m in results)

    def test_search_by_description(self):
        m = self._make_metric("cost_total", description="Total cost of goods sold")
        self.registry.register(m)
        results = self.registry.search("cost of goods")
        assert len(results) == 1

    def test_search_returns_empty_on_no_match(self):
        self.registry.register(self._make_metric())
        assert self.registry.search("zzz_no_match") == []

    def test_validate_conformance_passes_for_complete_metric(self):
        self.registry.register(self._make_metric())
        result = self.registry.validate_conformance("revenue_total", 12345.67)
        assert result["is_valid"] is True
        assert result["checks"]["has_lineage"] is True
        assert result["checks"]["has_formula"] is True

    def test_validate_conformance_fails_without_lineage(self):
        m = self._make_metric(lineage=[])
        self.registry.register(m)
        result = self.registry.validate_conformance("revenue_total", 100.0)
        assert result["is_valid"] is False
        assert result["checks"]["has_lineage"] is False

    def test_validate_conformance_for_unknown_metric(self):
        result = self.registry.validate_conformance("unknown", 0)
        assert result["is_valid"] is False
        assert "not found" in result["error"]

    def test_export_glossary_sorted_alphabetically(self):
        self.registry.register(self._make_metric("zzz_metric"))
        self.registry.register(self._make_metric("aaa_metric"))
        glossary = self.registry.export_glossary()
        assert glossary[0]["name"] == "aaa_metric"
        assert glossary[1]["name"] == "zzz_metric"

    def test_export_glossary_contains_required_keys(self):
        self.registry.register(self._make_metric())
        glossary = self.registry.export_glossary()
        required = {"name", "description", "formula", "owner", "sensitivity", "version"}
        assert required.issubset(set(glossary[0].keys()))

    def test_count_property(self):
        assert self.registry.count == 0
        self.registry.register(self._make_metric("m1"))
        self.registry.register(self._make_metric("m2"))
        assert self.registry.count == 2


# ---------------------------------------------------------------------------
# Layer 4 — CSF2ControlMapper
# ---------------------------------------------------------------------------

class TestCSF2ControlMapper:
    """Tests for NIST CSF 2.0 compliance mapping."""

    def setup_method(self):
        self.mapper = CSF2ControlMapper()

    def test_get_control_valid_id(self):
        control = self.mapper.get_control("GV.OC-01")
        assert control is not None
        assert control.control_id == "GV.OC-01"
        assert control.function_name == "GOVERN"

    def test_get_control_invalid_id_returns_none(self):
        assert self.mapper.get_control("INVALID-99") is None

    def test_all_expected_controls_present(self):
        expected = {"GV.OC-01", "GV.RM-01", "PR.AA-01", "PR.DS-01", "DE.CM-01"}
        for cid in expected:
            assert self.mapper.get_control(cid) is not None, f"Missing control: {cid}"

    def test_compliance_summary_structure(self):
        summary = self.mapper.compliance_summary()
        assert summary["framework"] == "NIST CSF 2.0"
        assert "total_controls_mapped" in summary
        assert "functions_covered" in summary
        assert "mapping_coverage_pct" in summary
        assert summary["assessment_scope"] == "engineering_control_mapping_not_certification"
        assert "controls" in summary
        assert isinstance(summary["controls"], list)
        assert len(summary["controls"]) > 0

    def test_compliance_summary_covers_govern_protect_detect(self):
        summary = self.mapper.compliance_summary()
        functions = set(summary["functions_covered"])
        assert "GOVERN" in functions
        assert "PROTECT" in functions
        assert "DETECT" in functions

    def test_compliance_summary_controls_have_required_keys(self):
        summary = self.mapper.compliance_summary()
        for ctrl in summary["controls"]:
            assert "id" in ctrl
            assert "function" in ctrl
            assert ctrl["evidence_status"] == "mapped"

    def test_generate_audit_report_is_string(self):
        report = self.mapper.generate_audit_report()
        assert isinstance(report, str)
        assert len(report) > 100

    def test_generate_audit_report_contains_header(self):
        report = self.mapper.generate_audit_report()
        assert "NIST CSF 2.0" in report
        assert "COMPLIANCE REPORT" in report

    def test_generate_audit_report_contains_all_control_ids(self):
        report = self.mapper.generate_audit_report()
        for m in self.mapper.mappings:
            assert m.control_id in report, f"Control {m.control_id} missing from report"

    def test_generate_audit_report_contains_evidence_artifacts(self):
        report = self.mapper.generate_audit_report()
        # Every evidence artifact should appear somewhere in the report.
        for m in self.mapper.mappings:
            for artifact in m.evidence_artifacts:
                assert artifact in report, f"Artifact '{artifact}' missing from report"

    def test_custom_mappings_accepted(self):
        from gera.nist.csf2_controls import CSF2Control
        custom = [
            CSF2Control(
                control_id="TEST-01",
                function_name="TEST",
                category="Test Category",
                description="A test control",
                gera_implementation="Implemented via test",
                evidence_artifacts=["test.json"],
            )
        ]
        mapper = CSF2ControlMapper(mappings=custom)
        assert mapper.get_control("TEST-01") is not None
        assert mapper.get_control("GV.OC-01") is None
