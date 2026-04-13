"""Tests for Layer 1: Reconciliation and governance modules."""

import pytest
from dataclasses import FrozenInstanceError

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

    def test_key_normalization(self):
        src = [{"id": " ABC ", "amount": 100}]
        tgt = [{"id": "abc", "amount": 100}]
        report = self.matcher.match(src, tgt)
        assert report.matched_count == 1

    def test_duplicate_target_keys(self):
        src = [{"id": "1", "amount": 100}]
        tgt = [{"id": "1", "amount": 100}, {"id": "1", "amount": 200}]
        report = self.matcher.match(src, tgt)
        assert any(
            r.status == MatchStatus.DUPLICATE for r in report.results
        )

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

    def test_resolve_exception(self):
        exc = self.router.route("recon", "test", ExceptionSeverity.LOW)
        assert self.router.resolve(exc.exception_id, "Fixed")
        assert exc.status == ExceptionStatus.RESOLVED
        assert exc.resolution_notes == "Fixed"

    def test_resolve_nonexistent(self):
        assert not self.router.resolve("EXC-999999")

    def test_open_count(self):
        self.router.route("a", "x", ExceptionSeverity.LOW)
        self.router.route("b", "y", ExceptionSeverity.HIGH)
        exc3 = self.router.route("c", "z", ExceptionSeverity.MEDIUM)
        self.router.resolve(exc3.exception_id)
        assert self.router.open_count == 2

    def test_queue_summary(self):
        self.router.route("a", "x", ExceptionSeverity.LOW)
        self.router.route("b", "y", ExceptionSeverity.HIGH)
        summary = self.router.get_queue_summary()
        assert summary["total"] == 2
        assert summary["open"] == 2
        assert "low" in summary["by_severity"]
        assert "high" in summary["by_severity"]


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

    def test_immutable_events(self):
        event = self.logger.log(
            EventType.SYSTEM_EVENT, "sys", "start", "pipeline"
        )
        with pytest.raises((FrozenInstanceError, AttributeError)):
            event.action = "tampered"
