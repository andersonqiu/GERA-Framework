"""Tests for Layer 2: Validation modules."""

import pytest
import numpy as np

from gera.validation.zscore_gate import ZScoreGate, GateDecision
from gera.validation.reconciliation_checks import ReconciliationCheck, CheckStatus
from gera.validation.reasonableness import ReasonablenessCheck


class TestZScoreGate:
    """Tests for ZScoreGate anomaly detection."""

    def setup_method(self):
        self.gate = ZScoreGate(
            sigma_threshold=2.5,
            block_threshold=4.0,
            min_observations=30,
        )
        np.random.seed(42)
        self.historical = np.random.normal(100, 10, 100).tolist()

    def test_normal_values_pass(self):
        result = self.gate.validate(
            values=[100.0, 105.0, 95.0],
            historical_values=self.historical,
        )
        assert result.gate_decision == GateDecision.PASS
        assert result.passed == 3
        assert result.flagged == 0
        assert result.blocked == 0

    def test_extreme_value_flagged(self):
        result = self.gate.validate(
            values=[100.0, 135.0],  # 135 is ~3.5 sigma from mean ~100
            historical_values=self.historical,
        )
        assert result.flagged >= 1 or result.blocked >= 1

    def test_very_extreme_value_blocked(self):
        result = self.gate.validate(
            values=[100.0, 200.0],  # 200 is ~10 sigma
            historical_values=self.historical,
        )
        assert result.blocked >= 1
        assert result.gate_decision == GateDecision.BLOCK

    def test_insufficient_history_passes(self):
        result = self.gate.validate(
            values=[100.0, 500.0],
            historical_values=[100.0] * 5,  # Only 5, need 30
        )
        assert result.gate_decision == GateDecision.PASS
        assert result.passed == 2

    def test_batch_anomaly_rate_blocks(self):
        # More than 10% anomalies should trigger batch BLOCK
        values = [100.0] * 8 + [500.0, 600.0]  # 20% anomaly rate
        result = self.gate.validate(
            values=values,
            historical_values=self.historical,
        )
        assert result.gate_decision == GateDecision.BLOCK

    def test_segmented_validation(self):
        seg_hist = {
            "revenue": np.random.normal(1000, 100, 50).tolist(),
            "cost": np.random.normal(500, 50, 50).tolist(),
        }
        result = self.gate.validate_segmented(
            values=[1050.0, 520.0],
            segments=["revenue", "cost"],
            historical_values=seg_hist,
        )
        assert result.gate_decision == GateDecision.PASS

    def test_zero_std_handling(self):
        # All same values -> std = 0
        result = self.gate.validate(
            values=[100.0, 100.0, 101.0],
            historical_values=[100.0] * 50,
        )
        # 101.0 should be flagged/blocked since std=0 and it differs
        assert result.flagged + result.blocked >= 1

    def test_record_ids_preserved(self):
        result = self.gate.validate(
            values=[100.0, 500.0],
            historical_values=self.historical,
            record_ids=["MY-001", "MY-002"],
        )
        if result.anomalies:
            assert result.anomalies[0].record_id == "MY-002"

    def test_invalid_params_raise(self):
        with pytest.raises(ValueError):
            ZScoreGate(sigma_threshold=-1)
        with pytest.raises(ValueError):
            ZScoreGate(sigma_threshold=5, block_threshold=3)
        with pytest.raises(ValueError):
            ZScoreGate(window_days=0)
        with pytest.raises(ValueError):
            ZScoreGate(min_observations=0)


class TestReconciliationCheck:
    """Tests for deterministic reconciliation checks."""

    def setup_method(self):
        self.checker = ReconciliationCheck(tolerance=0.01)

    def test_count_match(self):
        result = self.checker.check_count(100, 100)
        assert result.status == CheckStatus.PASS

    def test_count_mismatch(self):
        result = self.checker.check_count(100, 99)
        assert result.status == CheckStatus.FAIL

    def test_amount_within_tolerance(self):
        result = self.checker.check_amount(10000.0, 10050.0)
        assert result.status == CheckStatus.PASS  # 0.5% < 1%

    def test_amount_beyond_tolerance(self):
        result = self.checker.check_amount(10000.0, 10200.0)
        assert result.status == CheckStatus.FAIL  # 2% > 1%

    def test_completeness_all_present(self):
        result = self.checker.check_completeness(
            {"a", "b", "c"}, {"a", "b", "c", "d"}
        )
        assert result.status == CheckStatus.PASS

    def test_completeness_missing(self):
        result = self.checker.check_completeness(
            {"a", "b", "c"}, {"a", "b"}
        )
        assert result.status == CheckStatus.FAIL


class TestReasonablenessCheck:
    """Tests for period-over-period variance."""

    def setup_method(self):
        self.checker = ReasonablenessCheck(variance_threshold=0.15)

    def test_within_threshold(self):
        result = self.checker.check_period_variance(1100.0, 1000.0)
        assert result.status == CheckStatus.PASS  # 10% < 15%

    def test_exceeds_threshold(self):
        result = self.checker.check_period_variance(1200.0, 1000.0)
        assert result.status == CheckStatus.FAIL  # 20% > 15%
