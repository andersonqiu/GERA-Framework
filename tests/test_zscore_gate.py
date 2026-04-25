"""Tests for Layer 2: Validation modules."""

import pytest
import numpy as np

from gera.validation.zscore_gate import ZScoreGate, GateDecision
from gera.validation.reconciliation_checks import ReconciliationCheck, CheckStatus
from gera.validation.reasonableness import ReasonablenessCheck


# ---------------------------------------------------------------------------
# Deterministic baseline shared across Z-Score tests.
# mean ≈ 100, std ≈ 10 — derived from a fixed seed so tests never flake.
# ---------------------------------------------------------------------------
_RNG = np.random.default_rng(42)
_HISTORICAL = _RNG.normal(100, 10, 100).tolist()
# Actual stats (pre-computed to avoid recomputing in every test):
_HIST_ARR = np.array(_HISTORICAL)
_HIST_MEAN = float(np.mean(_HIST_ARR))
_HIST_STD = float(np.std(_HIST_ARR, ddof=1))


class TestZScoreGate:
    """Tests for ZScoreGate anomaly detection."""

    def setup_method(self):
        self.gate = ZScoreGate(
            sigma_threshold=2.5,
            block_threshold=4.0,
            min_observations=30,
        )

    def test_normal_values_pass(self):
        result = self.gate.validate(
            values=[_HIST_MEAN, _HIST_MEAN + 0.5 * _HIST_STD],
            historical_values=_HISTORICAL,
        )
        assert result.gate_decision == GateDecision.PASS
        assert result.passed == 2
        assert result.flagged == 0
        assert result.blocked == 0

    def test_value_in_flag_zone_is_flagged(self):
        """
        A value at 3.0σ (between FLAG=2.5σ and BLOCK=4.0σ) must FLAG the individual
        record and produce a FLAG gate decision when the batch anomaly rate stays
        below the 10 % limit (1 flagged out of 20 = 5 %).
        """
        flag_value = _HIST_MEAN + 3.0 * _HIST_STD
        # 19 normal + 1 flag → 5 % anomaly rate, safely below 10 % batch limit.
        normal_values = [_HIST_MEAN] * 19
        result = self.gate.validate(
            values=normal_values + [flag_value],
            historical_values=_HISTORICAL,
        )
        assert result.flagged == 1
        assert result.blocked == 0
        assert result.gate_decision == GateDecision.FLAG

    def test_very_extreme_value_blocked(self):
        block_value = _HIST_MEAN + 10 * _HIST_STD
        result = self.gate.validate(
            values=[100.0, block_value],
            historical_values=_HISTORICAL,
        )
        assert result.blocked >= 1
        assert result.gate_decision == GateDecision.BLOCK

    def test_insufficient_history_flags_all_records(self):
        """
        When historical data is below min_observations, all records must be
        FLAG (not PASS).  Silently passing unvalidated financial data is a
        compliance risk — a FLAG ensures the batch requires manual review.
        """
        result = self.gate.validate(
            values=[100.0, 500.0],
            historical_values=[100.0] * 5,  # Only 5 observations, need 30.
        )
        assert result.gate_decision == GateDecision.FLAG
        assert result.flagged == 2
        assert result.passed == 0
        assert result.blocked == 0

    def test_batch_anomaly_rate_blocks(self):
        # More than 10 % anomalies should trigger batch BLOCK.
        values = [_HIST_MEAN] * 8 + [
            _HIST_MEAN + 5 * _HIST_STD,
            _HIST_MEAN + 6 * _HIST_STD,
        ]  # 20 % anomaly rate
        result = self.gate.validate(values=values, historical_values=_HISTORICAL)
        assert result.gate_decision == GateDecision.BLOCK

    def test_batch_anomaly_rate_limit_validation(self):
        """batch_anomaly_rate_limit must be strictly between 0 and 1."""
        with pytest.raises(ValueError, match="batch_anomaly_rate_limit"):
            ZScoreGate(batch_anomaly_rate_limit=0.0)
        with pytest.raises(ValueError, match="batch_anomaly_rate_limit"):
            ZScoreGate(batch_anomaly_rate_limit=1.0)
        with pytest.raises(ValueError, match="batch_anomaly_rate_limit"):
            ZScoreGate(batch_anomaly_rate_limit=999)

    def test_segmented_validation(self):
        rng = np.random.default_rng(7)
        seg_hist = {
            "revenue": rng.normal(1000, 100, 50).tolist(),
            "cost": rng.normal(500, 50, 50).tolist(),
        }
        result = self.gate.validate_segmented(
            values=[1050.0, 520.0],
            segments=["revenue", "cost"],
            historical_values=seg_hist,
        )
        assert result.gate_decision == GateDecision.PASS

    def test_zero_std_handling(self):
        """Constant historical values (std=0) should flag any deviation."""
        result = self.gate.validate(
            values=[100.0, 100.0, 101.0],
            historical_values=[100.0] * 50,
        )
        assert result.flagged + result.blocked >= 1

    def test_record_ids_preserved(self):
        block_value = _HIST_MEAN + 10 * _HIST_STD
        result = self.gate.validate(
            values=[_HIST_MEAN, block_value],
            historical_values=_HISTORICAL,
            record_ids=["MY-001", "MY-002"],
        )
        assert result.anomalies
        assert result.anomalies[0].record_id == "MY-002"

    def test_record_ids_length_mismatch_raises(self):
        with pytest.raises(ValueError, match="record_ids length"):
            self.gate.validate(
                values=[100.0, 101.0],
                historical_values=_HISTORICAL,
                record_ids=["REC-001"],
            )

    def test_segment_length_mismatch_raises(self):
        with pytest.raises(ValueError, match="segments length"):
            self.gate.validate_segmented(
                values=[100.0, 101.0],
                segments=["revenue"],
                historical_values={"revenue": _HISTORICAL},
            )

    def test_segmented_record_ids_length_mismatch_raises(self):
        with pytest.raises(ValueError, match="record_ids length"):
            self.gate.validate_segmented(
                values=[100.0, 101.0],
                segments=["revenue", "revenue"],
                historical_values={"revenue": _HISTORICAL},
                record_ids=["REC-001"],
            )

    def test_baseline_timestamp_length_mismatch_raises(self):
        with pytest.raises(ValueError, match="timestamps length"):
            self.gate.compute_baseline(
                historical_values=[100.0, 101.0],
                timestamps=[],
            )

    def test_invalid_params_raise(self):
        with pytest.raises(ValueError):
            ZScoreGate(sigma_threshold=-1)
        with pytest.raises(ValueError):
            ZScoreGate(sigma_threshold=5, block_threshold=3)
        with pytest.raises(ValueError):
            ZScoreGate(window_days=0)
        with pytest.raises(ValueError):
            ZScoreGate(min_observations=0)


# ---------------------------------------------------------------------------
# Layer 2 — ReconciliationCheck
# ---------------------------------------------------------------------------

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
        assert result.status == CheckStatus.PASS  # 0.5 % < 1 %

    def test_amount_beyond_tolerance(self):
        result = self.checker.check_amount(10000.0, 10200.0)
        assert result.status == CheckStatus.FAIL  # 2 % > 1 %

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


# ---------------------------------------------------------------------------
# Layer 2 — ReasonablenessCheck
# ---------------------------------------------------------------------------

class TestReasonablenessCheck:
    """Tests for period-over-period variance."""

    def setup_method(self):
        self.checker = ReasonablenessCheck(variance_threshold=0.15)

    def test_within_threshold(self):
        result = self.checker.check_period_variance(1100.0, 1000.0)
        assert result.status == CheckStatus.PASS  # 10 % < 15 %

    def test_exceeds_threshold(self):
        result = self.checker.check_period_variance(1200.0, 1000.0)
        assert result.status == CheckStatus.FAIL  # 20 % > 15 %
