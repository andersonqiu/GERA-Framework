"""
Z-Score Anomaly Detection Gate

Implements inline pipeline gates using modified Z-Score analysis for
detecting statistical anomalies in financial data streams. Designed
for SOX Section 404 compliance in regulated enterprises.

Key features:
- Configurable sigma thresholds (default: 2.5 FLAG, 4.0 BLOCK)
- Rolling baseline windows (default: 90 days)
- Minimum observation requirements to prevent false positives
- Batch anomaly rate limiting
- Per-segment baseline calibration

References:
- SOX Section 404: Internal control over financial reporting
- PCAOB AS 2201: Audit of internal control
- Qiu, Z. (2026). "Data Engineering Patterns for Cross-System
  Reconciliation in Regulated Enterprises." TechRxiv.
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

import numpy as np


class GateDecision(Enum):
    """Pipeline gate decision outcomes."""
    PASS = "pass"
    FLAG = "flag"
    BLOCK = "block"


@dataclass
class Anomaly:
    """Individual anomaly detection result."""
    record_id: str
    value: float
    z_score: float
    decision: GateDecision
    baseline_mean: float
    baseline_std: float
    segment: Optional[str] = None
    timestamp: Optional[datetime] = None

    @property
    def deviation_pct(self) -> float:
        """Percentage deviation from baseline mean."""
        if self.baseline_mean == 0:
            return float('inf') if self.value != 0 else 0.0
        return abs(self.value - self.baseline_mean) / abs(self.baseline_mean) * 100


@dataclass
class ZScoreResult:
    """Batch validation result from Z-Score gate."""
    total_records: int
    passed: int
    flagged: int
    blocked: int
    anomalies: List[Anomaly]
    gate_decision: GateDecision
    batch_anomaly_rate: float
    baseline_mean: float
    baseline_std: float
    evaluation_timestamp: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    @property
    def pass_rate(self) -> float:
        if self.total_records == 0:
            return 0.0
        return self.passed / self.total_records


class ZScoreGate:
    """
    Statistical anomaly detection gate for financial data pipelines.

    Uses (modified) Z-Score analysis with configurable thresholds to
    classify records as PASS, FLAG, or BLOCK. Supports rolling baseline
    windows, per-segment calibration, and two scoring methods:

    * ``method="zscore"`` (default) — classic (x − mean) / std scoring.
      Sensitive to extreme values in the baseline itself.
    * ``method="mad"`` — modified Z-Score per Iglewicz & Hoaglin (1993):
      ``0.6745 * (x − median) / MAD``. More robust when the historical
      baseline contains outliers (e.g., a prior-period fraud event
      contaminating the training window).

    The MAD option is useful for long-tail financial distributions where
    a handful of historical outliers would otherwise inflate std and
    mask new anomalies.

    Args:
        sigma_threshold: Threshold for FLAG (default: 2.5)
        block_threshold: Threshold for BLOCK (default: 4.0)
        window_days: Rolling baseline window in days (default: 90)
        min_observations: Minimum records for valid baseline (default: 30)
        batch_anomaly_rate_limit: Max anomaly rate before batch BLOCK
            (default: 0.10), must be strictly between 0 and 1.
        method: Scoring method, ``"zscore"`` or ``"mad"`` (default: ``"zscore"``).
    """

    _MAD_SCALE = 0.6745  # Iglewicz & Hoaglin constant: MAD → robust σ̂.

    def __init__(
        self,
        sigma_threshold: float = 2.5,
        block_threshold: float = 4.0,
        window_days: int = 90,
        min_observations: int = 30,
        batch_anomaly_rate_limit: float = 0.10,
        method: str = "zscore",
    ):
        if sigma_threshold <= 0:
            raise ValueError("sigma_threshold must be positive")
        if block_threshold <= sigma_threshold:
            raise ValueError("block_threshold must exceed sigma_threshold")
        if window_days <= 0:
            raise ValueError("window_days must be positive")
        if min_observations <= 0:
            raise ValueError("min_observations must be positive")
        if not (0 < batch_anomaly_rate_limit < 1):
            raise ValueError(
                "batch_anomaly_rate_limit must be strictly between 0 and 1, "
                f"got {batch_anomaly_rate_limit}"
            )
        if method not in ("zscore", "mad"):
            raise ValueError(
                f"method must be 'zscore' or 'mad', got {method!r}"
            )

        self.sigma_threshold = sigma_threshold
        self.block_threshold = block_threshold
        self.window_days = window_days
        self.min_observations = min_observations
        self.batch_anomaly_rate_limit = batch_anomaly_rate_limit
        self.method = method

    def compute_baseline(
        self,
        historical_values: List[float],
        timestamps: Optional[List[datetime]] = None,
    ) -> Tuple[float, float, bool]:
        """
        Compute baseline statistics from historical data.

        For ``method="zscore"`` returns ``(mean, std, is_valid)``.
        For ``method="mad"`` returns ``(median, MAD, is_valid)`` where
        MAD is the median absolute deviation; ``evaluate_record`` scales
        by 0.6745 to obtain a modified Z-Score comparable to the sigma
        thresholds.

        Returns:
            Tuple of (center, scale, is_valid) where is_valid indicates
            whether sufficient observations exist for a reliable baseline.
        """
        if timestamps is not None:
            if len(timestamps) != len(historical_values):
                raise ValueError(
                    "timestamps length must match historical_values length "
                    f"({len(timestamps)} != {len(historical_values)})"
                )
            cutoff = datetime.now(timezone.utc) - timedelta(days=self.window_days)
            filtered = [
                v for v, t in zip(historical_values, timestamps)
                if t >= cutoff
            ]
        else:
            filtered = list(historical_values)

        if len(filtered) < self.min_observations:
            return 0.0, 0.0, False

        arr = np.array(filtered, dtype=np.float64)
        if self.method == "mad":
            median = float(np.median(arr))
            mad = float(np.median(np.abs(arr - median)))
            return median, mad, True
        return float(np.mean(arr)), float(np.std(arr, ddof=1)), True

    def evaluate_record(
        self,
        value: float,
        baseline_mean: float,
        baseline_std: float,
        record_id: str = "",
        segment: Optional[str] = None,
    ) -> Anomaly:
        """
        Evaluate a single record against the baseline.

        The ``baseline_mean`` / ``baseline_std`` parameter names are
        retained for backwards compatibility — under ``method="mad"``
        the two arguments carry ``(median, MAD)`` instead.
        """
        if baseline_std == 0:
            z_score = 0.0 if value == baseline_mean else float('inf')
        elif self.method == "mad":
            # Modified Z-Score (Iglewicz & Hoaglin, 1993): the 0.6745
            # constant scales MAD to an σ̂ comparable to a Gaussian std,
            # so the same sigma_threshold / block_threshold apply.
            z_score = abs(self._MAD_SCALE * (value - baseline_mean) / baseline_std)
        else:
            z_score = abs(value - baseline_mean) / baseline_std

        if z_score >= self.block_threshold:
            decision = GateDecision.BLOCK
        elif z_score >= self.sigma_threshold:
            decision = GateDecision.FLAG
        else:
            decision = GateDecision.PASS

        return Anomaly(
            record_id=record_id,
            value=value,
            z_score=round(z_score, 2) if z_score != float('inf') else z_score,
            decision=decision,
            baseline_mean=round(baseline_mean, 2),
            baseline_std=round(baseline_std, 2),
            segment=segment,
            timestamp=datetime.now(timezone.utc),
        )

    def validate(
        self,
        values: List[float],
        historical_values: List[float],
        record_ids: Optional[List[str]] = None,
        timestamps: Optional[List[datetime]] = None,
    ) -> ZScoreResult:
        """
        Validate a batch of values against historical baseline.

        If historical data is insufficient (< min_observations), all records
        are flagged for manual review rather than passed silently.  Passing
        unvalidated records without any signal is a compliance risk —
        FLAG ensures the batch is reviewed before downstream processing.
        """
        mean, std, is_valid = self.compute_baseline(historical_values, timestamps)

        if record_ids is None:
            record_ids = [f"record-{i}" for i in range(len(values))]
        elif len(record_ids) != len(values):
            raise ValueError(
                "record_ids length must match values length "
                f"({len(record_ids)} != {len(values)})"
            )

        if not is_valid:
            flagged_anomalies = [
                Anomaly(
                    record_id=rid,
                    value=val,
                    z_score=0.0,
                    decision=GateDecision.FLAG,
                    baseline_mean=0.0,
                    baseline_std=0.0,
                )
                for val, rid in zip(values, record_ids)
            ]
            return ZScoreResult(
                total_records=len(values),
                passed=0,
                flagged=len(values),
                blocked=0,
                anomalies=flagged_anomalies,
                gate_decision=GateDecision.FLAG,
                batch_anomaly_rate=1.0,
                baseline_mean=mean,
                baseline_std=std,
            )

        anomalies = []
        passed = flagged = blocked = 0

        for val, rid in zip(values, record_ids):
            result = self.evaluate_record(val, mean, std, rid)
            if result.decision == GateDecision.BLOCK:
                blocked += 1
                anomalies.append(result)
            elif result.decision == GateDecision.FLAG:
                flagged += 1
                anomalies.append(result)
            else:
                passed += 1

        total = len(values)
        anomaly_rate = (flagged + blocked) / total if total > 0 else 0.0

        if blocked > 0:
            gate_decision = GateDecision.BLOCK
        elif anomaly_rate > self.batch_anomaly_rate_limit:
            gate_decision = GateDecision.BLOCK
        elif flagged > 0:
            gate_decision = GateDecision.FLAG
        else:
            gate_decision = GateDecision.PASS

        return ZScoreResult(
            total_records=total,
            passed=passed,
            flagged=flagged,
            blocked=blocked,
            anomalies=anomalies,
            gate_decision=gate_decision,
            batch_anomaly_rate=round(anomaly_rate, 4),
            baseline_mean=mean,
            baseline_std=std,
        )

    def validate_segmented(
        self,
        values: List[float],
        segments: List[str],
        historical_values: Dict[str, List[float]],
        record_ids: Optional[List[str]] = None,
    ) -> ZScoreResult:
        """
        Validate with per-segment baseline calibration.

        Different business segments may have different normal ranges.
        This method computes separate baselines per segment.

        Fail-closed on insufficient baseline: when a segment lacks
        :attr:`min_observations` historical values, the record cannot be
        statistically evaluated, so it is FLAGged for manual review
        rather than PASSed silently. Passing an unvalidated record
        without any signal is a compliance risk in regulated pipelines
        (a new segment, a data-pipeline outage, or a cold-start
        scenario can otherwise slip through unnoticed).
        """
        if record_ids is None:
            record_ids = [f"record-{i}" for i in range(len(values))]
        elif len(record_ids) != len(values):
            raise ValueError(
                "record_ids length must match values length "
                f"({len(record_ids)} != {len(values)})"
            )
        if len(segments) != len(values):
            raise ValueError(
                "segments length must match values length "
                f"({len(segments)} != {len(values)})"
            )

        all_anomalies = []
        passed = flagged = blocked = 0

        for val, seg, rid in zip(values, segments, record_ids):
            hist = historical_values.get(seg, [])
            mean, std, is_valid = self.compute_baseline(hist)

            if not is_valid:
                # Insufficient baseline — FLAG for manual review.
                # The Anomaly carries z_score=0.0 and baseline_*=0.0 as
                # sentinels that the evaluation was baseline-less.
                flagged += 1
                all_anomalies.append(Anomaly(
                    record_id=rid,
                    value=val,
                    z_score=0.0,
                    decision=GateDecision.FLAG,
                    baseline_mean=0.0,
                    baseline_std=0.0,
                    segment=seg,
                    timestamp=datetime.now(timezone.utc),
                ))
                continue

            result = self.evaluate_record(val, mean, std, rid, segment=seg)
            if result.decision == GateDecision.BLOCK:
                blocked += 1
                all_anomalies.append(result)
            elif result.decision == GateDecision.FLAG:
                flagged += 1
                all_anomalies.append(result)
            else:
                passed += 1

        total = len(values)
        anomaly_rate = (flagged + blocked) / total if total > 0 else 0.0

        if blocked > 0:
            gate_decision = GateDecision.BLOCK
        elif anomaly_rate > self.batch_anomaly_rate_limit:
            gate_decision = GateDecision.BLOCK
        elif flagged > 0:
            gate_decision = GateDecision.FLAG
        else:
            gate_decision = GateDecision.PASS

        return ZScoreResult(
            total_records=total,
            passed=passed,
            flagged=flagged,
            blocked=blocked,
            anomalies=all_anomalies,
            gate_decision=gate_decision,
            batch_anomaly_rate=round(anomaly_rate, 4),
            baseline_mean=0.0,
            baseline_std=0.0,
        )
