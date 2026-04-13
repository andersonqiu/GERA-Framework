"""
Reasonableness Checks

Period-over-period variance analysis for detecting unexpected
shifts in financial metrics. Complements Z-Score statistical
validation with business-logic-level sanity checks.
"""

from typing import List

from gera.validation.reconciliation_checks import CheckStatus, ReconciliationResult


class ReasonablenessCheck:
    """
    Period-over-period variance analysis.

    Args:
        variance_threshold: Maximum acceptable period-over-period
            variance as a fraction (default: 0.15 = 15%)
    """

    def __init__(self, variance_threshold: float = 0.15):
        self.variance_threshold = variance_threshold

    def check_period_variance(
        self,
        current_value: float,
        prior_value: float,
        metric_name: str = "metric",
    ) -> ReconciliationResult:
        """Check if current period deviates from prior period."""
        if prior_value == 0:
            pct_change = 0.0 if current_value == 0 else float('inf')
        else:
            pct_change = abs(current_value - prior_value) / abs(prior_value)

        if pct_change <= self.variance_threshold:
            return ReconciliationResult(
                check_name=f"period_variance_{metric_name}",
                status=CheckStatus.PASS,
                message=(
                    f"{metric_name} variance {pct_change:.2%} within "
                    f"{self.variance_threshold:.0%} threshold"
                ),
                source_value=prior_value,
                target_value=current_value,
                difference=current_value - prior_value,
                tolerance_used=self.variance_threshold,
            )
        return ReconciliationResult(
            check_name=f"period_variance_{metric_name}",
            status=CheckStatus.FAIL,
            message=(
                f"{metric_name} variance {pct_change:.2%} exceeds "
                f"{self.variance_threshold:.0%} threshold"
            ),
            source_value=prior_value,
            target_value=current_value,
            difference=current_value - prior_value,
            tolerance_used=self.variance_threshold,
        )

    def check_against_historical(
        self,
        current_value: float,
        historical_values: List[float],
        metric_name: str = "metric",
    ) -> ReconciliationResult:
        """Check current value against historical average."""
        if not historical_values:
            return ReconciliationResult(
                check_name=f"historical_variance_{metric_name}",
                status=CheckStatus.PASS,
                message="No historical data for comparison",
            )
        avg = sum(historical_values) / len(historical_values)
        return self.check_period_variance(current_value, avg, metric_name)
