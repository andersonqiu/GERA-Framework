#!/usr/bin/env python3
"""
GERA Framework — Basic Reconciliation Example

Demonstrates end-to-end reconciliation workflow:
1. Layer 1: Deterministic cross-system matching
2. Layer 1: Reconciliation checks (count, amount, completeness)
3. Layer 2: Z-Score anomaly detection
4. Audit: Hash-chained event logging
"""

from gera.reconciliation import DeterministicMatcher
from gera.validation import ZScoreGate, ReconciliationCheck
from gera.governance import AuditLogger

# ─── Sample Data ───────────────────────────────────────────

source_records = [
    {"txn_id": "TXN-001", "amount": 1250.00, "department": "finance"},
    {"txn_id": "TXN-002", "amount": 8750.00, "department": "ops"},
    {"txn_id": "TXN-003", "amount": 3100.00, "department": "finance"},
    {"txn_id": "TXN-004", "amount": 4200.00, "department": "ops"},
    {"txn_id": "TXN-005", "amount": 67890.25, "department": "finance"},
]

target_records = [
    {"txn_id": "TXN-001", "amount": 1250.00, "department": "finance"},
    {"txn_id": "TXN-002", "amount": 8750.00, "department": "ops"},
    {"txn_id": "TXN-003", "amount": 3100.00, "department": "finance"},
    {"txn_id": "TXN-004", "amount": 4200.00, "department": "ops"},
    # TXN-005 missing from target — simulates a real exception
]

# Historical baseline (~90 days of normal transactions)
import numpy as np
np.random.seed(42)
historical_amounts = np.random.normal(loc=2000, scale=800, size=100).tolist()

# ─── Layer 1: Deterministic Matching ──────────────────────

print("\u2550" * 60)
print("LAYER 1: DETERMINISTIC RECONCILIATION")
print("\u2550" * 60)

matcher = DeterministicMatcher(
    key_fields=["txn_id"],
    value_fields=["amount"],
)
match_report = matcher.match(source_records, target_records)

print(f"Source records:    {len(source_records)}")
print(f"Target records:    {len(target_records)}")
print(f"Matched:           {match_report.matched_count}")
print(f"Unmatched (source): {match_report.unmatched_source_count}")
print(f"Match rate:        {match_report.match_rate:.1%}")
print(f"Fully reconciled:  {match_report.is_fully_reconciled}")

# ─── Layer 1: Reconciliation Checks ──────────────────────

print(f"\n{'═' * 60}")
print("LAYER 1: RECONCILIATION CHECKS")
print("═" * 60)

checker = ReconciliationCheck(tolerance=0.01)
report = checker.run_all(
    source_count=len(source_records),
    target_count=len(target_records),
    source_amount=sum(r["amount"] for r in source_records),
    target_amount=sum(r["amount"] for r in target_records),
    source_keys={r["txn_id"] for r in source_records},
    target_keys={r["txn_id"] for r in target_records},
)

for r in report.results:
    symbol = "\u2713" if r.status.value == "PASS" else "\u2717"
    print(f"  {symbol} {r.check_name}: {r.status.value} {r.message}")

print(f"\nOverall: {report.overall_status.value}")

# ─── Layer 2: Z-Score Anomaly Detection ──────────────────

print(f"\n{'═' * 60}")
print("LAYER 2: Z-SCORE ANOMALY DETECTION")
print("═" * 60)

gate = ZScoreGate(sigma_threshold=2.5, block_threshold=4.0)
amounts = [r["amount"] for r in source_records]
record_ids = [r["txn_id"] for r in source_records]

result = gate.validate(
    values=amounts,
    historical_values=historical_amounts,
    record_ids=record_ids,
)

print(f"Sigma threshold:  {gate.sigma_threshold}")
print(f"Total records:    {result.total_records}")
print(f"Passed:           {result.passed}")
print(f"Flagged:          {result.flagged}")
print(f"Blocked:          {result.blocked}")
print(f"Gate decision:    {result.gate_decision.value.upper()}")

if result.anomalies:
    print(f"\nAnomalies detected:")
    for a in result.anomalies:
        print(
            f"  {a.record_id}: value={a.value:,.2f}, "
            f"z={a.z_score:.2f}, decision={a.decision.value.upper()}"
        )

# ─── Audit Logging ────────────────────────────────────────

print(f"\n{'═' * 60}")
print("AUDIT LOG")
print("═" * 60)

logger = AuditLogger(retention_days=2555)

# Log reconciliation result
logger.log_gate_decision(
    gate_name="deterministic_reconciliation",
    decision="fail" if not match_report.is_fully_reconciled else "pass",
    details={"match_rate": match_report.match_rate},
)

# Log Z-Score result
logger.log_gate_decision(
    gate_name="zscore_anomaly_detection",
    decision=result.gate_decision.value,
    details={
        "anomaly_rate": result.batch_anomaly_rate,
        "blocked_count": result.blocked,
    },
)

print(f"Events logged:     {logger.event_count}")
print(f"Chain integrity:   {'VALID' if logger.verify_chain() else 'BROKEN'}")
print(f"Retention:         {logger.retention_days} days (~7 years)")

for event in logger.query():
    print(f"  [{event.event_id}] {event.event_type.value.upper()} — {event.action} on {event.resource}")
