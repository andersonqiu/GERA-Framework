#!/usr/bin/env python3
"""
GERA Framework — Multi-Source (Three-Way) Reconciliation Example

Demonstrates a realistic enterprise pattern: reconciling transactions
across three independent systems of record.

Scenario
--------
A financial services firm records transactions in three places:

    Source A — General Ledger (GL)      authoritative accounting record
    Source B — Transaction Processor    operational event stream
    Source C — Data Warehouse           downstream analytical copy

Each pair (A⇄B, A⇄C, B⇄C) must reconcile to tie out the books. A single
missing or mutated record in any one system can produce *conflicting*
discrepancy reports depending on which pair you look at — the job of
this example is to show how GERA detects, classifies, and audits these
three-way exceptions.

Pattern
-------
1. Generate three variants of the same underlying transaction universe
   with different defects seeded into each system.
2. Run pair-wise reconciliation (A⇄B, A⇄C, B⇄C) and collect match
   reports.
3. Classify every discrepancy into one of:
       LEDGER_TRUTH_MISSING_DOWNSTREAM
       PROCESSOR_TRUTH_MISSING_LEDGER
       WAREHOUSE_DRIFT
4. Route each classified exception to the FIFO queue with an appropriate
   severity.
5. Record the full Z-Score gate decision for the ledger amount stream
   against a 90-day baseline.
6. Verify the append-only audit chain remains intact after the run.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from typing import Dict, List, Set, Tuple

import numpy as np

from gera.governance import AuditLogger, EventType
from gera.reconciliation import (
    DeterministicMatcher,
    ExceptionRouter,
    ExceptionSeverity,
    MatchStatus,
)
from gera.validation import ZScoreGate


# ---------------------------------------------------------------------------
# 1. Synthetic three-source dataset with seeded defects
# ---------------------------------------------------------------------------

RNG = np.random.default_rng(17)
random.seed(17)


def _base_universe(n: int = 500) -> List[Dict]:
    """Generate the true ledger universe."""
    amounts = RNG.normal(2_500, 600, n).round(2)
    return [
        {
            "txn_id": f"TXN-{i:06d}",
            "amount": float(amounts[i]),
            "dept": RNG.choice(["finance", "ops", "treasury"]),
        }
        for i in range(n)
    ]


def _make_three_sources(base: List[Dict]) -> Tuple[List[Dict], List[Dict], List[Dict]]:
    """
    Derive three system-of-record views of the same universe with
    realistic defects seeded into each.

    Defects injected:
        GL (A):          perfectly clean (authoritative)
        Processor (B):   2 drops (2 never processed by B)
        Warehouse (C):   1 drop + 3 amount drifts (+1 cent each, rounding bug)
    """
    gl = [dict(r) for r in base]

    processor = [dict(r) for r in base]
    del processor[100]   # never processed
    del processor[201]   # never processed (index shifts but that is fine)

    warehouse = [dict(r) for r in base]
    # Drift 3 amounts by $0.01 (silent rounding issue)
    for i in (50, 150, 300):
        warehouse[i]["amount"] = round(warehouse[i]["amount"] + 0.01, 2)
    del warehouse[400]   # missed by overnight ETL

    return gl, processor, warehouse


# ---------------------------------------------------------------------------
# 2. Pair-wise reconciliation
# ---------------------------------------------------------------------------

@dataclass
class PairDiff:
    """Discrepancies between one ordered pair of sources."""
    left_name: str
    right_name: str
    only_in_left: Set[str]
    only_in_right: Set[str]
    conflicts: List[Tuple[str, float, float]]  # (txn_id, left_amt, right_amt)


def _diff_pair(
    left_name: str,
    left: List[Dict],
    right_name: str,
    right: List[Dict],
) -> PairDiff:
    matcher = DeterministicMatcher(
        key_fields=["txn_id"],
        value_fields=["amount"],
    )
    report = matcher.match(left, right)

    only_left: Set[str] = set()
    only_right: Set[str] = set()
    conflicts: List[Tuple[str, float, float]] = []

    for res in report.results:
        if res.status == MatchStatus.UNMATCHED_SOURCE:
            only_left.add(res.source_record["txn_id"])
        elif res.status == MatchStatus.UNMATCHED_TARGET:
            only_right.add(res.target_record["txn_id"])
        elif res.status == MatchStatus.CONFLICT:
            for field_name, lv, rv in res.conflicts:
                if field_name == "amount":
                    conflicts.append(
                        (res.source_record["txn_id"], float(lv), float(rv))
                    )

    return PairDiff(
        left_name=left_name,
        right_name=right_name,
        only_in_left=only_left,
        only_in_right=only_right,
        conflicts=conflicts,
    )


# ---------------------------------------------------------------------------
# 3. Three-way classification
# ---------------------------------------------------------------------------

@dataclass
class ThreeWayException:
    txn_id: str
    category: str
    evidence: str
    severity: ExceptionSeverity


def _classify_three_way(
    gl_proc: PairDiff,
    gl_wh: PairDiff,
    proc_wh: PairDiff,
) -> List[ThreeWayException]:
    """
    Combine the three pair-wise diffs into a single exception list.

    Classification rules (priority order):
        1. TXN in GL but missing from BOTH processor and warehouse
           → LEDGER_ONLY_TRUTH (severity HIGH) — likely a stale GL entry.
        2. TXN in GL and processor but missing from warehouse
           → WAREHOUSE_MISSING (severity MEDIUM) — downstream ETL gap.
        3. TXN in GL and warehouse but missing from processor
           → PROCESSOR_MISSING (severity HIGH) — operational miss.
        4. Amount conflict anywhere involving warehouse only
           → WAREHOUSE_DRIFT (severity LOW) — likely rounding.
        5. Amount conflict involving processor
           → PROCESSOR_DRIFT (severity HIGH) — material.
    """
    issues: List[ThreeWayException] = []

    missing_from_proc = gl_proc.only_in_left       # in GL, not in processor
    missing_from_wh = gl_wh.only_in_left           # in GL, not in warehouse

    for txn in missing_from_proc & missing_from_wh:
        issues.append(ThreeWayException(
            txn_id=txn,
            category="LEDGER_ONLY_TRUTH",
            evidence="present in GL but absent from processor AND warehouse",
            severity=ExceptionSeverity.HIGH,
        ))

    for txn in missing_from_wh - missing_from_proc:
        issues.append(ThreeWayException(
            txn_id=txn,
            category="WAREHOUSE_MISSING",
            evidence="present in GL + processor but missing from warehouse",
            severity=ExceptionSeverity.MEDIUM,
        ))

    for txn in missing_from_proc - missing_from_wh:
        issues.append(ThreeWayException(
            txn_id=txn,
            category="PROCESSOR_MISSING",
            evidence="present in GL + warehouse but missing from processor",
            severity=ExceptionSeverity.HIGH,
        ))

    for txn, gl_amt, wh_amt in gl_wh.conflicts:
        issues.append(ThreeWayException(
            txn_id=txn,
            category="WAREHOUSE_DRIFT",
            evidence=f"GL={gl_amt:.2f} vs WH={wh_amt:.2f} (Δ={wh_amt - gl_amt:+.2f})",
            severity=ExceptionSeverity.LOW,
        ))

    for txn, gl_amt, proc_amt in gl_proc.conflicts:
        issues.append(ThreeWayException(
            txn_id=txn,
            category="PROCESSOR_DRIFT",
            evidence=f"GL={gl_amt:.2f} vs PROC={proc_amt:.2f} (Δ={proc_amt - gl_amt:+.2f})",
            severity=ExceptionSeverity.HIGH,
        ))

    return issues


# ---------------------------------------------------------------------------
# 4. Main workflow
# ---------------------------------------------------------------------------

def main() -> None:
    print("═" * 72)
    print("GERA Framework — Multi-Source (3-Way) Reconciliation")
    print("═" * 72)

    base = _base_universe(n=500)
    gl, processor, warehouse = _make_three_sources(base)
    print(f"  GL records:        {len(gl):>6}")
    print(f"  Processor records: {len(processor):>6}")
    print(f"  Warehouse records: {len(warehouse):>6}")

    # Pair-wise matching
    print("\n── Pair-wise reconciliation ───────────────────────────────────────")
    gl_proc = _diff_pair("GL", gl, "PROC", processor)
    gl_wh = _diff_pair("GL", gl, "WH", warehouse)
    proc_wh = _diff_pair("PROC", processor, "WH", warehouse)

    for pair in (gl_proc, gl_wh, proc_wh):
        print(
            f"  {pair.left_name} ⇄ {pair.right_name}: "
            f"only_{pair.left_name}={len(pair.only_in_left)}, "
            f"only_{pair.right_name}={len(pair.only_in_right)}, "
            f"conflicts={len(pair.conflicts)}"
        )

    # Three-way classification
    print("\n── 3-way classification ───────────────────────────────────────────")
    issues = _classify_three_way(gl_proc, gl_wh, proc_wh)
    category_counts: Dict[str, int] = {}
    for issue in issues:
        category_counts[issue.category] = category_counts.get(issue.category, 0) + 1
    for cat, count in sorted(category_counts.items()):
        print(f"  {cat:<22} {count:>3}")

    # Route to exception queue
    print("\n── Exception routing ──────────────────────────────────────────────")
    router = ExceptionRouter()
    for issue in issues:
        router.route(
            source=issue.txn_id,
            description=f"{issue.category}: {issue.evidence}",
            severity=issue.severity,
        )
    summary = router.get_queue_summary()
    print(f"  Total exceptions:  {summary['total']}")
    print(f"  By severity:       {summary['by_severity']}")

    # Statistical gate on the ledger (authoritative) amount stream
    print("\n── Layer 2: Z-Score gate on GL amounts ────────────────────────────")
    historical = RNG.normal(2_500, 600, 2_000).tolist()
    gate = ZScoreGate(sigma_threshold=2.5, block_threshold=5.0)
    gate_result = gate.validate(
        values=[r["amount"] for r in gl],
        historical_values=historical,
        record_ids=[r["txn_id"] for r in gl],
    )
    print(f"  Gate decision:     {gate_result.gate_decision.value.upper()}")
    print(f"  Passed / Flag / Block: "
          f"{gate_result.passed} / {gate_result.flagged} / {gate_result.blocked}")
    print(f"  Batch anomaly rate: {gate_result.batch_anomaly_rate:.2%}")

    # Audit chain
    print("\n── Layer 3: Audit chain ───────────────────────────────────────────")
    logger = AuditLogger(retention_days=2555)
    for pair in (gl_proc, gl_wh, proc_wh):
        logger.log(
            event_type=EventType.RECONCILIATION,
            actor="gera_pipeline",
            action=f"pair_{pair.left_name}_{pair.right_name}",
            resource="multi_source_reconciliation",
            details={
                "only_left": len(pair.only_in_left),
                "only_right": len(pair.only_in_right),
                "conflicts": len(pair.conflicts),
            },
        )
    for issue in issues:
        logger.log(
            event_type=EventType.EXCEPTION_CREATED,
            actor="gera_pipeline",
            action="three_way_exception",
            resource=issue.txn_id,
            details={"category": issue.category, "severity": issue.severity.value},
        )
    logger.log_gate_decision(
        gate_name="zscore_gl_amounts",
        decision=gate_result.gate_decision.value,
        details={
            "blocked": gate_result.blocked,
            "batch_anomaly_rate": gate_result.batch_anomaly_rate,
        },
    )
    print(f"  Events logged:     {logger.event_count}")
    print(f"  Chain integrity:   {'VALID' if logger.verify_chain() else 'BROKEN'}")

    # Summary
    print("\n" + "═" * 72)
    print("SUMMARY")
    print("═" * 72)
    print(f"  3-way exceptions:  {len(issues)}")
    print(f"  Audit events:      {logger.event_count}")
    print(f"  Gate decision:     {gate_result.gate_decision.value.upper()}")
    print(f"  Chain integrity:   {'VALID' if logger.verify_chain() else 'BROKEN'}")
    print("═" * 72)


if __name__ == "__main__":
    main()
