#!/usr/bin/env python3
"""
GERA Framework — Performance Benchmark Suite

Measures latency and throughput of each GERA layer at enterprise-scale
record counts (1K / 10K / 100K / 1M).  Results inform pipeline sizing
and SLA budgeting for regulated reconciliation workloads.

The benchmark is deterministic (fixed RNG seed) so results are
reproducible run-to-run on the same hardware.  Absolute numbers vary
with CPU/memory; the shape of the curves (linear vs super-linear) is
what matters for capacity planning.

Usage:
    python -m benchmarks.benchmark_reconciliation
    python -m benchmarks.benchmark_reconciliation --scales 1000 10000
    python -m benchmarks.benchmark_reconciliation --json > results.json

Environment:
    - Reports OS, CPU count, Python version, NumPy version
    - Single-process, single-threaded (no parallelism)
    - Warms up each benchmark to avoid JIT / cache artefacts
"""

from __future__ import annotations

import argparse
import gc
import json
import platform
import statistics
import sys
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Callable, Dict, List, Tuple

import numpy as np

from gera.governance import (
    AuditLogger,
    DataSensitivity,
    MetricDefinition,
    SemanticRegistry,
)
from gera.reconciliation import (
    DeterministicMatcher,
    ExceptionRouter,
    ExceptionSeverity,
)
from gera.validation import ReconciliationCheck, ZScoreGate


# ---------------------------------------------------------------------------
# Benchmark harness
# ---------------------------------------------------------------------------

DEFAULT_SCALES: Tuple[int, ...] = (1_000, 10_000, 100_000)
REPEATS_DEFAULT = 5
WARMUP_ITERS = 1


@dataclass
class BenchmarkResult:
    """Result of a single benchmark at a given record scale."""
    layer: str
    operation: str
    records: int
    repeats: int
    min_ms: float
    median_ms: float
    mean_ms: float
    p95_ms: float
    max_ms: float
    throughput_per_sec: float

    def to_row(self) -> str:
        return (
            f"  {self.operation:<32} n={self.records:>8,}  "
            f"median={self.median_ms:>8.2f}ms  "
            f"p95={self.p95_ms:>8.2f}ms  "
            f"thr={self.throughput_per_sec:>12,.0f} rec/s"
        )


def _time_once(fn: Callable[[], None]) -> float:
    """Return milliseconds for a single invocation."""
    gc.collect()
    start = time.perf_counter()
    fn()
    return (time.perf_counter() - start) * 1000.0


def run_benchmark(
    layer: str,
    operation: str,
    records: int,
    fn: Callable[[], None],
    repeats: int = REPEATS_DEFAULT,
) -> BenchmarkResult:
    """Run a benchmark with warmup and return aggregated timings."""
    for _ in range(WARMUP_ITERS):
        fn()

    samples_ms = [_time_once(fn) for _ in range(repeats)]
    samples_ms.sort()

    median = statistics.median(samples_ms)
    p95 = samples_ms[int(0.95 * (len(samples_ms) - 1))]
    throughput = records / (median / 1000.0) if median > 0 else float("inf")

    return BenchmarkResult(
        layer=layer,
        operation=operation,
        records=records,
        repeats=repeats,
        min_ms=min(samples_ms),
        median_ms=median,
        mean_ms=statistics.mean(samples_ms),
        p95_ms=p95,
        max_ms=max(samples_ms),
        throughput_per_sec=throughput,
    )


# ---------------------------------------------------------------------------
# Data generators (deterministic)
# ---------------------------------------------------------------------------

def _gen_records(n: int, drop_rate: float = 0.0, rng: np.random.Generator = None):
    """
    Generate matched source/target record pairs with optional drops.

    ``drop_rate`` is the fraction of target records to drop so the
    benchmark exercises the unmatched-source code path realistically
    (≈5 % is typical for finance reconciliations).
    """
    if rng is None:
        rng = np.random.default_rng(42)

    amounts = rng.normal(2_000, 800, n).round(2)
    departments = rng.choice(["finance", "ops", "treasury", "risk"], size=n)

    source = [
        {"txn_id": f"TXN-{i:09d}", "amount": float(amounts[i]), "department": str(departments[i])}
        for i in range(n)
    ]

    if drop_rate > 0:
        keep_mask = rng.random(n) >= drop_rate
        target = [source[i] for i in range(n) if keep_mask[i]]
    else:
        target = list(source)

    return source, target


# ---------------------------------------------------------------------------
# Individual benchmarks
# ---------------------------------------------------------------------------

def bench_deterministic_matcher(n: int, repeats: int) -> BenchmarkResult:
    source, target = _gen_records(n, drop_rate=0.05)
    matcher = DeterministicMatcher(
        key_fields=["txn_id"],
        value_fields=["amount"],
    )

    def run():
        matcher.match(source, target)

    return run_benchmark("Layer 1", "DeterministicMatcher.match", n, run, repeats)


def bench_zscore_gate(n: int, repeats: int) -> BenchmarkResult:
    rng = np.random.default_rng(7)
    historical = rng.normal(2_000, 800, 5_000).tolist()
    values = rng.normal(2_000, 800, n).tolist()
    record_ids = [f"REC-{i}" for i in range(n)]

    gate = ZScoreGate(sigma_threshold=2.5, block_threshold=4.0)

    def run():
        gate.validate(values, historical_values=historical, record_ids=record_ids)

    return run_benchmark("Layer 2", "ZScoreGate.validate", n, run, repeats)


def bench_reconciliation_checks(n: int, repeats: int) -> BenchmarkResult:
    source, target = _gen_records(n, drop_rate=0.02)
    src_keys = {r["txn_id"] for r in source}
    tgt_keys = {r["txn_id"] for r in target}
    src_amt = sum(r["amount"] for r in source)
    tgt_amt = sum(r["amount"] for r in target)
    checker = ReconciliationCheck(tolerance=0.01)

    def run():
        checker.run_all(
            source_count=len(source),
            target_count=len(target),
            source_amount=src_amt,
            target_amount=tgt_amt,
            source_keys=src_keys,
            target_keys=tgt_keys,
        )

    return run_benchmark("Layer 2", "ReconciliationCheck.run_all", n, run, repeats)


def bench_audit_logger_append(n: int, repeats: int) -> BenchmarkResult:
    def run():
        # Fresh logger each run so we measure steady-state append + hash cost,
        # not retention cleanup on a log that keeps growing across repeats.
        logger = AuditLogger(retention_days=2555)
        for i in range(n):
            logger.log_gate_decision(
                gate_name="bench_gate",
                decision="pass",
                details={"i": i, "amount": float(i)},
            )

    return run_benchmark("Layer 3", "AuditLogger.log (append)", n, run, repeats)


def bench_audit_logger_verify(n: int, repeats: int) -> BenchmarkResult:
    logger = AuditLogger(retention_days=2555)
    for i in range(n):
        logger.log_gate_decision(
            gate_name="bench_gate",
            decision="pass",
            details={"i": i},
        )

    def run():
        assert logger.verify_chain()

    return run_benchmark("Layer 3", "AuditLogger.verify_chain", n, run, repeats)


def bench_exception_router(n: int, repeats: int) -> BenchmarkResult:
    def run():
        router = ExceptionRouter()
        for i in range(n):
            router.route(
                source="bench",
                description=f"exception {i}",
                severity=ExceptionSeverity.MEDIUM,
            )

    return run_benchmark("Layer 1", "ExceptionRouter.route", n, run, repeats)


def bench_semantic_registry(n: int, repeats: int) -> BenchmarkResult:
    def run():
        reg = SemanticRegistry()
        for i in range(n):
            reg.register(MetricDefinition(
                name=f"metric_{i}",
                description=f"benchmark metric {i}",
                formula="SUM(amount)",
                owner="bench@example.com",
                sensitivity=DataSensitivity.INTERNAL,
                lineage=[f"source_{i % 10}"],
            ))

    return run_benchmark("Layer 3", "SemanticRegistry.register", n, run, repeats)


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

BENCHMARKS: List[Tuple[str, Callable[[int, int], BenchmarkResult]]] = [
    ("matcher", bench_deterministic_matcher),
    ("zscore", bench_zscore_gate),
    ("recon_checks", bench_reconciliation_checks),
    ("audit_append", bench_audit_logger_append),
    ("audit_verify", bench_audit_logger_verify),
    ("router", bench_exception_router),
    ("registry", bench_semantic_registry),
]


def _env_info() -> Dict[str, str]:
    return {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "python_version": sys.version.split()[0],
        "numpy_version": np.__version__,
        "platform": platform.platform(),
        "processor": platform.processor() or "unknown",
        "cpu_count": str(__import__("os").cpu_count() or 0),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="GERA performance benchmarks")
    parser.add_argument(
        "--scales",
        type=int,
        nargs="+",
        default=list(DEFAULT_SCALES),
        help="Record counts to benchmark (default: 1000 10000 100000)",
    )
    parser.add_argument(
        "--repeats",
        type=int,
        default=REPEATS_DEFAULT,
        help=f"Number of timing samples per benchmark (default: {REPEATS_DEFAULT})",
    )
    parser.add_argument(
        "--only",
        type=str,
        nargs="+",
        choices=[name for name, _ in BENCHMARKS],
        help="Run only a subset of benchmarks",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit results as JSON instead of a human-readable table",
    )
    args = parser.parse_args()

    selected = [
        (name, fn) for name, fn in BENCHMARKS
        if args.only is None or name in args.only
    ]

    all_results: List[BenchmarkResult] = []
    for name, fn in selected:
        if not args.json:
            print(f"\n── {name} ──────────────────────────────────────────────")
        for n in args.scales:
            # Skip very large scales for operations that are O(n) in wall-clock
            # but dominated by Python overhead (e.g. audit append 1M events
            # would take minutes and isn't representative of batch usage).
            if name in ("audit_append", "audit_verify", "router", "registry") and n > 100_000:
                continue
            result = fn(n, args.repeats)
            all_results.append(result)
            if not args.json:
                print(result.to_row())

    if args.json:
        out = {
            "environment": _env_info(),
            "results": [asdict(r) for r in all_results],
        }
        print(json.dumps(out, indent=2))
    else:
        print("\n" + "═" * 70)
        print("ENVIRONMENT")
        print("═" * 70)
        for k, v in _env_info().items():
            print(f"  {k:<18} {v}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
