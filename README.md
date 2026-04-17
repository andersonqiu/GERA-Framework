# GERA Framework

**Governed Enterprise Reconciliation Architecture**

[![arXiv](https://img.shields.io/badge/arXiv-2604.15108-b31b1b.svg)](https://arxiv.org/abs/2604.15108)
[![DOI](https://img.shields.io/badge/DOI-10.48550%2FarXiv.2604.15108-blue.svg)](https://doi.org/10.48550/arXiv.2604.15108)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)

A four-layer, open-source framework for cross-system financial data reconciliation, statistical validation, semantic governance, and security controls in regulated enterprises.

Companion code for the arXiv preprint *"Data Engineering Patterns for Cross-System Reconciliation in Regulated Enterprises: Architecture, Anomaly Detection, and Governance"* ([arXiv:2604.15108](https://arxiv.org/abs/2604.15108)).

```
┌─────────────────────────────────────────────────────────────┐
│                    GERA Framework                           │
├─────────────────────────────────────────────────────────────┤
│  Layer 4: NIST CSF 2.0 Security Controls                    │
│  ┌─────────────┐ ┌──────────────┐ ┌──────────────────────┐  │
│  │ ABAC / RLS  │ │ Policy-as-   │ │ Compliance           │  │
│  │ (Terraform) │ │ Code (TF)    │ │ Mapping              │  │
│  └─────────────┘ └──────────────┘ └──────────────────────┘  │
├─────────────────────────────────────────────────────────────┤
│  Layer 3: Governed Semantic Standardization                 │
│  ┌─────────────┐ ┌──────────────┐ ┌──────────────────────┐  │
│  │ Semantic    │ │ Audit Logger │ │ Hash-Chain           │  │
│  │ Registry    │ │ (Append-Only)│ │ Verification         │  │
│  └─────────────┘ └──────────────┘ └──────────────────────┘  │
├─────────────────────────────────────────────────────────────┤
│  Layer 2: Multi-Layer Statistical Validation                │
│  ┌─────────────┐ ┌──────────────┐ ┌──────────────────────┐  │
│  │ Z-Score Gate│ │ Recon Checks │ │ Reasonableness       │  │
│  │ (2.5σ/4.0σ) │ │ (Count/Amt)  │ │ (Period Variance)    │  │
│  └─────────────┘ └──────────────┘ └──────────────────────┘  │
├─────────────────────────────────────────────────────────────┤
│  Layer 1: Deterministic Cross-System Reconciliation         │
│  ┌─────────────┐ ┌──────────────┐ ┌──────────────────────┐  │
│  │ Key Matcher │ │ Exception    │ │ FIFO Queue           │  │
│  │ (Composite) │ │ Router       │ │ (SLA Tracking)       │  │
│  └─────────────┘ └──────────────┘ └──────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

## Why GERA?

Financial enterprises operating under SOX Section 404, PCAOB standards, and NIST CSF 2.0 need **auditable, repeatable reconciliation** across disparate systems. GERA provides:

- **Deterministic matching** with composite keys, normalization, and conflict detection
- **Statistical anomaly detection** using Z-Score gates with configurable thresholds
- **Governed semantic definitions** with versioned metric registries
- **Tamper-evident audit logging** with SHA-256 hash chaining (7-year SOX retention)
- **Policy-as-Code security** via Terraform templates for BigQuery RLS and ABAC

## Quick Start

```python
from gera.reconciliation import DeterministicMatcher
from gera.validation import ZScoreGate, ReconciliationCheck
from gera.governance import AuditLogger

# Match records across systems
matcher = DeterministicMatcher(key_fields=["txn_id"], value_fields=["amount"])
report = matcher.match(source_records, target_records)

# Statistical anomaly detection
gate = ZScoreGate(sigma_threshold=2.5, block_threshold=4.0)
result = gate.validate(amounts, historical_baseline)

# Tamper-evident audit logging
logger = AuditLogger(retention_days=2555)  # ~7 years for SOX
logger.log_gate_decision("reconciliation", result.gate_decision.value)
assert logger.verify_chain()  # Verify no tampering
```

## Modules

| Module | Layer | Description |
|--------|-------|-------------|
| `gera.reconciliation` | 1 | Deterministic key matching + FIFO exception routing |
| `gera.validation` | 2 | Z-Score anomaly detection + reconciliation checks |
| `gera.governance` | 3 | Semantic registry + append-only audit logging |
| `gera.nist` | 4 | NIST CSF 2.0 control mapping + compliance reports |
| `terraform/` | 4 | BigQuery RLS, ABAC roles, audit sink templates |

## Regulatory Alignment

| Regulation | GERA Feature |
|-----------|-------------|
| SOX Section 404 | Hash-chained audit trail, 7-year retention |
| PCAOB AS 2201 | Deterministic reconciliation + statistical validation |
| NIST CSF 2.0 GV.OC | Sensitivity classification in SemanticRegistry |
| NIST CSF 2.0 PR.AA | Terraform ABAC + BigQuery Row-Level Security |
| NIST CSF 2.0 DE.CM | Real-time gate decisions + exception routing |

## Installation

```bash
pip install -e .
```

## Running Tests

```bash
pytest tests/ -v
```

Tests are split into unit (`tests/test_reconciliation.py`, `tests/test_zscore_gate.py`)
and integration (`tests/test_integration.py`). Integration tests exercise all four
layers end-to-end under clean, mismatched, anomalous, and tampered scenarios.

## Examples

| Script | Scenario |
|--------|----------|
| `examples/basic_reconciliation.py` | Two-source reconciliation with all four layers |
| `examples/multi_source_reconciliation.py` | Three-way GL ⇄ Processor ⇄ Warehouse reconciliation with classified exceptions |

Run from the project root:

```bash
python examples/basic_reconciliation.py
python examples/multi_source_reconciliation.py
```

## Performance

GERA ships a deterministic benchmark suite that measures each layer's
latency and throughput at enterprise-scale record counts.

```bash
python -m benchmarks.benchmark_reconciliation
python -m benchmarks.benchmark_reconciliation --scales 10000 100000 --json
```

Representative results on a dual-core x86_64 (Python 3.10, NumPy 2.2):

| Operation | n = 1,000 | n = 10,000 | Throughput |
|-----------|-----------|------------|------------|
| `DeterministicMatcher.match` | 2.1 ms | 29.2 ms | ≈ 340K rec/s |
| `ZScoreGate.validate` | 2.6 ms | 24.3 ms | ≈ 410K rec/s |
| `ReconciliationCheck.run_all` | 0.05 ms | 0.32 ms | ≈ 30M rec/s |
| `AuditLogger.log` (append) | 14 ms | 160 ms | ≈ 63K events/s |
| `AuditLogger.verify_chain` | 9 ms | 86 ms | ≈ 115K events/s |
| `ExceptionRouter.route` | 1.7 ms | 17.6 ms | ≈ 570K rec/s |
| `SemanticRegistry.register` | 2.1 ms | 21.3 ms | ≈ 470K rec/s |

All measured operations are **linear in record count** — there are no
hidden quadratic scans in the hot path. Absolute numbers vary with
hardware; use the benchmark on your own deployment target for capacity
planning.

## Publication

Qiu, Z. (2026). *Data Engineering Patterns for Cross-System Reconciliation in Regulated Enterprises: Architecture, Anomaly Detection, and Governance.* arXiv:2604.15108 [cs.DB; cs.CY]. DOI: [10.48550/arXiv.2604.15108](https://doi.org/10.48550/arXiv.2604.15108). <https://arxiv.org/abs/2604.15108>

Submitted 16 April 2026. 13 pages, 3 figures, 1 table. ACM classes: H.2.7, H.2.8, K.6.5.

## Citation

If you use this software or reference this architecture in academic work, please cite the paper:

```bibtex
@article{qiu2026gera,
  title   = {Data Engineering Patterns for Cross-System Reconciliation in Regulated Enterprises:
             Architecture, Anomaly Detection, and Governance},
  author  = {Qiu, Zhijun},
  journal = {arXiv preprint arXiv:2604.15108},
  year    = {2026},
  month   = apr,
  doi     = {10.48550/arXiv.2604.15108},
  url     = {https://arxiv.org/abs/2604.15108},
  archivePrefix = {arXiv},
  eprint  = {2604.15108},
  primaryClass  = {cs.DB}
}
```

A machine-readable [`CITATION.cff`](CITATION.cff) is also provided; GitHub will surface it automatically via the **"Cite this repository"** button in the sidebar.

## License

Apache License 2.0 — see [LICENSE](LICENSE).

## Author

**Zhijun Qiu** — [GitHub](https://github.com/andersonqiu)
