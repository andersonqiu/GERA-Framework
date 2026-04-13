# GERA Framework

**Governed Enterprise Reconciliation Architecture**

A four-layer, open-source framework for cross-system financial data reconciliation, statistical validation, semantic governance, and security controls in regulated enterprises.

```
┌─────────────────────────────────────────────────────────────┐
│                    GERA Framework                           │
├─────────────────────────────────────────────────────────────┤
│  Layer 4: NIST CSF 2.0 Security Controls                   │
│  ┌─────────────┐ ┌──────────────┐ ┌──────────────────────┐ │
│  │ ABAC / RLS  │ │ Policy-as-   │ │ Compliance           │ │
│  │ (Terraform) │ │ Code (TF)    │ │ Mapping              │ │
│  └─────────────┘ └──────────────┘ └──────────────────────┘ │
├─────────────────────────────────────────────────────────────┤
│  Layer 3: Governed Semantic Standardization                 │
│  ┌─────────────┐ ┌──────────────┐ ┌──────────────────────┐ │
│  │ Semantic     │ │ Audit Logger │ │ Hash-Chain           │ │
│  │ Registry     │ │ (Append-Only)│ │ Verification         │ │
│  └─────────────┘ └──────────────┘ └──────────────────────┘ │
├─────────────────────────────────────────────────────────────┤
│  Layer 2: Multi-Layer Statistical Validation                │
│  ┌─────────────┐ ┌──────────────┐ ┌──────────────────────┐ │
│  │ Z-Score Gate│ │ Recon Checks │ │ Reasonableness       │ │
│  │ (2.5σ/4.0σ)│ │ (Count/Amt)  │ │ (Period Variance)    │ │
│  └─────────────┘ └──────────────┘ └──────────────────────┘ │
├─────────────────────────────────────────────────────────────┤
│  Layer 1: Deterministic Cross-System Reconciliation         │
│  ┌─────────────┐ ┌──────────────┐ ┌──────────────────────┐ │
│  │ Key Matcher │ │ Exception    │ │ FIFO Queue           │ │
│  │ (Composite) │ │ Router       │ │ (SLA Tracking)       │ │
│  └─────────────┘ └──────────────┘ └──────────────────────┘ │
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

## Publication

Qiu, Z. (2026). "Data Engineering Patterns for Cross-System Reconciliation in Regulated Enterprises." *TechRxiv* (IEEE). DOI: pending.

## License

Apache License 2.0 — see [LICENSE](LICENSE).

## Author

**Zhijun Qiu** — [GitHub](https://github.com/zhijunqiu)
