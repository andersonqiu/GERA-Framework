"""
NIST CSF 2.0 Control Mapping

Maps GERA Framework features to NIST Cybersecurity Framework 2.0
control families. Provides compliance tracking and audit report
generation for regulated enterprises.

Mapped controls:
- GV.OC: Organizational Context (sensitivity classification)
- GV.RM: Risk Management (anomaly detection thresholds)
- PR.AA: Identity Management & Access Control (ABAC + RLS)
- PR.DS: Data Security (hash chain + append-only audit)
- DE.CM: Continuous Monitoring (real-time gate decisions)
"""

from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any


@dataclass
class CSF2Control:
    """A NIST CSF 2.0 control mapping to GERA."""
    control_id: str
    function_name: str
    category: str
    description: str
    gera_implementation: str
    evidence_artifacts: List[str] = field(default_factory=list)


# Pre-built GERA-to-CSF 2.0 mappings
GERA_CSF2_MAPPINGS: List[CSF2Control] = [
    CSF2Control(
        control_id="GV.OC-01",
        function_name="GOVERN",
        category="Organizational Context",
        description=(
            "The organizational mission is understood and informs "
            "cybersecurity risk management"
        ),
        gera_implementation=(
            "SemanticRegistry classifies all metrics by DataSensitivity "
            "(PUBLIC/INTERNAL/CONFIDENTIAL/RESTRICTED), ensuring data "
            "governance aligns with organizational risk tolerance."
        ),
        evidence_artifacts=[
            "semantic_registry_export.json",
            "data_classification_policy.md",
        ],
    ),
    CSF2Control(
        control_id="GV.RM-01",
        function_name="GOVERN",
        category="Risk Management Strategy",
        description=(
            "Risk management objectives are established and used to "
            "support operational risk decisions"
        ),
        gera_implementation=(
            "ZScoreGate configures anomaly detection thresholds "
            "(sigma=2.5 FLAG, sigma=4.0 BLOCK) based on organizational "
            "risk appetite. Rolling 90-day baselines adapt to business "
            "seasonality while maintaining sensitivity."
        ),
        evidence_artifacts=[
            "zscore_gate_config.json",
            "gate_decision_audit_log.json",
        ],
    ),
    CSF2Control(
        control_id="PR.AA-01",
        function_name="PROTECT",
        category="Identity Management, Authentication, and Access Control",
        description=(
            "Identities and credentials for authorized users, services, "
            "and hardware are managed by the organization"
        ),
        gera_implementation=(
            "Terraform ABAC templates enforce role-based access with "
            "sensitivity-level clearance. BigQuery Row-Level Security "
            "policies restrict data access based on user_attributes "
            "table containing NIST sensitivity labels."
        ),
        evidence_artifacts=[
            "terraform/abac/main.tf",
            "terraform/bigquery_rls/main.tf",
            "iam_role_assignments.json",
        ],
    ),
    CSF2Control(
        control_id="PR.DS-01",
        function_name="PROTECT",
        category="Data Security",
        description=(
            "The confidentiality, integrity, and availability of data-at-rest "
            "is protected"
        ),
        gera_implementation=(
            "AuditLogger implements append-only logging with SHA-256 hash "
            "chaining. Each event is cryptographically linked to its "
            "predecessor, enabling detection of any tampering. Events are "
            "frozen (immutable) after creation. 7-year retention for SOX."
        ),
        evidence_artifacts=[
            "audit_chain_verification.json",
            "terraform/audit_logging/main.tf",
        ],
    ),
    CSF2Control(
        control_id="DE.CM-01",
        function_name="DETECT",
        category="Continuous Monitoring",
        description=(
            "Networks and network services are monitored to find "
            "potentially adverse events"
        ),
        gera_implementation=(
            "Real-time gate decisions from ZScoreGate and "
            "ReconciliationCheck are logged to the audit trail. "
            "ExceptionRouter provides FIFO exception queuing with "
            "automatic SLA enforcement and escalation of overdue items."
        ),
        evidence_artifacts=[
            "gate_decision_audit_log.json",
            "exception_queue_summary.json",
        ],
    ),
]


class CSF2ControlMapper:
    """
    Maps GERA features to NIST CSF 2.0 controls.

    Provides compliance tracking and audit report generation.
    """

    def __init__(
        self, mappings: Optional[List[CSF2Control]] = None
    ):
        self.mappings = mappings or GERA_CSF2_MAPPINGS
        self._index = {m.control_id: m for m in self.mappings}

    def get_control(self, control_id: str) -> Optional[CSF2Control]:
        """Look up a control by ID."""
        return self._index.get(control_id)

    def compliance_summary(self) -> Dict[str, Any]:
        """Generate compliance summary."""
        return {
            "framework": "NIST CSF 2.0",
            "total_controls_mapped": len(self.mappings),
            "functions_covered": sorted(
                set(m.function_name for m in self.mappings)
            ),
            "coverage_pct": 100.0,  # All mapped controls are implemented
            "controls": [
                {
                    "id": m.control_id,
                    "function": m.function_name,
                    "category": m.category,
                    "implemented": True,
                }
                for m in self.mappings
            ],
        }

    def generate_audit_report(self) -> str:
        """Generate a formatted audit report."""
        lines = [
            "=" * 60,
            "NIST CSF 2.0 COMPLIANCE REPORT — GERA FRAMEWORK",
            "=" * 60,
            "",
        ]
        for m in self.mappings:
            lines.extend([
                f"Control: {m.control_id} ({m.function_name})",
                f"Category: {m.category}",
                f"Requirement: {m.description}",
                f"Implementation: {m.gera_implementation}",
                f"Evidence: {', '.join(m.evidence_artifacts)}",
                "-" * 60,
                "",
            ])
        lines.append(
            f"Total controls mapped: {len(self.mappings)}"
        )
        return "\n".join(lines)
