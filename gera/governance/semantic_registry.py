"""
Governed Semantic Registry

Centralized metric definitions with versioning, lineage tracking,
sensitivity classification (per NIST GV.OC), and conformance
validation. Ensures consistent business terminology across
enterprise data systems.
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional


class DataSensitivity(Enum):
    """Data sensitivity classification per NIST CSF 2.0 GV.OC."""
    PUBLIC = "public"
    INTERNAL = "internal"
    CONFIDENTIAL = "confidential"
    RESTRICTED = "restricted"


@dataclass
class MetricDefinition:
    """A governed metric definition with lineage and SLA."""
    name: str
    description: str
    formula: str
    owner: str
    sensitivity: DataSensitivity = DataSensitivity.INTERNAL
    source_system: str = ""
    refresh_frequency: str = "daily"
    sla_hours: float = 24.0
    lineage: List[str] = field(default_factory=list)
    version: int = 1
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)


class SemanticRegistry:
    """
    Centralized registry of governed metric definitions.

    Provides registration, versioning, search, and conformance
    validation for enterprise metrics.
    """

    def __init__(self):
        self._metrics: Dict[str, MetricDefinition] = {}

    def register(self, metric: MetricDefinition) -> MetricDefinition:
        """Register a new metric. Raises if name already exists."""
        if metric.name in self._metrics:
            raise ValueError(
                f"Metric '{metric.name}' already registered. "
                "Use update() to modify."
            )
        self._metrics[metric.name] = metric
        return metric

    def get(self, name: str) -> Optional[MetricDefinition]:
        """Get a metric by name."""
        return self._metrics.get(name)

    def update(self, name: str, **kwargs) -> MetricDefinition:
        """Update a metric, bumping its version."""
        metric = self._metrics.get(name)
        if metric is None:
            raise KeyError(f"Metric '{name}' not found")

        for key, value in kwargs.items():
            if hasattr(metric, key):
                setattr(metric, key, value)

        metric.version += 1
        metric.updated_at = datetime.utcnow()
        return metric

    def search(self, query: str) -> List[MetricDefinition]:
        """Search metrics by name or description."""
        query_lower = query.lower()
        return [
            m for m in self._metrics.values()
            if query_lower in m.name.lower()
            or query_lower in m.description.lower()
        ]

    def validate_conformance(
        self, metric_name: str, value: Any
    ) -> Dict[str, Any]:
        """Validate a value conforms to metric definition."""
        metric = self._metrics.get(metric_name)
        if metric is None:
            return {
                "is_valid": False,
                "error": f"Metric '{metric_name}' not found in registry",
            }

        checks = {
            "metric_exists": True,
            "has_owner": bool(metric.owner),
            "has_formula": bool(metric.formula),
            "has_lineage": len(metric.lineage) > 0,
            "value_type_valid": isinstance(value, (int, float, str)),
        }

        return {
            "is_valid": all(checks.values()),
            "checks": checks,
            "metric_version": metric.version,
            "sensitivity": metric.sensitivity.value,
        }

    def export_glossary(self) -> List[Dict[str, Any]]:
        """Export all metrics as a business glossary."""
        return [
            {
                "name": m.name,
                "description": m.description,
                "formula": m.formula,
                "owner": m.owner,
                "sensitivity": m.sensitivity.value,
                "source_system": m.source_system,
                "refresh_frequency": m.refresh_frequency,
                "version": m.version,
            }
            for m in sorted(self._metrics.values(), key=lambda x: x.name)
        ]

    @property
    def count(self) -> int:
        return len(self._metrics)
