"""
FIFO Exception Queue with SLA Tracking

Routes reconciliation exceptions through a priority queue with
automatic SLA enforcement and escalation. Designed for SOX
Section 404 compliance workflows.
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional


class ExceptionSeverity(Enum):
    """Exception severity levels."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class ExceptionStatus(Enum):
    """Exception lifecycle status."""
    OPEN = "open"
    ASSIGNED = "assigned"
    ESCALATED = "escalated"
    RESOLVED = "resolved"


# SLA hours by severity
_SLA_HOURS = {
    ExceptionSeverity.CRITICAL: 1,
    ExceptionSeverity.HIGH: 4,
    ExceptionSeverity.MEDIUM: 24,
    ExceptionSeverity.LOW: 72,
}


@dataclass
class GERAException:
    """A reconciliation exception with SLA tracking."""
    exception_id: str
    source: str
    description: str
    severity: ExceptionSeverity
    status: ExceptionStatus = ExceptionStatus.OPEN
    created_at: datetime = field(default_factory=datetime.utcnow)
    assigned_to: Optional[str] = None
    resolved_at: Optional[datetime] = None
    resolution_notes: Optional[str] = None

    @property
    def age_hours(self) -> float:
        """Hours since exception was created."""
        end = self.resolved_at or datetime.utcnow()
        return (end - self.created_at).total_seconds() / 3600

    @property
    def sla_breached(self) -> bool:
        """Whether this exception has exceeded its SLA."""
        if self.status == ExceptionStatus.RESOLVED:
            return False
        sla = _SLA_HOURS.get(self.severity, 72)
        return self.age_hours > sla


class ExceptionRouter:
    """
    FIFO exception queue with automatic SLA enforcement.

    Routes reconciliation exceptions, tracks resolution,
    and escalates overdue items.
    """

    def __init__(self):
        self._queue: List[GERAException] = []
        self._counter: int = 0

    def route(
        self,
        source: str,
        description: str,
        severity: ExceptionSeverity = ExceptionSeverity.MEDIUM,
    ) -> GERAException:
        """Create and route a new exception."""
        self._counter += 1
        exc = GERAException(
            exception_id=f"EXC-{self._counter:06d}",
            source=source,
            description=description,
            severity=severity,
        )
        self._queue.append(exc)
        return exc

    def resolve(
        self,
        exception_id: str,
        resolution_notes: str = "",
    ) -> bool:
        """Resolve an exception by ID. Returns False if not found."""
        for exc in self._queue:
            if exc.exception_id == exception_id:
                exc.status = ExceptionStatus.RESOLVED
                exc.resolved_at = datetime.utcnow()
                exc.resolution_notes = resolution_notes
                return True
        return False

    def escalate_overdue(self) -> List[GERAException]:
        """Escalate all exceptions that have breached their SLA."""
        escalated = []
        for exc in self._queue:
            if exc.sla_breached and exc.status != ExceptionStatus.ESCALATED:
                exc.status = ExceptionStatus.ESCALATED
                escalated.append(exc)
        return escalated

    @property
    def open_count(self) -> int:
        return sum(
            1 for e in self._queue
            if e.status not in (ExceptionStatus.RESOLVED,)
        )

    def get_queue_summary(self) -> Dict:
        """Get summary statistics of the exception queue."""
        by_status: Dict[str, int] = {}
        by_severity: Dict[str, int] = {}
        for exc in self._queue:
            by_status[exc.status.value] = by_status.get(exc.status.value, 0) + 1
            by_severity[exc.severity.value] = by_severity.get(exc.severity.value, 0) + 1

        return {
            "total": len(self._queue),
            "open": self.open_count,
            "by_status": by_status,
            "by_severity": by_severity,
        }
