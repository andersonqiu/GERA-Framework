"""
Append-Only Audit Logger with SHA-256 Hash Chaining

Provides tamper-evident audit logging for GERA pipeline events.
Each event is cryptographically chained to its predecessor,
enabling detection of any log tampering or deletion.

Designed for SOX Section 404 (7-year retention) and
NIST CSF 2.0 DE.CM continuous monitoring requirements.
"""

import hashlib
import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional


class EventType(Enum):
    """Audit event categories."""
    DATA_ACCESS = "data_access"
    DATA_MODIFICATION = "data_modification"
    GATE_DECISION = "gate_decision"
    RECONCILIATION = "reconciliation"
    EXCEPTION_CREATED = "exception_created"
    EXCEPTION_RESOLVED = "exception_resolved"
    SCHEMA_CHANGE = "schema_change"
    POLICY_VIOLATION = "policy_violation"
    USER_ACTION = "user_action"
    SYSTEM_EVENT = "system_event"


@dataclass(frozen=True)
class AuditEvent:
    """Immutable audit event with hash chain link."""
    event_id: str
    event_type: EventType
    timestamp: datetime
    actor: str
    action: str
    resource: str
    details: Dict[str, Any]
    previous_hash: str
    event_hash: str


class AuditLogger:
    """
    Append-only audit logger with SHA-256 hash chaining.

    Events are immutable once logged. The hash chain allows
    verification that no events have been modified or deleted.

    Args:
        retention_days: Log retention period (default: 2555 ~7 years for SOX)
    """

    def __init__(self, retention_days: int = 2555):
        self.retention_days = retention_days
        self._events: List[AuditEvent] = []
        self._last_hash: str = "genesis"

    def _compute_hash(self, event_data: Dict[str, Any]) -> str:
        """Compute SHA-256 hash of event data."""
        serialized = json.dumps(event_data, sort_keys=True, default=str)
        return hashlib.sha256(serialized.encode()).hexdigest()

    def log(
        self,
        event_type: EventType,
        actor: str,
        action: str,
        resource: str,
        details: Optional[Dict[str, Any]] = None,
    ) -> AuditEvent:
        """Log an audit event with hash chain linking."""
        event_id = f"AUD-{len(self._events) + 1:08d}"
        timestamp = datetime.utcnow()
        details = details or {}

        event_data = {
            "event_id": event_id,
            "event_type": event_type.value,
            "timestamp": str(timestamp),
            "actor": actor,
            "action": action,
            "resource": resource,
            "details": details,
            "previous_hash": self._last_hash,
        }

        event_hash = self._compute_hash(event_data)

        event = AuditEvent(
            event_id=event_id,
            event_type=event_type,
            timestamp=timestamp,
            actor=actor,
            action=action,
            resource=resource,
            details=details,
            previous_hash=self._last_hash,
            event_hash=event_hash,
        )

        self._events.append(event)
        self._last_hash = event_hash
        return event

    def log_gate_decision(
        self,
        gate_name: str,
        decision: str,
        details: Optional[Dict[str, Any]] = None,
    ) -> AuditEvent:
        """Convenience method for logging gate decisions."""
        return self.log(
            event_type=EventType.GATE_DECISION,
            actor="gera_pipeline",
            action=f"gate_{decision}",
            resource=gate_name,
            details=details or {},
        )

    def log_access(
        self,
        actor: str,
        resource: str,
        action: str = "read",
    ) -> AuditEvent:
        """Convenience method for logging data access."""
        return self.log(
            event_type=EventType.DATA_ACCESS,
            actor=actor,
            action=action,
            resource=resource,
        )

    def verify_chain(self) -> bool:
        """Verify the integrity of the entire hash chain."""
        if not self._events:
            return True

        expected_prev = "genesis"
        for event in self._events:
            if event.previous_hash != expected_prev:
                return False

            event_data = {
                "event_id": event.event_id,
                "event_type": event.event_type.value,
                "timestamp": str(event.timestamp),
                "actor": event.actor,
                "action": event.action,
                "resource": event.resource,
                "details": event.details,
                "previous_hash": event.previous_hash,
            }
            computed = self._compute_hash(event_data)
            if computed != event.event_hash:
                return False

            expected_prev = event.event_hash

        return True

    def query(
        self,
        event_type: Optional[EventType] = None,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        actor: Optional[str] = None,
    ) -> List[AuditEvent]:
        """Query events with optional filters."""
        results = self._events
        if event_type is not None:
            results = [e for e in results if e.event_type == event_type]
        if start is not None:
            results = [e for e in results if e.timestamp >= start]
        if end is not None:
            results = [e for e in results if e.timestamp <= end]
        if actor is not None:
            results = [e for e in results if e.actor == actor]
        return results

    def export(self, format: str = "json") -> str:
        """Export audit log as JSON."""
        records = []
        for e in self._events:
            records.append({
                "event_id": e.event_id,
                "event_type": e.event_type.value,
                "timestamp": str(e.timestamp),
                "actor": e.actor,
                "action": e.action,
                "resource": e.resource,
                "details": e.details,
                "previous_hash": e.previous_hash,
                "event_hash": e.event_hash,
            })
        return json.dumps(records, indent=2, default=str)

    @property
    def event_count(self) -> int:
        return len(self._events)
