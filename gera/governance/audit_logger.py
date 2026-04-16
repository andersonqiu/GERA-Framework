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
import math
import threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple


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
    Thread-safe: concurrent log() calls are serialised by an
    internal lock to guarantee unique event IDs and chain integrity.

    Args:
        retention_days: Log retention period (default: 2555 ~7 years for SOX)
    """

    def __init__(self, retention_days: int = 2555):
        self.retention_days = retention_days
        self._events: List[AuditEvent] = []
        self._last_hash: str = "genesis"
        self._lock = threading.Lock()
        # Track oldest timestamp so we can skip O(n) cleanup scans when we
        # know nothing could possibly have aged out.  Without this, log()
        # becomes O(n) and append-heavy workloads degrade to O(n²).
        self._oldest_timestamp: Optional[datetime] = None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _sanitize_value(value: Any) -> Any:
        """
        Recursively replace non-RFC-7159-compliant float values.

        Python's json module serialises float('inf') as the bare token
        ``Infinity``, which is rejected by strict JSON parsers (BigQuery,
        Athena, etc.).  Replace with labelled strings before export so the
        output is always valid JSON.
        """
        if isinstance(value, float):
            if math.isnan(value):
                return "NaN"
            if math.isinf(value):
                return "Infinity" if value > 0 else "-Infinity"
            return value
        if isinstance(value, dict):
            return {k: AuditLogger._sanitize_value(v) for k, v in value.items()}
        if isinstance(value, list):
            return [AuditLogger._sanitize_value(v) for v in value]
        return value

    def _compute_hash(self, event_data: Dict[str, Any]) -> str:
        """Compute SHA-256 hash of event data."""
        serialized = json.dumps(event_data, sort_keys=True, default=str)
        return hashlib.sha256(serialized.encode()).hexdigest()

    def _cleanup_expired(self) -> None:
        """
        Remove in-memory events older than retention_days.

        Called inside the lock on every log() so memory is bounded.
        In production, events should be flushed to a persistent backend
        before they age out — this method only trims the in-memory copy.

        Optimised: events are always appended in chronological order, so
        we only need to scan when the oldest known timestamp is actually
        past the retention cutoff.  Skipping the scan keeps log() O(1)
        amortised instead of O(n) per call.
        """
        if self._oldest_timestamp is None:
            return

        cutoff = datetime.now(timezone.utc) - timedelta(days=self.retention_days)
        if self._oldest_timestamp >= cutoff:
            return

        # At least one event has expired — find the first survivor.
        # Linear scan is fine here because this path is taken rarely
        # (only after retention_days of continuous logging).
        first_keep = 0
        for i, e in enumerate(self._events):
            if e.timestamp >= cutoff:
                first_keep = i
                break
        else:
            # Every event expired.
            self._events = []
            self._oldest_timestamp = None
            return

        if first_keep > 0:
            self._events = self._events[first_keep:]
            self._oldest_timestamp = self._events[0].timestamp

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def log(
        self,
        event_type: EventType,
        actor: str,
        action: str,
        resource: str,
        details: Optional[Dict[str, Any]] = None,
    ) -> AuditEvent:
        """Log an audit event with hash chain linking."""
        details = details or {}

        with self._lock:
            event_id = f"AUD-{len(self._events) + 1:08d}"
            timestamp = datetime.now(timezone.utc)

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
            if self._oldest_timestamp is None:
                self._oldest_timestamp = timestamp
            self._cleanup_expired()

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
        """
        Verify the integrity of the entire hash chain.

        Returns True if the chain is intact, False if any event has been
        tampered with or deleted.  For forensic detail on the first
        violation found, use :meth:`verify_chain_detail`.
        """
        valid, _ = self.verify_chain_detail()
        return valid

    def verify_chain_detail(self) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """
        Verify the hash chain and return the first violation found.

        Returns:
            (True, None) if the chain is intact.
            (False, detail_dict) where detail_dict contains:
                - event_index: position of the violating event
                - event_id: ID of the violating event
                - violation: "prev_hash_mismatch" or "hash_mismatch"
                - expected / actual fields describing the discrepancy
        """
        if not self._events:
            return True, None

        expected_prev = "genesis"
        for i, event in enumerate(self._events):
            if event.previous_hash != expected_prev:
                return False, {
                    "event_index": i,
                    "event_id": event.event_id,
                    "violation": "prev_hash_mismatch",
                    "expected_prev_hash": expected_prev,
                    "actual_prev_hash": event.previous_hash,
                }

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
                return False, {
                    "event_index": i,
                    "event_id": event.event_id,
                    "violation": "hash_mismatch",
                    "expected_hash": computed,
                    "actual_hash": event.event_hash,
                }

            expected_prev = event.event_hash

        return True, None

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
        """
        Export audit log as valid RFC-7159 JSON.

        Non-finite float values (inf, -inf, NaN) in event details are
        replaced with labelled strings so the output is accepted by all
        standard JSON parsers.
        """
        records = []
        for e in self._events:
            records.append({
                "event_id": e.event_id,
                "event_type": e.event_type.value,
                "timestamp": str(e.timestamp),
                "actor": e.actor,
                "action": e.action,
                "resource": e.resource,
                "details": self._sanitize_value(e.details),
                "previous_hash": e.previous_hash,
                "event_hash": e.event_hash,
            })
        return json.dumps(records, indent=2, default=str)

    @property
    def event_count(self) -> int:
        return len(self._events)
