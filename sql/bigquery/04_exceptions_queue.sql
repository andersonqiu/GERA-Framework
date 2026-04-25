-- GERA Framework — BigQuery DDL
-- Table: exceptions_queue
-- Layer: 1 (Deterministic Reconciliation)
-- Python counterpart: gera.reconciliation.exception_router.GERAException
--
-- FIFO-ordered queue of reconciliation exceptions with SLA tracking. Severity
-- drives the SLA clock: critical = 1 h, high = 4 h, medium = 24 h, low = 72 h
-- (defaults match the Python ExceptionRouter). The derived view normalizes
-- severity case so warehouse rows exported from Python enum values keep the
-- same SLA semantics.
--
-- NIST CSF 2.0 controls: DE.AE-02, RS.MA-02.

CREATE TABLE IF NOT EXISTS `${project}.${dataset}.exceptions_queue` (
  exception_id     STRING    NOT NULL OPTIONS(description="Unique exception identifier (FIFO ordering preserves creation order)"),
  source           STRING    NOT NULL OPTIONS(description="Source system that raised the exception"),
  description      STRING    NOT NULL OPTIONS(description="Human-readable description of the reconciliation gap"),
  severity         STRING    NOT NULL OPTIONS(description="Enum: low | medium | high | critical — drives SLA"),
  status           STRING    NOT NULL OPTIONS(description="Enum: open | assigned | escalated | resolved"),
  created_at       TIMESTAMP NOT NULL OPTIONS(description="UTC creation timestamp — SLA clock starts here"),
  assigned_to      STRING             OPTIONS(description="Current assignee (email, team alias, or NULL if unassigned)"),
  resolved_at      TIMESTAMP          OPTIONS(description="UTC resolution timestamp; NULL while status != RESOLVED"),
  resolution_notes STRING             OPTIONS(description="Free-text notes written when the exception is closed"),

  PRIMARY KEY (exception_id) NOT ENFORCED
)
PARTITION BY DATE(created_at)
CLUSTER BY severity, status
OPTIONS(
  description = "GERA Layer 1 reconciliation exceptions with SLA tracking",
  labels      = [("framework", "gera"), ("layer", "1")]
);

-- Derived view — SLA age and breach flag, matching GERAException.is_sla_breached.
CREATE OR REPLACE VIEW `${project}.${dataset}.v_exceptions_sla` AS
SELECT
  e.*,
  TIMESTAMP_DIFF(
    IFNULL(e.resolved_at, CURRENT_TIMESTAMP()),
    e.created_at,
    HOUR
  ) AS age_hours,
  CASE UPPER(e.severity)
    WHEN 'CRITICAL' THEN 1
    WHEN 'HIGH'   THEN 4
    WHEN 'MEDIUM' THEN 24
    WHEN 'LOW'    THEN 72
    ELSE 24
  END AS sla_hours,
  TIMESTAMP_DIFF(
    IFNULL(e.resolved_at, CURRENT_TIMESTAMP()),
    e.created_at,
    HOUR
  ) >
  CASE UPPER(e.severity)
    WHEN 'CRITICAL' THEN 1
    WHEN 'HIGH'   THEN 4
    WHEN 'MEDIUM' THEN 24
    WHEN 'LOW'    THEN 72
    ELSE 24
  END AS is_sla_breached
FROM `${project}.${dataset}.exceptions_queue` AS e;
