-- GERA Framework — Athena / Glue DDL
-- Table: exceptions_queue
-- Layer: 1 (Deterministic Reconciliation)
-- Python counterpart: gera.reconciliation.exception_router.GERAException
--
-- FIFO-ordered queue of reconciliation exceptions with SLA tracking.
-- Severity drives the SLA clock: critical = 1 h, high = 4 h, medium = 24 h, low = 72 h
-- (defaults match the Python ExceptionRouter). The derived view normalizes
-- severity case so warehouse rows exported from Python enum values keep the
-- same SLA semantics.
--
-- NIST CSF 2.0 controls: DE.AE-02, RS.MA-02.

CREATE EXTERNAL TABLE IF NOT EXISTS `${database}`.`exceptions_queue` (
  exception_id     STRING    COMMENT 'Unique exception identifier (FIFO ordering preserves creation order)',
  source           STRING    COMMENT 'Source system that raised the exception',
  description      STRING    COMMENT 'Human-readable description of the reconciliation gap',
  severity         STRING    COMMENT 'Enum: low | medium | high | critical',
  status           STRING    COMMENT 'Enum: open | assigned | escalated | resolved',
  created_at       TIMESTAMP COMMENT 'UTC creation timestamp',
  assigned_to      STRING    COMMENT 'Current assignee (email, team alias, or NULL if unassigned)',
  resolved_at      TIMESTAMP COMMENT 'UTC resolution timestamp; NULL while open',
  resolution_notes STRING    COMMENT 'Free-text notes written when the exception is closed'
)
PARTITIONED BY (
  created_date STRING COMMENT 'YYYY-MM-DD partition key derived from created_at'
)
STORED AS PARQUET
LOCATION 's3://${bucket}/exceptions_queue/'
TBLPROPERTIES (
  'classification'                       = 'parquet',
  'parquet.compression'                  = 'SNAPPY',
  'projection.enabled'                   = 'true',
  'projection.created_date.type'         = 'date',
  'projection.created_date.format'       = 'yyyy-MM-dd',
  'projection.created_date.range'        = '2025-01-01,NOW',
  'projection.created_date.interval'     = '1',
  'projection.created_date.interval.unit' = 'DAYS',
  'storage.location.template'            = 's3://${bucket}/exceptions_queue/created_date=${created_date}/',
  'framework'                            = 'gera',
  'layer'                                = '1'
);

-- SLA age + breach view — mirrors BigQuery v_exceptions_sla and the Python
-- GERAException.is_sla_breached property.
CREATE OR REPLACE VIEW `${database}`.`v_exceptions_sla` AS
SELECT
  e.*,
  date_diff(
    'hour',
    e.created_at,
    COALESCE(e.resolved_at, current_timestamp)
  ) AS age_hours,
  CASE UPPER(e.severity)
    WHEN 'CRITICAL' THEN 1
    WHEN 'HIGH'   THEN 4
    WHEN 'MEDIUM' THEN 24
    WHEN 'LOW'    THEN 72
    ELSE 24
  END AS sla_hours,
  date_diff(
    'hour',
    e.created_at,
    COALESCE(e.resolved_at, current_timestamp)
  ) >
  CASE UPPER(e.severity)
    WHEN 'CRITICAL' THEN 1
    WHEN 'HIGH'   THEN 4
    WHEN 'MEDIUM' THEN 24
    WHEN 'LOW'    THEN 72
    ELSE 24
  END AS is_sla_breached
FROM `${database}`.`exceptions_queue` AS e;
