-- The same table will track Bronze, Silver, Gold, batch, and streaming executions.

CREATE TABLE IF NOT EXISTS adas_catalog.dev.pipeline_audit
(
    audit_id BIGINT GENERATED ALWAYS AS IDENTITY,

    pipeline_name STRING,
    environment STRING,
    layer STRING,
    table_name STRING,

    batch_id STRING,

    start_time TIMESTAMP,
    end_time TIMESTAMP,

    records_read BIGINT,
    records_written BIGINT,
    records_rejected BIGINT,

    status STRING,

    error_message STRING,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA;
