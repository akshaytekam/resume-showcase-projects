CREATE TABLE dbo.ingestion_watermarks
(
    dataset_name       VARCHAR(200) PRIMARY KEY,
    watermark_value    VARCHAR(200) NULL,
    updated_at_utc     DATETIME2 NOT NULL
        DEFAULT SYSUTCDATETIME(),
    last_success_run_id VARCHAR(100) NULL
);

CREATE TABLE dbo.ingestion_file_manifest
(
    manifest_id       BIGINT IDENTITY(1,1) PRIMARY KEY,
    dataset_name      VARCHAR(200) NOT NULL,
    source_file       VARCHAR(500) NOT NULL,
    batch_id          VARCHAR(200) NOT NULL,
    status            VARCHAR(20) NOT NULL,
    rows_copied       BIGINT NULL,
    pipeline_run_id   VARCHAR(100) NULL,
    processed_at_utc  DATETIME2 NULL,
    error_message     NVARCHAR(4000) NULL,
    created_at_utc    DATETIME2 NOT NULL
        DEFAULT SYSUTCDATETIME(),

    CONSTRAINT UQ_ingestion_file_batch
        UNIQUE (dataset_name, source_file, batch_id)
);


CREATE SCHEMA IF NOT EXISTS quince_dev.audit;

CREATE TABLE IF NOT EXISTS quince_dev.audit.ingestion_audit (
    run_id STRING,
    dataset_name STRING,
    source_file STRING,
    batch_id STRING,
    status STRING,
    rows_loaded BIGINT,
    started_at_utc TIMESTAMP,
    ended_at_utc TIMESTAMP,
    error_message STRING
)
USING DELTA;

CREATE TABLE IF NOT EXISTS quince_dev.audit.ingestion_manifest (
    dataset_name STRING,
    batch_id STRING,
    source_file STRING,
    status STRING,
    run_id STRING,
    rows_loaded BIGINT,
    completed_at_utc TIMESTAMP
)
USING DELTA;
