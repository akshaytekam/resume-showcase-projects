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
