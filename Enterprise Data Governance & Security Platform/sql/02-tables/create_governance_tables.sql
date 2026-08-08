-- ============================================================
-- File        : create_governance_tables.sql
-- Project     : Enterprise Data Governance Platform
-- Purpose     : Create governance, metadata, quality and
--               audit tables
-- ============================================================


-- ============================================================
-- 1. DATASET METADATA
-- ============================================================

CREATE TABLE IF NOT EXISTS dev_catalog.governance.dataset_metadata
(
    dataset_id              STRING,
    catalog_name            STRING,
    schema_name             STRING,
    table_name              STRING,

    domain                  STRING,
    dataset_description     STRING,

    source_system           STRING,
    source_owner            STRING,

    data_owner              STRING,
    data_steward            STRING,

    classification          STRING,

    contains_pii            BOOLEAN,
    contains_financial_data BOOLEAN,

    refresh_frequency       STRING,

    retention_period_days   INT,

    status                  STRING,

    created_at              TIMESTAMP,
    updated_at              TIMESTAMP

)
USING DELTA
COMMENT 'Enterprise dataset metadata and ownership information';


-- ============================================================
-- 2. COLUMN METADATA
-- ============================================================

CREATE TABLE IF NOT EXISTS dev_catalog.governance.column_metadata
(
    dataset_id          STRING,

    catalog_name        STRING,
    schema_name         STRING,
    table_name          STRING,

    column_name         STRING,
    business_name       STRING,

    description         STRING,

    data_type           STRING,

    classification      STRING,

    pii_flag            BOOLEAN,
    financial_flag      BOOLEAN,

    masking_required    BOOLEAN,

    row_filter_required BOOLEAN,

    data_owner          STRING,
    data_steward        STRING,

    created_at          TIMESTAMP,
    updated_at          TIMESTAMP

)
USING DELTA
COMMENT 'Column-level metadata and sensitivity classification';


-- ============================================================
-- 3. DATA CLASSIFICATION
-- ============================================================

CREATE TABLE IF NOT EXISTS dev_catalog.governance.data_classification
(
    classification_id       STRING,

    catalog_name            STRING,
    schema_name             STRING,
    table_name              STRING,
    column_name             STRING,

    classification          STRING,

    sensitivity_level       STRING,

    pii_flag                BOOLEAN,
    financial_flag          BOOLEAN,

    regulatory_requirement  STRING,

    masking_required        BOOLEAN,

    access_restriction      STRING,

    classification_reason  STRING,

    classified_by           STRING,
    classified_at           TIMESTAMP,

    reviewed_by             STRING,
    reviewed_at             TIMESTAMP

)
USING DELTA
COMMENT 'Data sensitivity and regulatory classification';


-- ============================================================
-- 4. DATA QUALITY RESULTS
-- ============================================================

CREATE TABLE IF NOT EXISTS dev_catalog.governance.data_quality_results
(
    execution_id        STRING,

    pipeline_name       STRING,

    table_name          STRING,
    column_name         STRING,

    check_name          STRING,
    check_type          STRING,

    total_records       BIGINT,
    failed_records      BIGINT,

    failure_percentage  DOUBLE,

    threshold_percentage DOUBLE,

    status              STRING,

    error_message       STRING,

    execution_timestamp TIMESTAMP

)
USING DELTA
COMMENT 'Results of data quality and governance validation checks';


-- ============================================================
-- 5. INGESTION AUDIT
-- ============================================================

CREATE TABLE IF NOT EXISTS dev_catalog.governance.ingestion_audit
(
    execution_id       STRING,

    pipeline_name      STRING,

    source_system      STRING,
    source_file        STRING,

    target_catalog     STRING,
    target_schema      STRING,
    target_table       STRING,

    start_time         TIMESTAMP,
    end_time           TIMESTAMP,

    records_read       BIGINT,
    records_written    BIGINT,
    records_rejected   BIGINT,

    status             STRING,

    error_message      STRING

)
USING DELTA
COMMENT 'Audit information for data ingestion pipelines';


-- ============================================================
-- 6. PERMISSION AUDIT
-- ============================================================

CREATE TABLE IF NOT EXISTS dev_catalog.governance.permission_audit
(
    audit_id          STRING,

    principal         STRING,

    principal_type    STRING,

    object_type       STRING,

    object_name       STRING,

    permission        STRING,

    action            STRING,

    previous_permission STRING,
    new_permission      STRING,

    requested_by      STRING,
    approved_by       STRING,

    changed_by        STRING,

    change_reason     STRING,

    changed_at        TIMESTAMP

)
USING DELTA
COMMENT 'Audit trail for data access and permission changes';


-- ============================================================
-- 7. GOVERNANCE AUDIT
-- ============================================================

CREATE TABLE IF NOT EXISTS dev_catalog.governance.governance_audit
(
    audit_id          STRING,

    operation_type    STRING,

    object_type       STRING,

    object_name       STRING,

    attribute_name    STRING,

    previous_value    STRING,
    new_value         STRING,

    performed_by      STRING,

    reason            STRING,

    performed_at      TIMESTAMP

)
USING DELTA
COMMENT 'Audit trail for governance metadata and policy changes';
