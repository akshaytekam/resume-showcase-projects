-- ============================================================
-- File: governance_audit_report.sql
-- ============================================================

USE CATALOG dev_catalog;

USE SCHEMA governance;


CREATE OR REPLACE VIEW governance.governance_audit_report
AS

SELECT

    run_id,

    pipeline_name,

    dataset_name,

    check_category,

    check_name,

    check_status,

    severity,

    message,

    started_at,

    completed_at,

    date(started_at) AS audit_date

FROM governance.governance_pipeline_audit;
