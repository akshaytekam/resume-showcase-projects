-- ============================================================
-- File: governance_compliance_report.sql
-- ============================================================

USE CATALOG dev_catalog;

USE SCHEMA governance;


CREATE OR REPLACE VIEW governance.governance_compliance_report
AS

SELECT

    a.asset_id,

    a.catalog_name,

    a.schema_name,

    a.table_name,

    a.domain,

    a.business_owner,

    a.data_classification,

    a.criticality,

    a.contains_pii,

    a.contains_financial_data,

    a.contains_pci,

    CASE
        WHEN r.table_name IS NOT NULL
        THEN 'YES'
        ELSE 'NO'
    END AS rls_enabled,

    CASE
        WHEN a.data_classification IN
             ('PII', 'PCI', 'FINANCIAL')
        THEN 'REQUIRED'
        ELSE 'OPTIONAL'
    END AS rls_requirement,

    CASE

        WHEN a.data_classification IN
             ('PII', 'PCI', 'FINANCIAL')
             AND r.table_name IS NULL
        THEN 'NON_COMPLIANT'

        WHEN a.business_owner IS NULL
        THEN 'NON_COMPLIANT'

        ELSE 'COMPLIANT'

    END AS governance_status

FROM data_asset_registry a

LEFT JOIN row_level_security_registry r

    ON a.catalog_name = r.catalog_name
   AND a.schema_name = r.schema_name
   AND a.table_name = r.table_name
   AND r.policy_status = 'ACTIVE';
