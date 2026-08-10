-- ============================================================
-- File: governance_dashboard_metrics.sql
-- ============================================================

USE CATALOG dev_catalog;

USE SCHEMA governance;


CREATE OR REPLACE VIEW governance.dashboard_metrics
AS

SELECT

    COUNT(*) AS total_assets,

    SUM(
        CASE
            WHEN governance_status = 'COMPLIANT'
            THEN 1
            ELSE 0
        END
    ) AS compliant_assets,

    SUM(
        CASE
            WHEN governance_status = 'NON_COMPLIANT'
            THEN 1
            ELSE 0
        END
    ) AS non_compliant_assets,

    SUM(
        CASE
            WHEN data_classification = 'PII'
            THEN 1
            ELSE 0
        END
    ) AS pii_assets,

    SUM(
        CASE
            WHEN data_classification = 'PCI'
            THEN 1
            ELSE 0
        END
    ) AS pci_assets,

    SUM(
        CASE
            WHEN data_classification = 'FINANCIAL'
            THEN 1
            ELSE 0
        END
    ) AS financial_assets

FROM governance.governance_compliance_report;
