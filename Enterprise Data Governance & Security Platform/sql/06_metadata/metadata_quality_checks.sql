-- ============================================================
-- File: metadata_quality_checks.sql
-- ============================================================

USE CATALOG dev_catalog;

USE SCHEMA governance;


-- ============================================================
-- 1. FIND ASSETS WITHOUT BUSINESS OWNER
-- ============================================================

SELECT
    asset_id,
    catalog_name,
    schema_name,
    table_name,
    'MISSING_BUSINESS_OWNER' AS issue
FROM data_asset_registry
WHERE business_owner IS NULL
   OR trim(business_owner) = '';


-- ============================================================
-- 2. FIND ASSETS WITHOUT CLASSIFICATION
-- ============================================================

SELECT
    asset_id,
    table_name,
    'MISSING_CLASSIFICATION' AS issue
FROM data_asset_registry
WHERE data_classification IS NULL;


-- ============================================================
-- 3. FIND CRITICAL DATA WITHOUT OWNER
-- ============================================================

SELECT
    asset_id,
    table_name,
    criticality,
    business_owner
FROM data_asset_registry
WHERE criticality = 'CRITICAL'
  AND business_owner IS NULL;


-- ============================================================
-- 4. FIND SENSITIVE DATA WITHOUT SECURITY CONTROLS
-- ============================================================

SELECT
    a.asset_id,
    a.table_name,
    a.data_classification
FROM data_asset_registry a

LEFT JOIN row_level_security_registry r
    ON a.catalog_name = r.catalog_name
   AND a.schema_name = r.schema_name
   AND a.table_name = r.table_name
   AND r.policy_status = 'ACTIVE'

WHERE a.data_classification
      IN ('PII', 'PCI', 'FINANCIAL')

  AND r.table_name IS NULL;
