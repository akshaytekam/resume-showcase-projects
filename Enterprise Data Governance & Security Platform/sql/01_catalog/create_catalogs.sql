
-- ============================================================
-- File        : create_catalogs.sql
-- Project     : Enterprise Data Governance Platform
-- Purpose     : Create environment-level Unity Catalog catalogs
-- Author      : Data Engineering Team
-- ============================================================


-- ============================================================
-- 1. Development Catalog
-- ============================================================

CREATE CATALOG IF NOT EXISTS dev_catalog
COMMENT 'Development catalog for the Enterprise Data Governance Platform';


-- ============================================================
-- 2. Test Catalog
-- ============================================================

CREATE CATALOG IF NOT EXISTS test_catalog
COMMENT 'Test/UAT catalog for the Enterprise Data Governance Platform';


-- ============================================================
-- 3. Production Catalog
-- ============================================================

CREATE CATALOG IF NOT EXISTS prod_catalog
COMMENT 'Production catalog for the Enterprise Data Governance Platform';
