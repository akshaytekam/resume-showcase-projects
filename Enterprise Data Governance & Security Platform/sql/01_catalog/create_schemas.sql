-- ============================================================
-- File        : create_schemas.sql
-- Project     : Enterprise Data Governance Platform
-- Purpose     : Create Bronze, Silver, Gold and Governance
--               schemas in all environments
-- ============================================================


-- ============================================================
-- DEVELOPMENT ENVIRONMENT
-- ============================================================

CREATE SCHEMA IF NOT EXISTS dev_catalog.bronze
COMMENT 'Raw ingested data from source systems';

CREATE SCHEMA IF NOT EXISTS dev_catalog.silver
COMMENT 'Cleaned, validated and standardized data';

CREATE SCHEMA IF NOT EXISTS dev_catalog.gold
COMMENT 'Business-ready curated datasets';

CREATE SCHEMA IF NOT EXISTS dev_catalog.governance
COMMENT 'Metadata, classification, audit and governance datasets';


-- ============================================================
-- TEST ENVIRONMENT
-- ============================================================

CREATE SCHEMA IF NOT EXISTS test_catalog.bronze
COMMENT 'Raw ingested data for testing';

CREATE SCHEMA IF NOT EXISTS test_catalog.silver
COMMENT 'Cleaned and validated test datasets';

CREATE SCHEMA IF NOT EXISTS test_catalog.gold
COMMENT 'Business-ready test datasets';

CREATE SCHEMA IF NOT EXISTS test_catalog.governance
COMMENT 'Governance and audit datasets for testing';


-- ============================================================
-- PRODUCTION ENVIRONMENT
-- ============================================================

CREATE SCHEMA IF NOT EXISTS prod_catalog.bronze
COMMENT 'Raw production data ingested from source systems';

CREATE SCHEMA IF NOT EXISTS prod_catalog.silver
COMMENT 'Cleaned, validated and standardized production data';

CREATE SCHEMA IF NOT EXISTS prod_catalog.gold
COMMENT 'Business-ready production datasets';

CREATE SCHEMA IF NOT EXISTS prod_catalog.governance
COMMENT 'Production metadata, classification, audit and governance datasets';
