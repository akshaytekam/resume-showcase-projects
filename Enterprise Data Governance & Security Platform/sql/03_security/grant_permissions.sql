-- ============================================================
-- File        : grant_permissions.sql
-- Project     : Enterprise Data Governance Platform
-- Purpose     : Unity Catalog RBAC permissions
-- ============================================================


-- ============================================================
-- ENVIRONMENT
-- ============================================================

USE CATALOG dev_catalog;

-- ============================================================
-- GOVERNANCE ADMIN
-- ============================================================

GRANT USE CATALOG
ON CATALOG dev_catalog
TO `data_governance_admins`;


GRANT USE SCHEMA
ON SCHEMA dev_catalog.governance
TO `data_governance_admins`;


GRANT SELECT
ON SCHEMA dev_catalog.governance
TO `data_governance_admins`;

-- ============================================================
-- AUDITOR
-- ============================================================

GRANT USE CATALOG
ON CATALOG dev_catalog
TO `auditors`;


GRANT USE SCHEMA
ON SCHEMA dev_catalog.governance
TO `auditors`;


GRANT SELECT
ON SCHEMA dev_catalog.governance
TO `auditors`;

-- ============================================================
-- DATA ENGINEERS
-- ============================================================

GRANT USE CATALOG
ON CATALOG dev_catalog
TO `data_engineers`;

-- ============================================================
-- Bronze Access
-- ============================================================

GRANT USE SCHEMA
ON SCHEMA dev_catalog.bronze
TO `data_engineers`;

GRANT SELECT
ON SCHEMA dev_catalog.bronze
TO `data_engineers`;

GRANT CREATE TABLE
ON SCHEMA dev_catalog.bronze
TO `data_engineers`;
-- ============================================================
-- Silver Access
-- ============================================================

GRANT USE SCHEMA
ON SCHEMA dev_catalog.silver
TO `data_engineers`;


GRANT SELECT
ON SCHEMA dev_catalog.silver
TO `data_engineers`;


GRANT CREATE TABLE
ON SCHEMA dev_catalog.silver
TO `data_engineers`;

-- ============================================================
-- Gold Access
-- ============================================================

GRANT USE SCHEMA
ON SCHEMA dev_catalog.gold
TO `data_engineers`;


GRANT SELECT
ON SCHEMA dev_catalog.gold
TO `data_engineers`;

-- ============================================================
-- FINANCE ANALYST
-- ============================================================

GRANT USE CATALOG
ON CATALOG dev_catalog
TO `finance_analysts`;


GRANT USE SCHEMA
ON SCHEMA dev_catalog.gold
TO `finance_analysts`;


GRANT SELECT
ON SCHEMA dev_catalog.gold
TO `finance_analysts`;

-- ============================================================
-- HR ANALYST
-- ============================================================

GRANT USE CATALOG
ON CATALOG dev_catalog
TO `hr_analysts`;


GRANT USE SCHEMA
ON SCHEMA dev_catalog.gold
TO `hr_analysts`;


GRANT SELECT
ON SCHEMA dev_catalog.gold
TO `hr_analysts`;

-- ============================================================
-- DATA ANALYST
-- ============================================================

GRANT USE CATALOG
ON CATALOG dev_catalog
TO `data_analysts`;


GRANT USE SCHEMA
ON SCHEMA dev_catalog.gold
TO `data_analysts`;


GRANT SELECT
ON SCHEMA dev_catalog.gold
TO `data_analysts`;

-- ============================================================
-- SECURITY ADMIN
-- ============================================================

GRANT USE CATALOG
ON CATALOG dev_catalog
TO `security_admins`;


GRANT USE SCHEMA
ON SCHEMA dev_catalog.governance
TO `security_admins`;


GRANT SELECT
ON SCHEMA dev_catalog.governance
TO `security_admins`;

-- ============================================================
-- DATA PLATFORM SERVICE ACCOUNTS
-- ============================================================

GRANT USE CATALOG
ON CATALOG dev_catalog
TO `data_platform_service_accounts`;


GRANT USE SCHEMA
ON SCHEMA dev_catalog.bronze
TO `data_platform_service_accounts`;


GRANT SELECT
ON SCHEMA dev_catalog.bronze
TO `data_platform_service_accounts`;


GRANT CREATE TABLE
ON SCHEMA dev_catalog.bronze
TO `data_platform_service_accounts`;


GRANT USE SCHEMA
ON SCHEMA dev_catalog.silver
TO `data_platform_service_accounts`;


GRANT SELECT
ON SCHEMA dev_catalog.silver
TO `data_platform_service_accounts`;


GRANT CREATE TABLE
ON SCHEMA dev_catalog.silver
TO `data_platform_service_accounts`;


GRANT USE SCHEMA
ON SCHEMA dev_catalog.gold
TO `data_platform_service_accounts`;


GRANT SELECT
ON SCHEMA dev_catalog.gold
TO `data_platform_service_accounts`;

-- Governance Tables for Service Accounts
  
GRANT USE SCHEMA
ON SCHEMA dev_catalog.governance
TO `data_platform_service_accounts`;


GRANT SELECT
ON SCHEMA dev_catalog.governance
TO `data_platform_service_accounts`;

-- Table-Level Access for Specific Engineering Needs

GRANT SELECT
ON TABLE dev_catalog.bronze.products
TO `data_analysts`;

