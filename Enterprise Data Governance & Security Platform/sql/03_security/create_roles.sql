-- ============================================================
-- File        : create_roles.sql
-- Project     : Enterprise Data Governance Platform
-- Purpose     : Define enterprise RBAC groups and personas
-- ============================================================


-- ============================================================
-- IMPORTANT
-- ============================================================
--
-- In Databricks Unity Catalog, access is normally granted
-- to account-level groups managed through the identity system.
--
-- The group names below represent enterprise groups that would
-- normally be provisioned through Microsoft Entra ID / Okta /
-- another corporate identity provider.
--
-- For this practice project, we use these names consistently.
-- ============================================================


-- ============================================================
-- 1. DATA ENGINEERING GROUP
-- ============================================================

-- Enterprise group:
--
-- data_engineers
--
-- Purpose:
--   Build and operate data pipelines.
--
-- Typical users:
--   Data Engineers
--   Platform Engineers


-- ============================================================
-- 2. DATA ANALYST GROUP
-- ============================================================

-- Enterprise group:
--
-- data_analysts
--
-- Purpose:
--   Analyze approved business datasets.


-- ============================================================
-- 3. FINANCE ANALYST GROUP
-- ============================================================

-- Enterprise group:
--
-- finance_analysts
--
-- Purpose:
--   Analyze financial and payment data.


-- ============================================================
-- 4. HR ANALYST GROUP
-- ============================================================

-- Enterprise group:
--
-- hr_analysts
--
-- Purpose:
--   Analyze employee/HR datasets.


-- ============================================================
-- 5. SECURITY ADMINISTRATOR GROUP
-- ============================================================

-- Enterprise group:
--
-- security_admins
--
-- Purpose:
--   Manage security policies and investigate security events.


-- ============================================================
-- 6. DATA GOVERNANCE ADMINISTRATOR GROUP
-- ============================================================

-- Enterprise group:
--
-- data_governance_admins
--
-- Purpose:
--   Manage metadata, classification, ownership,
--   governance policies, and compliance controls.


-- ============================================================
-- 7. AUDITOR GROUP
-- ============================================================

-- Enterprise group:
--
-- auditors
--
-- Purpose:
--   Read governance and audit information.


-- ============================================================
-- 8. SERVICE ACCOUNT GROUP
-- ============================================================

-- Enterprise group:
--
-- data_platform_service_accounts
--
-- Purpose:
--   Run automated ingestion and transformation jobs.
