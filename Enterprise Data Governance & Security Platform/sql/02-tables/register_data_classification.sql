-- ============================================================
-- File        : register_data_classification.sql
-- Project     : Enterprise Data Governance Platform
-- Purpose     : Register enterprise data classification rules
-- ============================================================


-- ============================================================
-- 1. Use Governance Schema
-- ============================================================

USE CATALOG dev_catalog;

USE SCHEMA governance;


-- ============================================================
-- 2. Create Classification Reference Table
-- ============================================================

CREATE TABLE IF NOT EXISTS data_classification (
    
    classification_id BIGINT GENERATED ALWAYS AS IDENTITY,

    classification_name STRING NOT NULL,

    description STRING,

    sensitivity_level INT,

    requires_masking BOOLEAN,

    requires_restricted_access BOOLEAN,

    regulatory_category STRING,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()

)
USING DELTA;


-- ============================================================
-- 3. Insert Classification Definitions
-- ============================================================

INSERT INTO data_classification
(
    classification_name,
    description,
    sensitivity_level,
    requires_masking,
    requires_restricted_access,
    regulatory_category
)

VALUES

(
    'PUBLIC',
    'Information approved for public distribution',
    1,
    FALSE,
    FALSE,
    NULL
),

(
    'INTERNAL',
    'Information intended for internal company use',
    2,
    FALSE,
    FALSE,
    NULL
),

(
    'CONFIDENTIAL',
    'Business-sensitive information requiring controlled access',
    3,
    FALSE,
    TRUE,
    NULL
),

(
    'PII',
    'Personally identifiable information',
    4,
    TRUE,
    TRUE,
    'Privacy'
),

(
    'SENSITIVE',
    'Highly sensitive information requiring additional protection',
    5,
    TRUE,
    TRUE,
    'Privacy'
),

(
    'FINANCIAL',
    'Financial or monetary business information',
    5,
    TRUE,
    TRUE,
    'Financial'
),

(
    'RESTRICTED',
    'Highly restricted information with limited authorized access',
    6,
    TRUE,
    TRUE,
    'Security'
);
