-- ============================================================
-- File        : register_data_assets.sql
-- Project     : Enterprise Data Governance Platform
-- Purpose     : Enterprise Data Asset Inventory
-- ============================================================


-- ============================================================
-- 1. ENVIRONMENT
-- ============================================================

USE CATALOG dev_catalog;

USE SCHEMA governance;

-- ============================================================
-- 2. DATA ASSET REGISTRY
-- ============================================================

CREATE TABLE IF NOT EXISTS data_asset_registry (

    asset_id BIGINT
        GENERATED ALWAYS AS IDENTITY,

    catalog_name STRING NOT NULL,

    schema_name STRING NOT NULL,

    table_name STRING NOT NULL,

    asset_type STRING NOT NULL,

    asset_name STRING NOT NULL,

    business_domain STRING NOT NULL,

    business_owner STRING NOT NULL,

    data_steward STRING,

    technical_owner STRING,

    source_system STRING,

    source_type STRING,

    environment STRING,

    criticality STRING,

    data_classification STRING,

    contains_pii BOOLEAN DEFAULT FALSE,

    contains_financial_data BOOLEAN DEFAULT FALSE,

    contains_pci_data BOOLEAN DEFAULT FALSE,

    refresh_frequency STRING,

    expected_sla_minutes INT,

    retention_period_days INT,

    business_description STRING,

    technical_description STRING,

    status STRING DEFAULT 'ACTIVE',

    created_at TIMESTAMP
        DEFAULT CURRENT_TIMESTAMP(),

    updated_at TIMESTAMP
        DEFAULT CURRENT_TIMESTAMP()

)
USING DELTA;

-- ============================================================
-- 3. CUSTOMER DATASET
-- ============================================================

INSERT INTO data_asset_registry
(
    catalog_name,
    schema_name,
    table_name,
    asset_type,
    asset_name,
    business_domain,
    business_owner,
    data_steward,
    technical_owner,
    source_system,
    source_type,
    environment,
    criticality,
    data_classification,
    contains_pii,
    contains_financial_data,
    contains_pci_data,
    refresh_frequency,
    expected_sla_minutes,
    retention_period_days,
    business_description,
    technical_description
)

VALUES
(
    'dev_catalog',
    'silver',
    'customers',
    'TABLE',
    'Customer Master',
    'Customer',
    'Customer Operations',
    'Customer Data Steward',
    'Data Engineering',
    'CRM',
    'API',
    'DEV',
    'HIGH',
    'PII',
    TRUE,
    FALSE,
    FALSE,
    'DAILY',
    120,
    2555,
    'Central customer master dataset used by sales, marketing and customer operations.',
    'Cleansed customer data stored in Delta format in the Silver layer.'
);

-- ============================================================
-- 4. ORDER DATASET
-- ============================================================

INSERT INTO data_asset_registry
(
    catalog_name,
    schema_name,
    table_name,
    asset_type,
    asset_name,
    business_domain,
    business_owner,
    data_steward,
    technical_owner,
    source_system,
    source_type,
    environment,
    criticality,
    data_classification,
    contains_pii,
    contains_financial_data,
    refresh_frequency,
    expected_sla_minutes,
    retention_period_days,
    business_description,
    technical_description
)

VALUES
(
    'dev_catalog',
    'silver',
    'orders',
    'TABLE',
    'Customer Orders',
    'Sales',
    'Sales Operations',
    'Sales Data Steward',
    'Data Engineering',
    'Retail POS',
    'CSV',
    'DEV',
    'HIGH',
    'INTERNAL',
    TRUE,
    TRUE,
    'DAILY',
    90,
    2555,
    'Customer order transactions used for sales reporting and operational analytics.',
    'Validated and transformed order records stored as Delta tables.'
);

-- ============================================================
-- 5. PAYMENT DATASET
-- ============================================================

INSERT INTO data_asset_registry
(
    catalog_name,
    schema_name,
    table_name,
    asset_type,
    asset_name,
    business_domain,
    business_owner,
    data_steward,
    technical_owner,
    source_system,
    source_type,
    environment,
    criticality,
    data_classification,
    contains_pii,
    contains_financial_data,
    contains_pci_data,
    refresh_frequency,
    expected_sla_minutes,
    retention_period_days,
    business_description,
    technical_description
)

VALUES
(
    'dev_catalog',
    'silver',
    'payments',
    'TABLE',
    'Payment Transactions',
    'Finance',
    'Finance Operations',
    'Payments Data Steward',
    'Data Engineering',
    'Payment Gateway',
    'API',
    'DEV',
    'CRITICAL',
    'RESTRICTED',
    FALSE,
    TRUE,
    TRUE,
    'DAILY',
    60,
    2555,
    'Payment transaction dataset used for financial reconciliation and reporting.',
    'Validated payment transactions stored in Delta format with restricted access.'
);

-- ============================================================
-- 6. EMPLOYEE DATASET
-- ============================================================

INSERT INTO data_asset_registry
(
    catalog_name,
    schema_name,
    table_name,
    asset_type,
    asset_name,
    business_domain,
    business_owner,
    data_steward,
    technical_owner,
    source_system,
    source_type,
    environment,
    criticality,
    data_classification,
    contains_pii,
    contains_financial_data,
    refresh_frequency,
    expected_sla_minutes,
    retention_period_days,
    business_description,
    technical_description
)

VALUES
(
    'dev_catalog',
    'silver',
    'employees',
    'TABLE',
    'Employee Master',
    'Human Resources',
    'Human Resources',
    'HR Data Steward',
    'Data Engineering',
    'HRIS',
    'API',
    'DEV',
    'CRITICAL',
    'RESTRICTED',
    TRUE,
    TRUE,
    FALSE,
    'DAILY',
    120,
    3650,
    'Employee master data used for HR operations and workforce analytics.',
    'Restricted employee data stored in Delta format with department-level row security.'
);

-- ============================================================
-- 7. PRODUCT DATASET
-- ============================================================

INSERT INTO data_asset_registry
(
    catalog_name,
    schema_name,
    table_name,
    asset_type,
    asset_name,
    business_domain,
    business_owner,
    data_steward,
    technical_owner,
    source_system,
    source_type,
    environment,
    criticality,
    data_classification,
    contains_pii,
    contains_financial_data,
    contains_pci_data,
    refresh_frequency,
    expected_sla_minutes,
    retention_period_days,
    business_description,
    technical_description
)

VALUES
(
    'dev_catalog',
    'silver',
    'products',
    'TABLE',
    'Product Master',
    'Product',
    'Product Management',
    'Product Data Steward',
    'Data Engineering',
    'Product Management System',
    'API',
    'DEV',
    'MEDIUM',
    'INTERNAL',
    FALSE,
    FALSE,
    FALSE,
    'DAILY',
    240,
    3650,
    'Product catalog used for sales and inventory reporting.',
    'Standardized product reference data stored as Delta.'
);

-- ============================================================
-- 8. STORE DATASET
-- ============================================================

INSERT INTO data_asset_registry
(
    catalog_name,
    schema_name,
    table_name,
    asset_type,
    asset_name,
    business_domain,
    business_owner,
    data_steward,
    technical_owner,
    source_system,
    source_type,
    environment,
    criticality,
    data_classification,
    contains_pii,
    contains_financial_data,
    contains_pci_data,
    refresh_frequency,
    expected_sla_minutes,
    retention_period_days,
    business_description,
    technical_description
)

VALUES
(
    'dev_catalog',
    'silver',
    'stores',
    'TABLE',
    'Retail Store Master',
    'Retail',
    'Retail Operations',
    'Retail Data Steward',
    'Data Engineering',
    'Store Management System',
    'Database',
    'DEV',
    'MEDIUM',
    'INTERNAL',
    FALSE,
    FALSE,
    FALSE,
    'DAILY',
    240,
    3650,
    'Store reference data used for regional sales and operational reporting.',
    'Standardized store metadata stored as Delta.'
);

-- ============================================================
-- 9. GOLD CUSTOMER DATA PRODUCT
-- ============================================================

INSERT INTO data_asset_registry
(
    catalog_name,
    schema_name,
    table_name,
    asset_type,
    asset_name,
    business_domain,
    business_owner,
    data_steward,
    technical_owner,
    source_system,
    source_type,
    environment,
    criticality,
    data_classification,
    contains_pii,
    refresh_frequency,
    expected_sla_minutes,
    retention_period_days,
    business_description,
    technical_description
)

VALUES
(
    'dev_catalog',
    'gold',
    'secure_customers',
    'VIEW',
    'Governed Customer Data Product',
    'Customer',
    'Customer Operations',
    'Customer Data Steward',
    'Data Engineering',
    'dev_catalog.silver.customers',
    'Delta',
    'DEV',
    'HIGH',
    'PII',
    TRUE,
    'DAILY',
    120,
    2555,
    'Governed customer dataset exposed to authorized analytical users.',
    'Secure view implementing column masking and governance controls.'
);

INSERT INTO data_asset_registry
(
    catalog_name,
    schema_name,
    table_name,
    asset_type,
    asset_name,
    business_domain,
    business_owner,
    data_steward,
    technical_owner,
    source_system,
    source_type,
    environment,
    criticality,
    data_classification,
    contains_financial_data,
    contains_pci_data,
    refresh_frequency,
    expected_sla_minutes,
    retention_period_days,
    business_description,
    technical_description
)

VALUES
(
    'dev_catalog',
    'gold',
    'secure_payments',
    'VIEW',
    'Governed Payment Data Product',
    'Finance',
    'Finance Operations',
    'Payments Data Steward',
    'Data Engineering',
    'dev_catalog.silver.payments',
    'Delta',
    'DEV',
    'CRITICAL',
    'RESTRICTED',
    FALSE,
    TRUE,
    'DAILY',
    60,
    2555,
    'Governed payment data exposed to authorized finance users.',
    'Secure view implementing country-based row filtering and column masking.'
);

