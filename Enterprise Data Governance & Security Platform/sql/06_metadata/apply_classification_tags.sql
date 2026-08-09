-- ============================================================
-- File        : apply_classification_tags.sql
-- Project     : Enterprise Data Governance Platform
-- Purpose     : Classify and tag sensitive data assets
-- ============================================================


-- ============================================================
-- 1. ENVIRONMENT
-- ============================================================

USE CATALOG dev_catalog;

USE SCHEMA governance;

-- ============================================================
-- 2. CLASSIFICATION REGISTRY
-- ============================================================

CREATE TABLE IF NOT EXISTS
column_classification_registry (

    classification_id BIGINT
        GENERATED ALWAYS AS IDENTITY,

    catalog_name STRING NOT NULL,

    schema_name STRING NOT NULL,

    table_name STRING NOT NULL,

    column_name STRING NOT NULL,

    classification STRING NOT NULL,

    sensitivity_level STRING NOT NULL,

    contains_pii BOOLEAN DEFAULT FALSE,

    contains_financial_data BOOLEAN DEFAULT FALSE,

    contains_pci_data BOOLEAN DEFAULT FALSE,

    masking_required BOOLEAN DEFAULT FALSE,

    encryption_required BOOLEAN DEFAULT FALSE,

    business_owner STRING,

    data_steward STRING,

    classification_reason STRING,

    classification_status STRING DEFAULT 'ACTIVE',

    created_at TIMESTAMP
        DEFAULT CURRENT_TIMESTAMP(),

    updated_at TIMESTAMP
        DEFAULT CURRENT_TIMESTAMP()

)
USING DELTA;

-- ============================================================
-- 3. CUSTOMER DATA CLASSIFICATION
-- ============================================================

INSERT INTO column_classification_registry
(
    catalog_name,
    schema_name,
    table_name,
    column_name,
    classification,
    sensitivity_level,
    contains_pii,
    contains_financial_data,
    contains_pci_data,
    masking_required,
    encryption_required,
    business_owner,
    data_steward,
    classification_reason
)

VALUES
(
    'dev_catalog',
    'silver',
    'customers',
    'customer_id',
    'PII',
    'HIGH',
    TRUE,
    FALSE,
    FALSE,
    TRUE,
    TRUE,
    'Customer Operations',
    'Data Governance Team',
    'Unique customer identifier'
);

INSERT INTO column_classification_registry
(
    catalog_name,
    schema_name,
    table_name,
    column_name,
    classification,
    sensitivity_level,
    contains_pii,
    masking_required,
    encryption_required,
    business_owner,
    data_steward,
    classification_reason
)

VALUES
(
    'dev_catalog',
    'silver',
    'customers',
    'email',
    'PII',
    'HIGH',
    TRUE,
    TRUE,
    TRUE,
    'Customer Operations',
    'Data Governance Team',
    'Customer email address'
);

INSERT INTO column_classification_registry
(
    catalog_name,
    schema_name,
    table_name,
    column_name,
    classification,
    sensitivity_level,
    contains_pii,
    masking_required,
    encryption_required,
    business_owner,
    data_steward,
    classification_reason
)

VALUES
(
    'dev_catalog',
    'silver',
    'customers',
    'phone_number',
    'PII',
    'HIGH',
    TRUE,
    TRUE,
    TRUE,
    'Customer Operations',
    'Data Governance Team',
    'Customer telephone number'
);

INSERT INTO column_classification_registry
(
    catalog_name,
    schema_name,
    table_name,
    column_name,
    classification,
    sensitivity_level,
    contains_pii,
    masking_required,
    encryption_required,
    business_owner,
    data_steward,
    classification_reason
)

VALUES
(
    'dev_catalog',
    'silver',
    'customers',
    'date_of_birth',
    'SENSITIVE',
    'HIGH',
    TRUE,
    TRUE,
    TRUE,
    'Customer Operations',
    'Data Governance Team',
    'Customer date of birth'
);

INSERT INTO column_classification_registry
(
    catalog_name,
    schema_name,
    table_name,
    column_name,
    classification,
    sensitivity_level,
    contains_pii,
    masking_required,
    encryption_required,
    business_owner,
    data_steward,
    classification_reason
)

VALUES
(
    'dev_catalog',
    'silver',
    'customers',
    'address',
    'PII',
    'HIGH',
    TRUE,
    TRUE,
    TRUE,
    'Customer Operations',
    'Data Governance Team',
    'Customer residential address'
);

INSERT INTO column_classification_registry
(
    catalog_name,
    schema_name,
    table_name,
    column_name,
    classification,
    sensitivity_level,
    contains_pii,
    masking_required,
    encryption_required,
    business_owner,
    data_steward,
    classification_reason
)

VALUES
(
    'dev_catalog',
    'silver',
    'customers',
    'customer_segment',
    'INTERNAL',
    'MEDIUM',
    FALSE,
    FALSE,
    FALSE,
    'Marketing',
    'Customer Data Steward',
    'Customer segmentation attribute'
);

-- ============================================================
-- 4. EMPLOYEE DATA CLASSIFICATION
-- ============================================================

INSERT INTO column_classification_registry
(
    catalog_name,
    schema_name,
    table_name,
    column_name,
    classification,
    sensitivity_level,
    contains_pii,
    masking_required,
    encryption_required,
    business_owner,
    data_steward,
    classification_reason
)

VALUES
(
    'dev_catalog',
    'silver',
    'employees',
    'employee_id',
    'PII',
    'HIGH',
    TRUE,
    TRUE,
    TRUE,
    'Human Resources',
    'HR Data Steward',
    'Unique employee identifier'
);

INSERT INTO column_classification_registry
(
    catalog_name,
    schema_name,
    table_name,
    column_name,
    classification,
    sensitivity_level,
    contains_pii,
    masking_required,
    encryption_required,
    business_owner,
    data_steward,
    classification_reason
)

VALUES
(
    'dev_catalog',
    'silver',
    'employees',
    'email',
    'PII',
    'HIGH',
    TRUE,
    TRUE,
    TRUE,
    'Human Resources',
    'HR Data Steward',
    'Employee email address'
);

INSERT INTO column_classification_registry
(
    catalog_name,
    schema_name,
    table_name,
    column_name,
    classification,
    sensitivity_level,
    contains_pii,
    contains_financial_data,
    masking_required,
    encryption_required,
    business_owner,
    data_steward,
    classification_reason
)

VALUES
(
    'dev_catalog',
    'silver',
    'employees',
    'annual_salary',
    'FINANCIAL',
    'RESTRICTED',
    TRUE,
    TRUE,
    TRUE,
    TRUE,
    'Human Resources',
    'HR Data Steward',
    'Employee compensation information'
);

INSERT INTO column_classification_registry
(
    catalog_name,
    schema_name,
    table_name,
    column_name,
    classification,
    sensitivity_level,
    contains_pii,
    masking_required,
    encryption_required,
    business_owner,
    data_steward,
    classification_reason
)

VALUES
(
    'dev_catalog',
    'silver',
    'employees',
    'department',
    'INTERNAL',
    'MEDIUM',
    FALSE,
    FALSE,
    FALSE,
    'Human Resources',
    'HR Data Steward',
    'Employee organizational information'
);

-- ============================================================
-- 5. PAYMENT DATA CLASSIFICATION
-- ============================================================

INSERT INTO column_classification_registry
(
    catalog_name,
    schema_name,
    table_name,
    column_name,
    classification,
    sensitivity_level,
    contains_pii,
    contains_financial_data,
    contains_pci_data,
    masking_required,
    encryption_required,
    business_owner,
    data_steward,
    classification_reason
)

VALUES
(
    'dev_catalog',
    'silver',
    'payments',
    'card_last4',
    'PCI',
    'RESTRICTED',
    FALSE,
    FALSE,
    TRUE,
    TRUE,
    TRUE,
    'Finance',
    'Payments Data Steward',
    'Payment card information'
);

INSERT INTO column_classification_registry
(
    catalog_name,
    schema_name,
    table_name,
    column_name,
    classification,
    sensitivity_level,
    contains_pii,
    contains_financial_data,
    contains_pci_data,
    masking_required,
    encryption_required,
    business_owner,
    data_steward,
    classification_reason
)

VALUES
(
    'dev_catalog',
    'silver',
    'payments',
    'transaction_amount',
    'FINANCIAL',
    'HIGH',
    FALSE,
    TRUE,
    FALSE,
    TRUE,
    TRUE,
    'Finance',
    'Payments Data Steward',
    'Payment transaction value'
);

INSERT INTO column_classification_registry
(
    catalog_name,
    schema_name,
    table_name,
    column_name,
    classification,
    sensitivity_level,
    contains_pii,
    masking_required,
    encryption_required,
    business_owner,
    data_steward,
    classification_reason
)

VALUES
(
    'dev_catalog',
    'silver',
    'payments',
    'customer_id',
    'PII',
    'HIGH',
    TRUE,
    TRUE,
    TRUE,
    'Finance',
    'Payments Data Steward',
    'Customer reference associated with payment'
);

-- ============================================================
-- 6. ORDER DATA CLASSIFICATION
-- ============================================================

INSERT INTO column_classification_registry
(
    catalog_name,
    schema_name,
    table_name,
    column_name,
    classification,
    sensitivity_level,
    contains_pii,
    masking_required,
    encryption_required,
    business_owner,
    data_steward,
    classification_reason
)

VALUES

(
    'dev_catalog',
    'silver',
    'orders',
    'order_id',
    'INTERNAL',
    'MEDIUM',
    FALSE,
    FALSE,
    FALSE,
    'Sales',
    'Sales Data Steward',
    'Unique order identifier'
),

(
    'dev_catalog',
    'silver',
    'orders',
    'customer_id',
    'PII',
    'HIGH',
    TRUE,
    TRUE,
    TRUE,
    'Sales',
    'Sales Data Steward',
    'Customer associated with order'
),

(
    'dev_catalog',
    'silver',
    'orders',
    'total_amount',
    'FINANCIAL',
    'HIGH',
    FALSE,
    TRUE,
    TRUE,
    'Finance',
    'Sales Data Steward',
    'Order monetary value'
);

