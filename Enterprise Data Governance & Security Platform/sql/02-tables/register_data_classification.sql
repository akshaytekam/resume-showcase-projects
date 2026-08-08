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

-- ============================================================
-- 4. Column Classification Registry
-- ============================================================

CREATE TABLE IF NOT EXISTS column_classification_registry (

    classification_rule_id BIGINT
        GENERATED ALWAYS AS IDENTITY,

    catalog_name STRING NOT NULL,

    schema_name STRING NOT NULL,

    table_name STRING NOT NULL,

    column_name STRING NOT NULL,

    classification_name STRING NOT NULL,

    business_description STRING,

    masking_required BOOLEAN,

    row_filter_required BOOLEAN,

    data_owner STRING,

    compliance_requirement STRING,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),

    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()

)
USING DELTA;

-- ============================================================
-- 5. CUSTOMER CLASSIFICATION
-- ============================================================

INSERT INTO column_classification_registry
(
    catalog_name,
    schema_name,
    table_name,
    column_name,
    classification_name,
    business_description,
    masking_required,
    row_filter_required,
    data_owner,
    compliance_requirement
)

VALUES

(
    'dev_catalog',
    'bronze',
    'customers',
    'customer_id',
    'INTERNAL',
    'Unique identifier assigned to each customer',
    FALSE,
    FALSE,
    'Customer Data Team',
    NULL
),

(
    'dev_catalog',
    'bronze',
    'customers',
    'first_name',
    'PII',
    'Customer first name',
    TRUE,
    FALSE,
    'Customer Data Team',
    'Privacy
'
),

(
    'dev_catalog',
    'bronze',
    'customers',
    'last_name',
    'PII',
    'Customer last name',
    TRUE,
    FALSE,
    'Customer Data Team',
    'Privacy'
),

(
    'dev_catalog',
    'bronze',
    'customers',
    'email',
    'PII',
    'Customer email address',
    TRUE,
    FALSE,
    'Customer Data Team',
    'Privacy'
),

(
    'dev_catalog',
    'bronze',
    'customers',
    'phone_number',
    'PII',
    'Customer telephone number',
    TRUE,
    FALSE,
    'Customer Data Team',
    'Privacy'
),

(
    'dev_catalog',
    'bronze',
    'customers',
    'date_of_birth',
    'SENSITIVE',
    'Customer date of birth',
    TRUE,
    FALSE,
    'Customer Data Team',
    'Privacy'
),

(
    'dev_catalog',
    'bronze',
    'customers',
    'city',
    'INTERNAL',
    'Customer city',
    FALSE,
    FALSE,
    'Customer Data Team',
    NULL
),

(
    'dev_catalog',
    'bronze',
    'customers',
    'state',
    'INTERNAL',
    'Customer state',
    FALSE,
    FALSE,
    'Customer Data Team',
    NULL
);

-- ============================================================
-- 6. EMPLOYEE CLASSIFICATION
-- ============================================================

INSERT INTO column_classification_registry
(
    catalog_name,
    schema_name,
    table_name,
    column_name,
    classification_name,
    business_description,
    masking_required,
    row_filter_required,
    data_owner,
    compliance_requirement
)

VALUES

(
    'dev_catalog',
    'bronze',
    'employees',
    'employee_id',
    'INTERNAL',
    'Unique employee identifier',
    FALSE,
    FALSE,
    'HR Data Team',
    NULL
),

(
    'dev_catalog',
    'bronze',
    'employees',
    'first_name',
    'PII',
    'Employee first name',
    TRUE,
    FALSE,
    'HR Data Team',
    'Privacy'
),

(
    'dev_catalog',
    'bronze',
    'employees',
    'last_name',
    'PII',
    'Employee last name',
    TRUE,
    FALSE,
    'HR Data Team',
    'Privacy'
),

(
    'dev_catalog',
    'bronze',
    'employees',
    'email',
    'PII',
    'Employee corporate email',
    TRUE,
    FALSE,
    'HR Data Team',
    'Privacy'
),

(
    'dev_catalog',
    'bronze',
    'employees',
    'department',
    'INTERNAL',
    'Employee department',
    FALSE,
    TRUE,
    'HR Data Team',
    NULL
),

(
    'dev_catalog',
    'bronze',
    'employees',
    'job_title',
    'INTERNAL',
    'Employee job title',
    FALSE,
    FALSE,
    'HR Data Team',
    NULL
),

(
    'dev_catalog',
    'bronze',
    'employees',
    'annual_salary',
    'FINANCIAL',
    'Employee annual compensation',
    TRUE,
    TRUE,
    'HR Data Team',
    'Financial'
),

(
    'dev_catalog',
    'bronze',
    'employees',
    'manager_id',
    'INTERNAL',
    'Employee manager identifier',
    FALSE,
    FALSE,
    'HR Data Team',
    NULL
),

(
    'dev_catalog',
    'bronze',
    'employees',
    'joining_date',
    'INTERNAL',
    'Employee joining date',
    FALSE,
    FALSE,
    'HR Data Team',
    NULL
),

(
    'dev_catalog',
    'bronze',
    'employees',
    'employment_status',
    'CONFIDENTIAL',
    'Current employment status',
    FALSE,
    TRUE,
    'HR Data Team',
    'HR'
);

-- ============================================================
-- 7. PAYMENT CLASSIFICATION
-- ============================================================

INSERT INTO column_classification_registry
(
    catalog_name,
    schema_name,
    table_name,
    column_name,
    classification_name,
    business_description,
    masking_required,
    row_filter_required,
    data_owner,
    compliance_requirement
)

VALUES

(
    'dev_catalog',
    'bronze',
    'payments',
    'payment_id',
    'INTERNAL',
    'Unique payment transaction identifier',
    FALSE,
    FALSE,
    'Finance Data Team',
    NULL
),

(
    'dev_catalog',
    'bronze',
    'payments',
    'order_id',
    'INTERNAL',
    'Associated order identifier',
    FALSE,
    FALSE,
    'Finance Data Team',
    NULL
),

(
    'dev_catalog',
    'bronze',
    'payments',
    'customer_id',
    'PII',
    'Customer associated with payment',
    TRUE,
    TRUE,
    'Finance Data Team',
    'Privacy'
),

(
    'dev_catalog',
    'bronze',
    'payments',
    'payment_method',
    'CONFIDENTIAL',
    'Payment method used for transaction',
    FALSE,
    FALSE,
    'Finance Data Team',
    NULL
),

(
    'dev_catalog',
    'bronze',
    'payments',
    'payment_status',
    'INTERNAL',
    'Payment processing status',
    FALSE,
    FALSE,
    'Finance Data Team',
    NULL
),

(
    'dev_catalog',
    'bronze',
    'payments',
    'transaction_amount',
    'FINANCIAL',
    'Payment transaction amount',
    TRUE,
    TRUE,
    'Finance Data Team',
    'Financial'
),

(
    'dev_catalog',
    'bronze',
    'payments',
    'currency',
    'INTERNAL',
    'Transaction currency',
    FALSE,
    FALSE,
    'Finance Data Team',
    NULL
),

(
    'dev_catalog',
    'bronze',
    'payments',
    'card_last4',
    'SENSITIVE',
    'Last four digits of payment card',
    TRUE,
    TRUE,
    'Finance Data Team',
    'Payment Security'
),

(
    'dev_catalog',
    'bronze',
    'payments',
    'payment_provider',
    'CONFIDENTIAL',
    'Payment service provider',
    FALSE,
    FALSE,
    'Finance Data Team',
    NULL
);

-- ============================================================
-- 8. PRODUCT CLASSIFICATION
-- ============================================================

INSERT INTO column_classification_registry
(
    catalog_name,
    schema_name,
    table_name,
    column_name,
    classification_name,
    business_description,
    masking_required,
    row_filter_required,
    data_owner,
    compliance_requirement
)

VALUES

(
    'dev_catalog',
    'bronze',
    'products',
    'product_id',
    'INTERNAL',
    'Unique product identifier',
    FALSE,
    FALSE,
    'Product Data Team',
    NULL
),

(
    'dev_catalog',
    'bronze',
    'products',
    'product_name',
    'INTERNAL',
    'Product name',
    FALSE,
    FALSE,
    'Product Data Team',
    NULL
),

(
    'dev_catalog',
    'bronze',
    'products',
    'category',
    'INTERNAL',
    'Product category',
    FALSE,
    FALSE,
    'Product Data Team',
    NULL
),

(
    'dev_catalog',
    'bronze',
    'products',
    'unit_price',
    'FINANCIAL',
    'Retail selling price',
    TRUE,
    FALSE,
    'Product Data Team',
    'Financial'
),

(
    'dev_catalog',
    'bronze',
    'products',
    'unit_cost',
    'CONFIDENTIAL',
    'Internal product acquisition cost',
    TRUE,
    TRUE,
    'Product Data Team',
    'Commercial Confidentiality'
);

-- ============================================================
-- 9. STORE CLASSIFICATION
-- ============================================================

INSERT INTO column_classification_registry
(
    catalog_name,
    schema_name,
    table_name,
    column_name,
    classification_name,
    business_description,
    masking_required,
    row_filter_required,
    data_owner,
    compliance_requirement
)

VALUES

(
    'dev_catalog',
    'bronze',
    'stores',
    'store_id',
    'INTERNAL',
    'Unique store identifier',
    FALSE,
    FALSE,
    'Retail Operations',
    NULL
),

(
    'dev_catalog',
    'bronze',
    'stores',
    'store_name',
    'INTERNAL',
    'Store name',
    FALSE,
    FALSE,
    'Retail Operations',
    NULL
),

(
    'dev_catalog',
    'bronze',
    'stores',
    'city',
    'INTERNAL',
    'Store city',
    FALSE,
    FALSE,
    'Retail Operations',
    NULL
),

(
    'dev_catalog',
    'bronze',
    'stores',
    'state',
    'INTERNAL',
    'Store state',
    FALSE,
    FALSE,
    'Retail Operations',
    NULL
),

(
    'dev_catalog',
    'bronze',
    'stores',
    'region',
    'INTERNAL',
    'Business region',
    FALSE,
    TRUE,
    'Retail Operations',
    NULL
),

(
    'dev_catalog',
    'bronze',
    'stores',
    'manager_id',
    'INTERNAL',
    'Store manager employee identifier',
    FALSE,
    FALSE,
    'Retail Operations',
    NULL
);

-- ============================================================
-- 10. ORDER CLASSIFICATION
-- ============================================================

INSERT INTO column_classification_registry
(
    catalog_name,
    schema_name,
    table_name,
    column_name,
    classification_name,
    business_description,
    masking_required,
    row_filter_required,
    data_owner,
    compliance_requirement
)

VALUES

(
    'dev_catalog',
    'bronze',
    'orders',
    'order_id',
    'INTERNAL',
    'Unique order identifier',
    FALSE,
    FALSE,
    'Sales Data Team',
    NULL
),

(
    'dev_catalog',
    'bronze',
    'orders',
    'customer_id',
    'PII',
    'Customer associated with order',
    TRUE,
    TRUE,
    'Sales Data Team',
    'Privacy'
),

(
    'dev_catalog',
    'bronze',
    'orders',
    'store_id',
    'INTERNAL',
    'Store associated with order',
    FALSE,
    TRUE,
    'Sales Data Team',
    NULL
),

(
    'dev_catalog',
    'bronze',
    'orders',
    'order_date',
    'INTERNAL',
    'Date order was placed',
    FALSE,
    FALSE,
    'Sales Data Team',
    NULL
),

(
    'dev_catalog',
    'bronze',
    'orders',
    'total_amount',
    'FINANCIAL',
    'Total monetary value of order',
    TRUE,
    TRUE,
    'Finance Data Team',
    'Financial'
),

(
    'dev_catalog',
    'bronze',
    'orders',
    'order_status',
    'INTERNAL',
    'Current order status',
    FALSE,
    FALSE,
    'Sales Data Team',
    NULL
);

