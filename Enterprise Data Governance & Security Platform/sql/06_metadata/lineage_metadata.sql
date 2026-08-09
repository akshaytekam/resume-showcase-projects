-- ============================================================
-- File        : lineage_metadata.sql
-- Project     : Enterprise Data Governance Platform
-- Purpose     : Enterprise data lineage metadata
-- ============================================================

USE CATALOG dev_catalog;

USE SCHEMA governance;

-- ============================================================
-- 1. LINEAGE REGISTRY
-- ============================================================

CREATE TABLE IF NOT EXISTS data_lineage_registry (

    lineage_id BIGINT
        GENERATED ALWAYS AS IDENTITY,

    source_catalog STRING,

    source_schema STRING,

    source_table STRING,

    source_column STRING,

    transformation_type STRING,

    transformation_logic STRING,

    target_catalog STRING,

    target_schema STRING,

    target_table STRING,

    target_column STRING,

    downstream_system STRING,

    downstream_asset STRING,

    business_domain STRING,

    pipeline_name STRING,

    pipeline_owner STRING,

    lineage_type STRING,

    criticality STRING,

    is_active BOOLEAN DEFAULT TRUE,

    created_at TIMESTAMP
        DEFAULT CURRENT_TIMESTAMP(),

    updated_at TIMESTAMP
        DEFAULT CURRENT_TIMESTAMP()

)
USING DELTA;

-- ============================================================
-- 2. CUSTOMER TABLE LINEAGE
-- ============================================================

INSERT INTO data_lineage_registry
(
    source_catalog,
    source_schema,
    source_table,
    transformation_type,
    transformation_logic,
    target_catalog,
    target_schema,
    target_table,
    downstream_system,
    downstream_asset,
    business_domain,
    pipeline_name,
    pipeline_owner,
    lineage_type,
    criticality
)

VALUES
(
    'external',
    'crm',
    'customers',
    'INGESTION',
    'CRM customer data ingested into AWS S3 and loaded into Delta Bronze.',
    'dev_catalog',
    'bronze',
    'customers',
    'Databricks',
    'bronze.customers',
    'Customer',
    'customer_ingestion_pipeline',
    'Data Engineering',
    'TABLE',
    'HIGH'
);

INSERT INTO data_lineage_registry
(
    source_catalog,
    source_schema,
    source_table,
    transformation_type,
    transformation_logic,
    target_catalog,
    target_schema,
    target_table,
    downstream_system,
    downstream_asset,
    business_domain,
    pipeline_name,
    pipeline_owner,
    lineage_type,
    criticality
)

VALUES
(
    'dev_catalog',
    'bronze',
    'customers',
    'TRANSFORMATION',
    'Schema validation, null handling, duplicate removal and standardization.',
    'dev_catalog',
    'silver',
    'customers',
    'Databricks',
    'silver.customers',
    'Customer',
    'customer_silver_transformation',
    'Data Engineering',
    'TABLE',
    'HIGH'
);

INSERT INTO data_lineage_registry
(
    source_catalog,
    source_schema,
    source_table,
    transformation_type,
    transformation_logic,
    target_catalog,
    target_schema,
    target_table,
    downstream_system,
    downstream_asset,
    business_domain,
    pipeline_name,
    pipeline_owner,
    lineage_type,
    criticality
)

VALUES
(
    'dev_catalog',
    'silver',
    'customers',
    'GOVERNANCE_TRANSFORMATION',
    'Column masking and governed access applied to customer data.',
    'dev_catalog',
    'gold',
    'secure_customers',
    'Databricks',
    'gold.secure_customers',
    'Customer',
    'customer_governance_pipeline',
    'Data Governance Engineering',
    'TABLE',
    'HIGH'
);

-- ============================================================
-- 3. CUSTOMER COLUMN LINEAGE
-- ============================================================

INSERT INTO data_lineage_registry
(
    source_catalog,
    source_schema,
    source_table,
    source_column,
    transformation_type,
    transformation_logic,
    target_catalog,
    target_schema,
    target_table,
    target_column,
    downstream_system,
    business_domain,
    pipeline_name,
    pipeline_owner,
    lineage_type,
    criticality
)

VALUES
(
    'dev_catalog',
    'silver',
    'customers',
    'customer_id',
    'DIRECT',
    'Direct column mapping with governance masking.',
    'dev_catalog',
    'gold',
    'secure_customers',
    'customer_id',
    'Databricks',
    'Customer',
    'customer_governance_pipeline',
    'Data Governance Engineering',
    'COLUMN',
    'HIGH'
);

INSERT INTO data_lineage_registry
(
    source_catalog,
    source_schema,
    source_table,
    source_column,
    transformation_type,
    transformation_logic,
    target_catalog,
    target_schema,
    target_table,
    target_column,
    downstream_system,
    business_domain,
    pipeline_name,
    pipeline_owner,
    lineage_type,
    criticality
)

VALUES
(
    'dev_catalog',
    'silver',
    'customers',
    'email',
    'MASKING',
    'mask_email(email) applied based on user authorization.',
    'dev_catalog',
    'gold',
    'secure_customers',
    'email',
    'Databricks',
    'Customer',
    'customer_governance_pipeline',
    'Data Governance Engineering',
    'COLUMN',
    'HIGH'
);

INSERT INTO data_lineage_registry
(
    source_catalog,
    source_schema,
    source_table,
    source_column,
    transformation_type,
    transformation_logic,
    target_catalog,
    target_schema,
    target_table,
    target_column,
    downstream_system,
    business_domain,
    pipeline_name,
    pipeline_owner,
    lineage_type,
    criticality
)

VALUES
(
    'dev_catalog',
    'silver',
    'customers',
    'phone_number',
    'MASKING',
    'mask_phone(phone_number) applied based on user authorization.',
    'dev_catalog',
    'gold',
    'secure_customers',
    'phone_number',
    'Databricks',
    'Customer',
    'customer_governance_pipeline',
    'Data Governance Engineering',
    'COLUMN',
    'HIGH'
);

-- ============================================================
-- 4. PAYMENT LINEAGE
-- ============================================================

INSERT INTO data_lineage_registry
(
    source_catalog,
    source_schema,
    source_table,
    transformation_type,
    transformation_logic,
    target_catalog,
    target_schema,
    target_table,
    downstream_system,
    downstream_asset,
    business_domain,
    pipeline_name,
    pipeline_owner,
    lineage_type,
    criticality
)

VALUES
(
    'external',
    'payment_gateway',
    'transactions',
    'INGESTION',
    'Payment gateway transaction data ingested into Bronze Delta.',
    'dev_catalog',
    'bronze',
    'payments',
    'Databricks',
    'bronze.payments',
    'Finance',
    'payment_ingestion_pipeline',
    'Data Engineering',
    'TABLE',
    'CRITICAL'
);

INSERT INTO data_lineage_registry
(
    source_catalog,
    source_schema,
    source_table,
    transformation_type,
    transformation_logic,
    target_catalog,
    target_schema,
    target_table,
    downstream_system,
    downstream_asset,
    business_domain,
    pipeline_name,
    pipeline_owner,
    lineage_type,
    criticality
)

VALUES
(
    'dev_catalog',
    'bronze',
    'payments',
    'TRANSFORMATION',
    'Schema validation, duplicate removal, currency normalization and payment status standardization.',
    'dev_catalog',
    'silver',
    'payments',
    'Databricks',
    'silver.payments',
    'Finance',
    'payment_silver_transformation',
    'Data Engineering',
    'TABLE',
    'CRITICAL'
);

INSERT INTO data_lineage_registry
(
    source_catalog,
    source_schema,
    source_table,
    transformation_type,
    transformation_logic,
    target_catalog,
    target_schema,
    target_table,
    downstream_system,
    downstream_asset,
    business_domain,
    pipeline_name,
    pipeline_owner,
    lineage_type,
    criticality
)

VALUES
(
    'dev_catalog',
    'silver',
    'payments',
    'GOVERNANCE_TRANSFORMATION',
    'Country-based row-level security and financial/PCI column masking.',
    'dev_catalog',
    'gold',
    'secure_payments',
    'Databricks',
    'gold.secure_payments',
    'Finance',
    'payment_governance_pipeline',
    'Data Governance Engineering',
    'TABLE',
    'CRITICAL'
);

INSERT INTO data_lineage_registry
(
    source_catalog,
    source_schema,
    source_table,
    source_column,
    transformation_type,
    transformation_logic,
    target_catalog,
    target_schema,
    target_table,
    target_column,
    downstream_system,
    downstream_asset,
    business_domain,
    pipeline_name,
    pipeline_owner,
    lineage_type,
    criticality
)

VALUES
(
    'dev_catalog',
    'silver',
    'payments',
    'card_last4',
    'MASKING',
    'mask_card_last4(card_last4) applied based on security group.',
    'dev_catalog',
    'gold',
    'secure_payments',
    'card_last4',
    'Power BI',
    'Finance Payment Dashboard',
    'Finance',
    'payment_governance_pipeline',
    'Data Governance Engineering',
    'COLUMN',
    'CRITICAL'
);

INSERT INTO data_lineage_registry
(
    source_catalog,
    source_schema,
    source_table,
    source_column,
    transformation_type,
    transformation_logic,
    target_catalog,
    target_schema,
    target_table,
    target_column,
    downstream_system,
    downstream_asset,
    business_domain,
    pipeline_name,
    pipeline_owner,
    lineage_type,
    criticality
)

VALUES
(
    'dev_catalog',
    'silver',
    'payments',
    'transaction_amount',
    'MASKING',
    'mask_financial_amount(transaction_amount) applied according to finance authorization.',
    'dev_catalog',
    'gold',
    'secure_payments',
    'transaction_amount',
    'Power BI',
    'Finance Payment Dashboard',
    'Finance',
    'payment_governance_pipeline',
    'Data Governance Engineering',
    'COLUMN',
    'CRITICAL'
);

-- ============================================================
-- 5. CROSS-PLATFORM LINEAGE
-- ============================================================

INSERT INTO data_lineage_registry
(
    source_catalog,
    source_schema,
    source_table,
    transformation_type,
    transformation_logic,
    target_catalog,
    target_schema,
    target_table,
    downstream_system,
    downstream_asset,
    business_domain,
    pipeline_name,
    pipeline_owner,
    lineage_type,
    criticality
)

VALUES
(
    'dev_catalog',
    'gold',
    'secure_customers',
    'DATA_SHARE',
    'Governed customer dataset published to Snowflake for enterprise analytics.',
    'snowflake_prod',
    'analytics',
    'customers',
    'Snowflake',
    'analytics.customers',
    'Customer',
    'snowflake_customer_publish',
    'Data Platform Engineering',
    'CROSS_PLATFORM',
    'HIGH'
);

INSERT INTO data_lineage_registry
(
    source_catalog,
    source_schema,
    source_table,
    transformation_type,
    transformation_logic,
    target_catalog,
    target_schema,
    target_table,
    downstream_system,
    downstream_asset,
    business_domain,
    pipeline_name,
    pipeline_owner,
    lineage_type,
    criticality
)

VALUES
(
    'snowflake_prod',
    'analytics',
    'customers',
    'REPORTING',
    'Power BI semantic model consumes governed customer analytics data.',
    'powerbi',
    'semantic_model',
    'customer_analytics',
    'Power BI',
    'Customer Analytics Dashboard',
    'Customer',
    'powerbi_customer_refresh',
    'BI Engineering',
    'DOWNSTREAM',
    'HIGH'
);

