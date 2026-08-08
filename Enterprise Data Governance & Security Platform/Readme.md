# Enterprise Data Governance & Security Platform

In projects, Data Engineers don't start by creating tables or writing PySpark jobs.
The first few weeks are spent understanding the business problem, existing architecture, compliance requirements, and identifying sensitive data.

Imagin every day, data is generated from all these systems:
```text
Store POS
      \
CRM ----\
ERP ----- > AWS S3 ---> Databricks ---> Snowflake ---> Power BI
Finance -/
Website /
Mobile /
```
Every system sends data to the company's Data Lake. The Data Lake already contains over 8 petabytes of data.

## Existing Problems:
- Anyone Can Read Customer Data
- No Data Classification (Which tables contain PII?, Which contain financial data?)
- No Metadata (Where is Customer Lifetime Value stored?, Which table contains customer email?)
- Duplicate Tables (Different teams created duplicate datasets)
- No Lineage
- Compliance Failure (In GDPR audit, auditor asks to show everyone who accessed customer data during the last 90 days)

So the Management decides to build a centralized Data Governance Platform. 
Only authorized users can access sensitive data.
Access should be based on business roles.
Metadata management, Data Lineage and Every action must be logged(audited).
Each column must be classified like below.
```text
| Column      | Classification    |
| ----------- | ----------------- |
| Customer_ID | Internal          |
| Name        | Confidential      |
| Email       | PII               |
| Phone       | PII               |
| Salary      | Restricted        |
| Credit Card | Highly Restricted |
```
## High-Level Architecture:
```text
                Source Systems
       --------------------------------
       CRM
       ERP
       POS
       HR
       Finance
       Ecommerce
       --------------------------------
                 |
                 v
              AWS S3
                 |
                 v
          Databricks Workspace
                 |
        -----------------------
        Unity Catalog
        -----------------------
                 |
                 v
      Bronze --> Silver --> Gold
                 |
                 v
            Delta Lake
                 |
      --------------------------
      Governance Services
      - RBAC
      - Metadata
      - Classification
      - Lineage
      - Audit
      - Data Quality
      --------------------------
                 |
                 v
            Snowflake
                 |
                 v
         Power BI / Tableau
```

## Repository Folder Structure:
```text
enterprise-data-governance-platform/
│
├── README.md
├── requirements.txt
├── .gitignore
│
├── configs/
│   ├── config_dev.yml
│   ├── config_test.yml
│   ├── config_prod.yml
│   └── governance_config.yml
│
├── src/
│   │
│   ├── common/
│   │   ├── config_loader.py
│   │   ├── logger.py
│   │   ├── spark_utils.py
│   │   └── date_utils.py
│   │
│   ├── ingestion/
│   │   ├── common_ingestion.py
│   │   ├── file_reader.py
│   │   ├── schema_manager.py
│   │   └── audit_manager.py
│   │
│   ├── validation/
│   │   ├── null_validation.py
│   │   ├── duplicate_validation.py
│   │   ├── schema_validation.py
│   │   ├── referential_validation.py
│   │   └── validation_runner.py
│   │
│   ├── governance/
│   │   ├── metadata_manager.py
│   │   ├── classification_manager.py
│   │   ├── tag_manager.py
│   │   ├── lineage_manager.py
│   │   └── policy_manager.py
│   │
│   ├── security/
│   │   ├── rbac_manager.py
│   │   ├── masking_manager.py
│   │   ├── row_filter_manager.py
│   │   └── permission_manager.py
│   │
│   └── audit/
│       ├── access_audit.py
│       ├── permission_audit.py
│       └── audit_report.py
│
├── notebooks/
│   │
│   ├── 01_bronze/
│   │   ├── 01_load_customers
│   │   ├── 02_load_orders
│   │   ├── 03_load_payments
│   │   ├── 04_load_employees
│   │   ├── 05_load_products
│   │   ├── 06_load_stores
│   │   └── 07_load_customer_segments
│   │
│   ├── 02_silver/
│   │   ├── 01_clean_customers
│   │   ├── 02_clean_orders
│   │   ├── 03_clean_payments
│   │   ├── 04_clean_employees
│   │   └── 05_clean_products
│   │
│   ├── 03_gold/
│   │   ├── 01_customer_360
│   │   ├── 02_daily_sales
│   │   ├── 03_customer_lifetime_value
│   │   └── 04_finance_summary
│   │
│   ├── 04_governance/
│   │   ├── 01_register_metadata
│   │   ├── 02_classify_columns
│   │   ├── 03_apply_tags
│   │   ├── 04_lineage_validation
│   │   └── 05_governance_quality_check
│   │
│   ├── 05_security/
│   │   ├── 01_create_groups
│   │   ├── 02_apply_permissions
│   │   ├── 03_apply_masking
│   │   └── 04_apply_row_security
│   │
│   └── 06_audit/
│       ├── 01_access_audit
│       ├── 02_permission_audit
│       └── 03_compliance_report
│
├── sql/
│   │
│   ├── 01_catalog/
│   │   ├── create_catalogs.sql
│   │   ├── create_schemas.sql
│   │   └── create_external_locations.sql
│   │
│   ├── 02_tables/
│   │   ├── create_governance_tables.sql
│   │   └── create_audit_tables.sql
│   │
│   ├── 03_security/
│   │   ├── grants.sql
│   │   ├── masking_policies.sql
│   │   └── row_filters.sql
│   │
│   ├── 04_validation/
│   │   ├── customer_checks.sql
│   │   ├── order_checks.sql
│   │   └── governance_checks.sql
│   │
│   └── 05_reporting/
│       ├── audit_report.sql
│       ├── permission_report.sql
│       └── data_quality_report.sql
│
├── schemas/
│   ├── customer_schema.py
│   ├── order_schema.py
│   ├── payment_schema.py
│   ├── employee_schema.py
│   ├── product_schema.py
│   └── store_schema.py
│
├── tests/
│   ├── test_ingestion.py
│   ├── test_validation.py
│   └── test_governance.py
│
├── airflow/
│   ├── governance_pipeline_dag.py
│   ├── ingestion_dag.py
│   └── audit_dag.py
│
├── monitoring/
│   ├── metrics.py
│   ├── cloudwatch.py
│   └── alerts.py
│
├── terraform/
│   ├── aws/
│   ├── databricks/
│   └── snowflake/
│
└── documentation/
    ├── architecture.md
    ├── data_dictionary.md
    ├── governance_policy.md
    ├── security_model.md
    ├── access_request_process.md
    └── audit_runbook.md
```

## My Data Lake Design

The AWS S3 bucket is organized by environment and data layer.
```text
s3://abc-retail-data-platform/

├── dev/
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
├── test/
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
├── prod/
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
├── logs/
├── audit/
├── metadata/
├── backups/
└── archive/
```

## My Unity Catalog Design

Instead of one large catalog, we separate by environment.
```text
Catalogs

dev_catalog
│
├── bronze
├── silver
└── gold

test_catalog
│
├── bronze
├── silver
└── gold

prod_catalog
│
├── bronze
├── silver
└── gold
```

## Security Design (High Level)

The platform uses Role-Based Access Control (RBAC).
```text
| Role             | Customer Data     | Finance Data | HR Data     |
| ---------------- | ----------------- | ------------ | ----------- |
| Data Engineer    | Read              | Read         | No Access   |
| HR Analyst       | No Access         | No Access    | Read        |
| Finance Analyst  | No Access         | Read         | No Access   |
| Sales Analyst    | Read (Masked PII) | No Access    | No Access   |
| Governance Admin | Full Access       | Full Access  | Full Access |
```
## Governance Standards

Before implementation begins, the team defines standards. Like naming conventions, Data Ownership, 

## Governance Data classification
Example customers.csv
```text
customer_id       → Internal
customer_name     → PII
email_address     → PII
phone_number      → PII
date_of_birth     → PII
address_line1     → PII
pan_number        → Highly Restricted
loyalty_tier      → Confidential
customer_status   → Internal
```
Later we'll use these classifications to determine who can see which columns.

## Governance Data Dictionary
Before loading anything into Databricks, we should document the datasets.
```text
| Dataset   | Domain     | Sensitivity         | Owner           |
| --------- | ---------- | ------------------- | --------------- |
| customers | Customer   | Highly Confidential | Customer Team   |
| orders    | Sales      | Confidential        | Sales Team      |
| payments  | Finance    | Highly Restricted   | Finance Team    |
| employees | HR         | Highly Restricted   | HR Team         |
| products  | Product    | Internal            | Product Team    |
| stores    | Operations | Internal            | Operations Team |
```

