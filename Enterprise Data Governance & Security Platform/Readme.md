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

## AWS S3 Data Lake Structure
```text
s3://abc-retail-governance/

│
├── dev/
│
│   ├── bronze/
│   │
│   │   ├── customers/
│   │   ├── orders/
│   │   ├── employees/
│   │   ├── products/
│   │   ├── payments/
│   │   └── stores/
│   │
│   ├── silver/
│   │
│   └── gold/
│
├── test/
│
├── prod/
│
├── audit/
│
├── metadata/
│
├── logs/
│
├── archive/
│
└── quarantine/
```

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

