# Data Pipeline Monitoring & Operations

```text
Enterprise Data Pipeline Monitoring Framework
│
├── Source Systems
│      │
│      ├── Sales
│      ├── Customers
│      ├── Products
│      ├── Inventory
│      └── Stores
│
├── Landing Zone
│      │
│      └── Daily CSV Files
│
├── Airflow
│      │
│      ├── File Arrival Check
│      ├── Data Validation
│      ├── Load to Databricks
│      ├── Data Quality Check
│      ├── Monitoring
│      └── Notification
│
├── Databricks
│      │
│      ├── Bronze
│      ├── Silver
│      └── Gold
│
├── Monitoring Database
│      │
│      ├── Job Status
│      ├── Execution History
│      ├── SLA
│      ├── Alerts
│      └── Metrics
│
├── CloudWatch
│
├── Grafana Dashboard
│
└── Email Alerts
```

## Business Scenario:
Imagine a Retail Company.

Every night

500 Stores upload

Sales
Customer
Product
Inventory

files.

Every morning at 6 AM,

Business wants dashboards ready.

If

one file is missing
duplicate records exist
pipeline fails
cluster crashes
execution exceeds SLA

the support team must know immediately.

This is exactly where this project comes in.

## Tech Stack:

Apache Airflow
Databricks
PySpark
Python
SQL
AWS S3
CloudWatch
Grafana
Git

## Project Folder:

```text
PipelineMonitoring/

│
├── airflow_dags/
│      monitor_pipeline.py
│
├── validation/
│      validate_sales.py
│      validate_customer.py
│      duplicate_check.py
│      file_check.py
│
├── notebooks/
│      bronze.py
│      silver.py
│      gold.py
│
├── monitoring/
│      metrics.py
│      sla.py
│      alert.py
│
├── reports/
│      execution_report.sql
│      dashboard.sql
│
├── datasets/
│      sales/
│      customer/
│      inventory/
│
├── logs/
│
├── config/
│      config.json
│
└── README.md
```

## Data Flow:

```text
Stores

↓

CSV Files

↓

S3 Landing

↓

Airflow

↓

Validation

↓

Databricks Bronze

↓

Silver

↓

Gold

↓

Dashboard

↓

Monitoring

↓

Alerts
```

## Data (daily file drops):
Sales.csv, Customer.csv, Product.csv, Inventory.csv In a real enterprise every day these files arrive in S3.

## Inside Airflow DAG:
```text
Check Files

↓

Validate Files

↓

Load Bronze

↓

Load Silver

↓

Load Gold

↓

Data Quality

↓

Generate Metrics

↓

Email
```
If any file is missing, Airflow should fail immediately.

