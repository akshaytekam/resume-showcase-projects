## Build it step by step, including:

- Business requirement
- Folder structure
- Data
- PySpark code
- Delta Lake
- Snowflake loading
- Airflow DAG
- Power BI

## Project Overview

Company: I work for a retail company called RetailMart.

RetailMart has:

- 500 stores
- 15 million customers
- Around 8 million sales transactions daily

Every night, each store sends its sales file.

for examples,
Store_001_2026-07-05.csv,
Store_002_2026-07-05.csv,
Store_003_2026-07-05.csv,
...
Store_500_2026-07-05.csv

Each file is uploaded automatically to AWS S3.

Our responsibility is to prepare clean data before 6 AM.

Business users use Power BI dashboards every morning.
 ----------------------------------------
 Architectural Workflow From source to s3 (AWS Glue Batch Ingestion Architecture):
```text
 ┌────────────────────────┐
 │   Source Database      │ (e.g., Amazon RDS / On-Premises Mysql)
 └───────────┬────────────┘
             │
             ▼ (Secure internal network access)
 ┌────────────────────────┐
 │  AWS Glue Connection   │ (Enforces VPC Subnet & Security Group rules)
 └───────────┬────────────┘
             │
             ▼ (Nightly scheduled trigger)
 ┌────────────────────────┐
 │   AWS Glue ETL Job     │ <── Orchestrated by AWS Glue Workflow / EventBridge
 └───────────┬────────────┘
             │
             ▼ (Executes Serverless Spark / Python script)
 ┌────────────────────────────────────────────────────────┐
 │               Target Amazon S3 Data Lake               │
 │                                                        │
 │  s3://retail-sales-for-retailmart/raw/sales/           │
 │    ├── /year=2026/                                     │
 │    │    └── /month=07/                                 │
 │    │         └── /day=09/                              │
 │    │              └── part-00000-data.parquet          │
 └────────────────────────────────────────────────────────┘

```

## How the Partitioning Logic Works (Zero Code)

1. The Source & Network Connection
   The Source:
   A transactional database holding the day's sales or application logs.
   AWS Glue Connection:
   A network configuration object within AWS Glue. It securely connects Glue's serverless environment into your specific VPC subnets, allowing it to communicate with your database through private IPs.

2. The Orchestration Layer
   AWS Glue Workflow (or Amazon EventBridge):
   A time-based cron trigger fires automatically every night (e.g., at 12:00 AM). It kicks off the Glue ETL job.

3. The Serverless ETL Processing Layer
   AWS Glue ETL Job:
   A managed PySpark or Python shell script that provisions compute workers on demand. It connects to the database, extracts the newest records, and partitions them on the fly.

4. The S3 Storage Layer
   Target Path:
   The Glue script writes data directly into your bucket layout: s3://retail-sales-for-retailmart/raw/sales/.
   Automatic Partitioning:
   The Glue job uses Spark's native partitioning code to organize the files dynamically based on the current run date (year=2026/month=07/day=09/).
   File Format Optimization:
   While copying, Glue can convert raw row-based data into optimized formats like Apache Parquet or ORC for faster subsequent downstream queries.

 ----------------------------------------

## High-Level Architecture
```text
                500 Stores
                     │
                     ▼
              CSV Files Generated
                     │
                     ▼
              AWS S3 Landing Zone
                     │
                     ▼
        Databricks (PySpark Batch Job)
                     │
     ┌───────────────┼────────────────┐
     │               │                │
 Data Validation  Data Cleaning  Transformations
     │               │                │
     └───────────────┼────────────────┘
                     ▼
               Delta Lake (Silver)
                     │
                     ▼
           Snowflake Data Warehouse
                     │
                     ▼
              Power BI Dashboard
```

Store Generates Sales Data. Each store creates a CSV.

## Upload to AWS S3

Landing Bucket: s3://retail-sales-for-retailmart/raw/sales/

This is the Raw Layer.

<img width="1172" height="656" alt="bucket1" src="https://github.com/user-attachments/assets/8a85f2cc-b3d2-4cad-bc64-e9b3d8aedb0f" />

## Databricks Reads Files


## project structure is:

```text
RetailBatchPipeline/
│
├── data/
│   ├── raw/
│   │   ├── sales/
│   │   │   ├── Store001_2026-07-05.csv
│   │   │   ├── Store002_2026-07-05.csv
│   │   │   ├── Store003_2026-07-05.csv
│   │   │   └── ...
│   │
│   ├── lookup/
│   │   ├── products.csv
│   │   ├── stores.csv
│   │   └── customers.csv
│   │
│   └── rejected/
│
├── notebooks/
├── scripts/
├── airflow/
└── README.md
```

## Sales Files (Raw Data)

Columns:

```text
| Column           | Description               |
| ---------------- | ------------------------- |
| transaction_id   | Unique transaction ID     |
| store_id         | Store identifier          |
| customer_id      | Customer ID               |
| product_id       | Product sold              |
| quantity         | Units sold                |
| unit_price       | Price per unit            |
| transaction_time | Timestamp                 |
| payment_method   | Cash/Card/UPI             |
| cashier_id       | Cashier handling the sale |
```

## Product, Store, Customers Lookup Table

This acts as a Dimension table. (products.csv, stores.csv, customers.csv)

## Bad Records

Production systems always contain bad data. (Duplicates, Null, Invalid reords, missing values...etc)

## S3 Bucket Structure

Your S3 bucket could look like this:

```text
retail-sales-data/
│
├── raw/
│   └── sales/
│       └── 2026/
│           └── 07/
│               └── 05/
│                   ├── Store001_2026-07-05.csv
│                   ├── Store002_2026-07-05.csv
│                   ├── Store003_2026-07-05.csv
│                   ├── Store004_2026-07-05.csv
│                   └── Store005_2026-07-05.csv
│
├── lookup/
│   ├── customers.csv
│   ├── products.csv
│   └── stores.csv
│
├── rejected/
│
├── processed/
│
└── archive/
```

## Basic Validation
always validates before processing. Check required columns.
   1. Data Quality Enforcement (Delta Live Tables Expectations)
   2. Schema Enforcement & Evolution (Delta Lake Layer)
   3. Structural Constraints (Delta Table Constraints)
   4. Third-party Advanced Programmatic Validation (Great Expectations / Pandera)

## Airflow Scheduling
```text
Every Night

12:30 AM

↓

Run pipeline

↓

Finish before 5 AM
```
## End-to-End Airflow Orchestration Architecture

```text
 [Midnight (00:00)] 
                              │
                              ▼ (Triggers Airflow DAG)
                  ┌───────────────────────┐
                  │   S3KeySensor Task    │ ◄── Wires into s3://.../year=2026/...
                  └───────────┬───────────┘
                              │
                              ▼ (File Detected)
                  ┌───────────────────────┐
                  │ DatabricksSubmitJob   │
                  └───────────┬───────────┘
                              │
                              ▼ (Triggers Multi-Task Workflow / Notebook)
 ┌─────────────────────────────────────────────────────────────────────────┐
 │                       Databricks Medallion Core                         │
 │                                                                         │
 │  ┌─────────────────┐       ┌─────────────────┐       ┌───────────────┐  │
 │  │  Bronze Layer   │ ────> │  Silver Layer   │ ────> │  Gold Layer   │  │
 │  │  (Raw Parquet)  │       │ (Clean/Indexed) │       │ (Aggregated)  │  │
 │  └─────────────────┘       └─────────────────┘       └───────────────┘  │
 └─────────────────────────────────────────────────────────────────────────┘
```
The Airflow DAG is configured with cron job schedule="0 0 * * *" to trigger precisely at midnight. It implements an S3KeySensor to handle variable delivery times.

## Power BI
Bussiness Dashboard

```text
- Daily Revenue

- Monthly Revenue

- Top Products

- Top Stores

- Sales Trend

- Category Revenue

- Customer Growth
```

Power BI connects directly to Snowflake.

## Daily Timeline

```text
11:45 PM

Stores close

↓

12:00 AM

CSV generated

↓

12:15 AM

Upload to S3

↓

12:30 AM

Airflow starts

↓

12:35 AM

PySpark reads CSV

↓

12:45 AM

Data Validation & Cleaning(Bronz Layer)

↓

1:15 AM

Transformations(Silver Layer)

↓

2:00 AM

Delta Lake(Gold Layer)

↓

2:30 AM

Snowflake

↓

3:00 AM

Power BI Refresh

↓

6:00 AM

Business Opens
```
-------------------------------------------

