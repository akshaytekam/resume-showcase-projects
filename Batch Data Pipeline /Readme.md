## We'll build it step by step, including:

Business requirement
Folder structure
Data
PySpark code
Delta Lake
Snowflake loading
Airflow DAG
Best practices
Interview questions
Real-world improvements

## Project Overview

Company: I work for a retail company called RetailMart.

RetailMart has:

- 500 stores
- 15 million customers
- Around 8 million sales transactions daily

Every night, each store sends its sales file.

for examples,
Store_001_2026-07-05.csv
Store_002_2026-07-05.csv
Store_003_2026-07-05.csv
...
Store_500_2026-07-05.csv

Each file is uploaded automatically to AWS S3.

Our responsibility is to prepare clean data before 6 AM.

Business users use Power BI dashboards every morning.
 ----------------------------------------
 Architectural Workflow From source to s3:

 [Physical Retail Stores]
         │ 
         ▼ (Nightly Logs uploaded via SFTP)
 ┌───────────────┐
 │ AWS Transfer  │  <── Authenticates store servers using AWS Secrets Manager
 │  for SFTP     │
 └───────┬───────┘
         │
         ▼ (Directly routes incoming streams)
 ┌────────────────────────────────────────────────────────┐
 │           Amazon S3 Bucket (Target Location)           │
 │                                                        │
 │  s3://retail-sales-for-retailmart/raw/sales/           │
 │    ├── /year=2026/                                     │
 │    │    └── /month=07/                                 │
 │    │         └── /day=09/                              │
 │    │              └── store_001_sales.csv              │
 └────────────────────────────────────────────────────────┘

 ----------------------------------------

## High-Level Architecture

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

Store Generates Sales Data. Each store creates a CSV.

## Upload to AWS S3

Landing Bucket: s3://retail-sales-for-retailmart/raw/sales/

This is the Raw Layer.

<img width="1172" height="656" alt="bucket1" src="https://github.com/user-attachments/assets/8a85f2cc-b3d2-4cad-bc64-e9b3d8aedb0f" />

## Databricks Reads Files


## project structure is:

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

## Sales Files (Raw Data)

Columns:

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

## Product, Store, Customers Lookup Table

This acts as a Dimension table. (products.csv, stores.csv, customers.csv)

## Bad Records

Production systems always contain bad data. (Duplicates, Null, Invalid reords, missing values...etc)

## S3 Bucket Structure

Your S3 bucket could look like this:

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

## Basic Validation
always validates before processing. Check required columns.

## Airflow Scheduling
Every Night

12:30 AM

↓

Run pipeline

↓

Finish before 5 AM

## Power BI
Bussiness Dashboard

- Daily Revenue

- Monthly Revenue

- Top Products

- Top Stores

- Sales Trend

- Category Revenue

- Customer Growth

Power BI connects directly to Snowflake.

## Daily Timeline

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

Cleaning

↓

1:15 AM

Transformations

↓

2:00 AM

Delta Lake

↓

2:30 AM

Snowflake

↓

3:00 AM

Power BI Refresh

↓

6:00 AM

Business Opens

-------------------------------------------

