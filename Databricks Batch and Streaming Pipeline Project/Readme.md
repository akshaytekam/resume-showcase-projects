# Databricks Batch and Streaming Pipeline Project

## Project File Structure:
```text
databricks_lakehouse_project/
│
├── README.md
│
├── data/    (Source data coming from stores/applications)
│   │
│   ├── batch_orders/
│   │   ├── orders_part_1.csv
│   │   ├── orders_part_2.csv
│   │   └── orders_part_3.csv
│   │
│   └── streaming_events/
│       ├── events_part_1.json
│       ├── events_part_2.json
│       └── events_part_3.json
│
├── notebooks/    (Actual Databricks ETL/ELT logic)
│   │
│   ├── 01_batch_copy_into.sql
│   ├── 02_batch_idempotency.sql
│   ├── 03_streaming_autoloader.py
│   ├── 04_silver_transform.sql
│   ├── 05_gold_revenue.sql
│   ├── 06_gold_customers.sql
│   ├── 07_delta_time_travel.sql
│   └── 08_dlt_pipeline.py
│
├── jobs/
│   └── 09_job_tasks.json   (Production scheduling and task dependencies)
│
└── dashboard/
    └── sales_dashboard.sql   (Business reporting / visualization)
```

## The Overall Heirarchy:
```text
                    JOB
                     │
                     ↓
             ┌───────────────┐
             │    DATA       │
             └───────┬───────┘
                     │
          ┌──────────┴──────────┐
          ↓                     ↓
     batch_orders         streaming_events
          │                     │
     COPY INTO              AUTO LOADER
          │                     │
          └──────────┬──────────┘
                     ↓
                  BRONZE
                     │
                     ↓
             CLEAN + DEDUP
                     │
                     ↓
                  SILVER
                     │
              ┌──────┴──────┐
              ↓             ↓
       GOLD_REVENUE   GOLD_CUSTOMERS
              │             │
              └──────┬──────┘
                     ↓
                 DASHBOARD
```
