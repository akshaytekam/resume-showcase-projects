# Databricks Batch and Streaming Pipeline Project

So this single project demonstrates COPY INTO + idempotency + Auto Loader + Delta Lake + Medallion Architecture + data quality + Time Travel + DLT/Lakeflow + orchestration + dashboarding—a strong end-to-end project.

<img width="1774" height="887" alt="projectPlan" src="https://github.com/user-attachments/assets/773d1333-f114-4991-bf63-fc10bcbe76f1" />


This production-style retail Lakehouse project on Databricks, where i built both batch and streaming pipelines using Delta Lake and Medallion Architecture. I ingested 13717 batch order records using COPY INTO with idempotency checks and 1500 streaming events using Auto Loader with checkpointing. I implemented Bronze, Silver, and Gold layers with data-quality validation, deduplication, and business transformations. I also used Delta Time Travel for auditing and historical data analysis, built a declarative DLT/Lakeflow pipeline for batch and streaming processing, orchestrated the workflow using Databricks multi-task Jobs, and created a sales dashboard from the Gold layer for business reporting.

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

# Dashboard:
<img width="1892" height="902" alt="Dashboard retal sales" src="https://github.com/user-attachments/assets/6a06fa54-d9f0-45c3-b2e8-f5f3e411b095" />
