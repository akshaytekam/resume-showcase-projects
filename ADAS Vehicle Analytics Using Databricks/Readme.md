# ADAS Vehicle Analytics Using Databricks

## Business scenario:

Vehicle ADAS systems continuously generate events from camera, radar, LiDAR and ultrasonic sensors. 
We want to ingest those events into Databricks, validate and transform them, maintain vehicle/OEM history, and expose KPIs to a BI dashboard.

<img width="1220" height="442" alt="ADAS new leanage" src="https://github.com/user-attachments/assets/373dcbfb-fd5e-4f4f-8d46-f9672a3f8499" />

## Architecture:
```text
                 ADAS Source Systems
                        │
             ┌──────────┴──────────┐
             │                     │
        Batch CSV              Streaming JSON
             │                     │
             └──────────┬──────────┘
                        ▼
               Databricks Volume
              /Volumes/adas/dev/raw
                        │
                        ▼
              ┌───────────────────┐
              │ Bronze - DLT      │
              │ Raw ADAS Events   │
              └─────────┬─────────┘
                        │
              Schema + DQ Validation
                        │
                        ▼
              ┌───────────────────┐
              │ Silver - DLT      │
              │ Cleaned Events    │
              │ Watermarking      │
              │ CDC / SCD2        │
              └─────────┬─────────┘
                        │
                        ▼
              ┌───────────────────┐
              │ Gold              │
              │ Star Schema       │
              └─────────┬─────────┘
                        │
                        ▼
                    Power BI
```

NOTE: We are using DEV, TEST and PROD environment.


