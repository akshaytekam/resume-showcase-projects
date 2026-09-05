# Databricks DLT (Lakeflow pipelines) Streaming Pipeline Project

### Business problem:

A retail company receives customer, product, and sales data as JSON files in cloud storage.

The company needs to:

- ingest new files automatically
- process incremental data
- handle customer/product CDC
- maintain current and historical dimensions
- validate data quality
- generate business-level sales metrics
- expose the results through a dashboard
- deploy the pipeline across environments

- Customers — customer master data, with updates
- Products — product master data, with updates
- Sales — continuously arriving transactions

Lakeflow automatically analyzes dependencies between datasets and orchestrates the graph rather than requiring us to manually orchestrate every transformation. "A traditional MERGE can implement CDC, but AUTO CDC provides a declarative CDC abstraction and handles CDC-specific concerns such as ordering and out-of-order events more naturally. I therefore prefer AUTO CDC for a Lakeflow pipeline when the source provides change events."

<img width="1456" height="647" alt="pipeline graph" src="https://github.com/user-attachments/assets/d4ec1cc0-ce24-47c6-8cfd-4fb02b404cc3" />

I used a Medallion architecture with Bronze, Silver, and Gold layers. Bronze handles incremental ingestion, Silver handles cleansing, CDC, SCD and enrichment, and Gold provides business-ready aggregated datasets.

```text
                    RAW DATA
                       │
          ┌────────────┼────────────┐
          │            │            │
      customers     products      sales
       CDC JSON      CDC JSON     JSON
          │            │            │
          └────────────┼────────────┘
                       ▼
                 ┌───────────┐
                 │   BRONZE  │
                 │ Raw/CDC   │
                 └─────┬─────┘
                       │
                       ▼
                 ┌───────────┐
                 │  SILVER   │
                 │ Cleaned   │
                 │ SCD 1 / 2 │
                 └─────┬─────┘
                       │
                       ▼
                 ┌───────────┐
                 │    GOLD   │
                 │ Material. │
                 │   Views   │
                 └─────┬─────┘
                       │
                       ▼
                 ┌───────────┐
                 │ AI/BI     │
                 │ Dashboard │
                 └───────────┘
```

**We'll deliberately make the customer and product datasets CDC-style. SCD Type 1 and Type 2.**
**Sales are append-only events.**

## Bronze layer:-
Bronze should preserve the incoming data with minimal transformation.
We'll use Auto Loader because this is a streaming file-ingestion scenario.

## Silver layer:-
```text
bronze_customers
       │
       ├──────────────► SCD Type 1
       │
       └──────────────► SCD Type 2
bronze_products
       │
       ├──────────────► SCD Type 1
       │
       └──────────────► SCD Type 2
```

## Gold layer with Materialized Views:-
This is where Materialized Views make sense.
Databricks describes materialized views as batch flows that incrementally maintain results when possible.


## Final Lakeflow pipeline graph:-
```text
                    ┌──────────────────┐
                    │ customers JSON   │
                    └────────┬─────────┘
                             │
                             ▼
                    ┌──────────────────┐
                    │ bronze_customers │
                    └────────┬─────────┘
                             │
                 ┌───────────┴────────────┐
                 ▼                        ▼
        ┌──────────────────┐     ┌──────────────────┐
        │ customers SCD1   │     │ customers SCD2   │
        └──────────────────┘     └──────────────────┘


                    ┌──────────────────┐
                    │ products JSON    │
                    └────────┬─────────┘
                             │
                             ▼
                    ┌──────────────────┐
                    │ bronze_products  │
                    └────────┬─────────┘
                             │
                             ▼
                    ┌──────────────────┐
                    │ products SCD2    │
                    └──────────────────┘


                    ┌──────────────────┐
                    │ sales JSON       │
                    └────────┬─────────┘
                             │
                             ▼
                    ┌──────────────────┐
                    │ bronze_sales     │
                    └────────┬─────────┘
                             │
                             ▼
                    ┌──────────────────┐
                    │ silver_sales     │
                    └────────┬─────────┘
                             │
              ┌──────────────┼──────────────┐
              ▼              ▼              ▼
       ┌─────────────┐ ┌─────────────┐ ┌──────────────┐
       │ daily sales │ │product sales│ │customer sales│
       │     MV      │ │     MV      │ │      MV      │
       └─────────────┘ └─────────────┘ └──────────────┘
              │              │              │
              └──────────────┼──────────────┘
                             ▼
                    ┌──────────────────┐
                    │ AI/BI Dashboard  │
                    └──────────────────┘
```

