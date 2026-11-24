# Quince M2C – Factory-Direct Order Routing & Unit-Economics Lakehouse

We're going to build this project using Databricks Free Edition, Lakeflow Declarative Pipelines, Delta Lake, PySpark, SQL, and Power BI.

**Our platform will answer five important business questions:**
1. Which factory should manufacture and fulfill each incoming customer order?
2. Does the factory have sufficient production capacity and raw materials?
3. Will cross-border freight, customs duties, and payment fees make the order unprofitable?
4. Where is each shipment, and will it reach the customer within the promised delivery window?
5. How much profit does Quince actually retain after returns, refunds, duties, logistics, and other costs?

**We'll maintain two separate analytical domains:**
- ***Supply Chain Analytics:*** factories, production, capacity, allocation, shipments, customs, delivery, and returns.
- ***Finance Analytics:*** revenue, discounts, COGS, freight, duties, payment fees, refunds, salvage recovery, and net margin.

## Project Architecture:
```text
                  QUINCE M2C BUSINESS SOURCES
 ┌─────────────────────────────────────────────────────────┐
 │ Storefront Orders │ Factory ERP │ Raw Materials         │
 │ Factory Capacity  │ Carrier APIs│ Customs & Tariffs     │
 │ Freight Rates     │ Returns     │ Payment Transactions  │
 └──────────────────────────┬──────────────────────────────┘
                            │
             ┌──────────────┴──────────────┐
             │                             │
       REAL-TIME EVENTS               BATCH INGESTION
       Kafka / Event Hubs             Azure Data Factory
       Orders / Tracking              Factory / Cost / Tariffs
             │                             │
             └──────────────┬──────────────┘
                            │
                    ADLS GEN2 / RAW
                            │
                 DATABRICKS LAKEHOUSE
                            │
            ┌───────────────▼──────────────┐
            │         BRONZE LAYER         │
            │ Raw orders and event history │
            │ Factory and cost source data │
            │ Append-only ingestion        │
            └───────────────┬──────────────┘
                            │
            ┌───────────────▼──────────────┐
            │         SILVER LAYER         │
            │ Deduplication                │
            │ Data quality                 │
            │ Currency normalization       │
            │ Factory capacity validation  │
            │ Shipment lifecycle           │
            └───────────────┬──────────────┘
                            │
            ┌───────────────▼──────────────┐
            │          GOLD LAYER          │
            │ Fact and dimension tables    │
            │ Factory routing              │
            │ Unit economics                │
            │ Supply chain aggregates      │
            │ Finance aggregates           │
            └───────────────┬──────────────┘
                            │
             ┌──────────────┴──────────────┐
             │                             │
      SUPPLY CHAIN BI                 FINANCE BI
      Factory performance             Revenue & COGS
      Delivery performance            Landed costs
      Capacity utilization            Returns & refunds
      Customs performance             Net margin
             │                             │
             └──────────────┬──────────────┘
                            │
                       POWER BI
```
## Project Folder Structure:
```text
quince_m2c_lakehouse/
│
├── datasets/
│   ├── orders/
│   ├── order_events/
│   ├── factories/
│   ├── factory_capacity/
│   ├── raw_materials/
│   ├── factory_production/
│   ├── shipments/
│   ├── customs_tariffs/
│   ├── freight_rates/
│   ├── returns/
│   └── payments/
│
├── lakeflow_pipeline/
│   └── quince_m2c_pipeline.sql
│
├── dashboard_sql/
│   ├── supply_chain_dashboard.sql
│   ├── factory_performance.sql
│   ├── logistics_dashboard.sql
│   └── finance_dashboard.sql
│
├── routing/
│   └── factory_routing_logic.py
│
├── documentation/
│   ├── architecture.md
│   ├── data_dictionary.md
│   ├── kpi_dictionary.md
│   └── interview_explanation.md
│
└── README.md
```
**We'll use three schemas:**
```text
Schema	                   Responsibility
------                     --------------
quince_m2c.bronze	         Raw source data
quince_m2c.silver	         Cleaned and standardized data
quince_m2c.gold	           Business-ready facts, dimensions and aggregates
```

### Dashboards we will build:
- Dashboard 1 – Executive M2C Control Tower
    - Audience: COO, Head of Operations, Business Leadership
- Dashboard 2 – Factory & Production Analytics
    - Audience: Factory Operations and Procurement team
- Dashboard 3 – Global Logistics & Customs Analytics
    - Audience: Logistics and Supply Chain Managers
- Dashboard 4 – Finance & Unit Economics
    - Audience: CFO, Finance and Treasury
- Dashboard 5 – Returns & Product Quality Analytics
    - Audience: Customer Experience, Product Quality and Finance team

### A. Batch ingestion

- (Simulated ADF + ADLS Gen2)
- Factory ERP / Procurement / Customs
- CSV files in Unity Catalog Volume
- Lakeflow Materialized Views
- Bronze Delta Tables

### B. Event ingestion

- (Simulated Kafka / Event Hubs)
- Storefront and Carrier Webhooks
- JSON / CSV event files
- Lakeflow Streaming Table
- Bronze Event Delta Table

## Important Terminologies:
- Storefront Orders: Storefront orders are customer purchases placed directly through an online store's customer-facing website or app (its storefront), rather than through third-party marketplaces or POS.
