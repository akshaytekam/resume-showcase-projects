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

### Gold data model — what we're building:

```text
**A. Fact tables**
Table	                        Grain	                                  Purpose
-----                         -----                                  -------
fact_order_economics	    One row per order	                Revenue, material, freight, duties, fees, refunds, profit
fact_order_fulfillment	  One row per order	                Factory assignment, production and delivery performance
fact_factory_production	  One row per production run	      Planned vs actual production, defects
fact_factory_capacity	    Factory + SKU + date	            Available and utilized capacity
fact_returns	            One row per return	              Refund, return reason, disposition, salvage
fact_payments	            One row per payment	              Collection, gateway fee, refunds

**B. Dimension tables**
Table	                                                            Purpose
------                                                            -------
dim_factory	                                              Factory attributes, country, audit status, quality score
dim_product	                                              SKU and product attributes
dim_date	                                                Calendar analysis
dim_geography	                                            Customer destination and region
dim_carrier	                                              Carrier information

**C. Aggregate tables**
Table                                                            	Grain
---------                                                       -----------
agg_finance_monthly	                                        Month + currency
agg_factory_performance                                    	Factory + reporting period
agg_sku_profitability	                                      SKU + reporting period
agg_country_logistics	                                      Country + reporting period
```

### Power BI data model design:
After Gold is ready, we'll connect Power BI to the Gold tables using Databricks SQL.

We will use a star-schema approach where appropriate.

### Gold KPIs we have prepared:
**Finance KPIs:**

KPI	                                  Gold source	                              Calculation
----                                   ----------                              -------------
Gross Revenue	                    fact_order_economics	                        Qty × Selling price
Net Sales	                        fact_order_economics	                        Revenue less discount
Material Cost	                    fact_order_economics	                        Qty × material cost
Freight Cost	                    fact_order_economics	                        Shipment freight
Customs Duty	                    fact_order_economics	                        Estimated material value × tariff
Payment Fees	                    fact_order_economics	                        Gateway fees
Refunds	                          fact_order_economics	                        Return refund amount
Contribution Profit	              fact_order_economics	                        Net sales minus costs plus salvage
Contribution Margin	              fact_order_economics	                        Profit / net sales
SKU Profitability	                agg_sku_profitability	                        Contribution profit by SKU

**Supply Chain KPIs:**

KPI	                                  Gold source	                              Calculation
----                                   ----------                              -------------
Factory Utilization	                  fact_factory_capacity                        	Utilized / total capacity
Defect Rate	                        fact_factory_production	                          Defects / produced units
Production Completion	              fact_factory_production	                        Produced / planned
Average Delivery Time	                fact_order_fulfillment	                      Avg order-to-delivery days
On-Time Delivery Rate	                fact_order_fulfillment	                      On-time / delivered
Late Delivery Rate	                fact_order_fulfillment	                        Late / delivered
Allocation Delay	                  fact_order_fulfillment	                        Allocation date - order date
Freight per Order	                  fact_order_economics	                          Freight / orders
Factory Performance	                agg_factory_performance	                        Combined production and delivery metrics
Return Rate	                        fact_returns + order facts	                    Returned orders / orders


### production improvements:
```text
Current limitation	                                                  Production improvement
-------------------                                                   -------------------- 
Material cost used as customs value	                                  Use actual customs-declared valuation
Factory labor cost unavailable	                                      Add labor and conversion costs
USD-only synthetic dataset	                                          Add currency conversion and FX rates
Refund datasets may disagree	                                        Reconcile payment refunds to return records
One shipment assumed per order	                                      Model order-to-shipment relationship separately
Factory assignment inconsistencies	                                  Create authoritative factory allocation fact
Date dimension only contains order dates	                            Generate a complete calendar
Synthetic data	                                                      Integrate real ERP, carrier and payment sources
Simple capacity calculation	                                          Add reservations, concurrency handling and allocation history
Simplified tariff matching	                                          Implement effective-date and customs-rule logic
```


## Important Terminologies:
- Storefront Orders: Storefront orders are customer purchases placed directly through an online store's customer-facing website or app (its storefront), rather than through third-party marketplaces or POS.
