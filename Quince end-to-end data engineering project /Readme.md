# Quince end-to-end data engineering project

### We will build this as a realistic, portfolio-grade retail data platform based on Quince's publicly described business model, with three integrated pipelines:
1. Supply Chain Data Pipeline — factory production, inventory, carrier tracking, delivery, returns, and demand forecasting.
2. Finance Data Pipeline — orders, revenue, product costs, expenses, payment settlements, and profitability.
3. Treasury Data Pipeline — bank transactions, cash position, cash-flow forecasting, payment reconciliation, and financial risk.

**Project Stack**: Azure Data Factory (ADF), ADLS Gen2, Azure Databricks, PySpark, SQL, Python, Power BI, and MLflow, with Lakeflow Declarative Pipelines, Delta Lake, Change Data Capture (CDC), watermarking, centralized audit logging, schema validation, and liquid clustering.

## Understand Quince's business:
- **Business model**: Manufacturer-to-consumer (M2C) : Its a factory-direct model designed to remove traditional retail intermediaries.
- **Product categories**: Its public assortment includes apparel, accessories, jewelry, home goods, and other lifestyle categories. Our product model therefore needs category, SKU, variant, material, factory, and cost information.
- **Global manufacturing partners**: Quince working with specialist manufacturing partners across different regions and selecting factories for quality and responsible production.
- **Direct fulfillment and tracking**: Quince describes shipping directly from manufacturers to customers. Its logistics subsidiary also describes capabilities spanning planning, first-mile and last-mile logistics, tracking, returns, and fulfillment. These support our supply-chain use cases.

**Our platform should answer questions such as**:
- Which products should we produce more or less of?
- Which factory or shipping lane is causing delays?
- Where is each shipment, and which deliveries are at risk?
- Which products have high returns, defects, or low profitability?
- What are the actual product, freight, duty, and payment-processing costs?
- How much cash will Treasury need for upcoming obligations?
- Are payment gateway settlements matching bank deposits?
- Can Finance trust the numbers displayed in Power BI?

**I would ask the below questions to Stakeholders/Business Teams/Data Owners:**
- Which products, regions, and order journeys matter most?
- How do we measure factory delays, stockouts, and on-time delivery?
- How is net revenue, COGS, and gross margin calculated?
- How do we reconcile settlements and forecast cash?
- What are the source systems, volumes, and integration patterns?
- Are APIs, CDC feeds, databases, or file exports available?
- Which dashboards, filters, drill-downs, and security are required?
- Which users can access financial or customer data?
- What forecasts are useful, and how will accuracy be evaluated?

**Outbound Product Delivery:**
- They frequently utilize standard carriers like FedEx, UPS, and the USPS. (Returns also happens)
- Regional & Last-Mile Partners: Depending on your specific zip code, Quince often delegates regional delivery to next-generation last-mile networks like OnTrac, Veho, and Jitsu.
- Heavy Furniture & Freight: For oversized items or rugs measuring 7×10 and above, Quince bypasses parcel networks entirely and partners with Metropolitan Warehouse to handle specialized freight and white-glove delivery.


## Initial KPIs Catalogue:
**A. Supply Chain dashboard:**
- On-time delivery % => Measure carrier delivery reliability
- Average order-to-delivery time => Measure customer fulfillment speed
- Factory production lead time	=> Identify manufacturing bottlenecks
- Production defect rate	=> Monitor quality problems
- Stockout rate	 => Identify lost-sales risk from unavailable stock
- Inventory turnover	=> Understand how efficiently inventory moves
- Demand forecast error	 => Compare predictions with actual demand
- Return rate by product	=> Find quality, sizing, or fulfillment issues

**B. Finance dashboard:**
- Gross sales and net revenue	 => Measure sales after approved adjustments
- Gross profit and margin %	 => Measure product profitability
- Cost per order	=> Understand manufacturing and fulfillment economics
- Freight and duty cost per unit	=> Identify logistics cost drivers
- Actual vs budget variance	 => Detect financial deviations
- Returns and refund value	=> Understand revenue reversals
- Profitability by SKU/category	 => Find high- and low-margin products

**C. Treasury dashboard:**
- Available cash balance	=> Understand liquidity
- Forecast closing cash	 => Identify future cash shortfalls
- Expected vs actual settlements	=> Track payout completeness
- Bank reconciliation rate	 => Measure successful matching
- Unreconciled amount	 => Quantify outstanding exceptions
- Settlement delay	 => Identify late processor payouts
- FX exposure	 => Measure sensitivity to exchange-rate movements

For every KPI, our requirements document will specify its formula, source, grain, currency, refresh frequency, owner, filters, and acceptance test.

**data sources:**
- E-commerce/order platform
- Product catalogue
- Factory/manufacturing systems
- Procurement systems
- Inventory systems
- Carrier tracking systems
- Returns system
- Payment gateway
- Banking systems
- ERP / General Ledger
- Finance planning
- FX provider

**Source access checklist:** For every source, I would confirm the owner, API/file location, authentication method, schema, data volume, update frequency, historical backfill, CDC support, SLA, and data retention policy.

## Architecture:
**Lambda-style architecture:** a batch path for complete historical and periodic data, and a streaming path for shipment events and other time-sensitive updates. Both paths ultimately produce governed Delta tables for analytics.
***Batch***: Historical orders, full product/factory masters, monthly budgets, GL extracts, bank statements, inventory snapshots, and backfills.
***Streaming or micro-batch***: Carrier tracking events, shipment status updates, and payment status events where an appropriate feed is available.

## Folder Structure:
```text

quince-data-platform/
├── README.md
├── requirements/
│   ├── business_requirements.md
│   ├── kpi_catalogue.xlsx
│   ├── source_inventory.xlsx
│   └── acceptance_criteria.md
├── docs/
│   ├── business_overview.md
│   ├── architecture.md
│   ├── source_to_target_mapping.xlsx
│   ├── data_dictionary.md
│   ├── data_quality_rules.md
│   ├── security_and_access.md
│   ├── operations_runbook.md
│   └── decisions_log.md
├── infrastructure/
│   ├── adls_layout.md
│   ├── adf/
│   └── databricks/
├── adf/
│   ├── linked_services/
│   ├── datasets/
│   ├── pipelines/
│   │   ├── pl_ingest_batch_sources
│   │   ├── pl_ingest_cdc_sources
│   │   ├── pl_ingest_reference_data
│   │   └── pl_orchestrate_gold_refresh
│   └── triggers/
├── databricks/
│   ├── notebooks/
│   │   ├── 00_environment_setup.py
│   │   ├── 01_schema_validation.py
│   │   ├── 02_bronze_ingestion.py
│   │   ├── 03_silver_orders.py
│   │   ├── 04_silver_products_factories.py
│   │   ├── 05_silver_inventory.py
│   │   ├── 06_silver_shipments.py
│   │   ├── 07_silver_finance.py
│   │   ├── 08_silver_treasury.py
│   │   ├── 09_cdc_and_scd.py
│   │   ├── 10_reconciliation.py
│   │   ├── 11_gold_supply_chain.py
│   │   ├── 12_gold_finance.py
│   │   ├── 13_gold_treasury.py
│   │   ├── 14_audit_and_dq.py
│   │   ├── 15_liquid_clustering.py
│   │   └── 16_ml_forecasting.py
│   ├── lakeflow/
│   │   ├── pipeline_config/
│   │   ├── bronze_streaming.py
│   │   ├── shipment_events.py
│   │   ├── silver_streaming.py
│   │   └── expectations.py
│   ├── sql/
│   │   ├── create_catalogs_schemas.sql
│   │   ├── create_tables.sql
│   │   ├── reconciliation.sql
│   │   └── gold_views.sql
│   ├── src/
│   │   ├── schema_registry.py
│   │   ├── audit_logger.py
│   │   ├── watermark_manager.py
│   │   ├── dq_rules.py
│   │   └── common_transformations.py
│   └── tests/
│       ├── test_schema_validation.py
│       ├── test_deduplication.py
│       ├── test_reconciliation.py
│       └── test_kpi_logic.py
├── powerbi/
│   ├── supply_chain_control_tower/
│   ├── finance_performance/
│   ├── treasury_reconciliation/
│   └── semantic_model_and_dax.md
├── ml/
│   ├── demand_forecasting/
│   ├── shipment_delay_prediction/
│   └── model_evaluation.md
└── data/
    ├── source_exports/
    └── streaming_events/

```

## Important Terminologies and Definations:
- Shipment milestones: (Order Confirmed, Cargo Ready, Origin Pickup / First Mile, Arrived at Distribution Center, Out for Delivery / Last Mile)
- Gross margin: the percentage of revenue a company retains after covering the direct costs of producing its goods or delivering its services.
- Cost of Goods Sold (COGS): The direct costs attributable to production. This includes raw materials, direct manufacturing labor, and factory overheads.
- Freight: Inbound Freight (Included in COGS), Outbound Freight (Excluded from COGS)
- Duties: If you import inventory from international suppliers, any non-refundable customs duties, tariffs, and clearance fees levied by the government.
- Fulfillment : Pick-and-Pack, 3PL, and Last-Mile Fees: "Fulfillment" typically refers to the process of picking an item off a warehouse shelf, packaging it, and prepping it for a specific customer order.
- SKU category (Stock Keeping Unit category): It is a high-level classification used in retail inventory management to group similar individual products (SKUs) based on shared characteristics.
- In Scope:  your team is officially responsible for completing. Out of Scope: goal that falls outside the agreed-upon project boundaries and is not funded.
