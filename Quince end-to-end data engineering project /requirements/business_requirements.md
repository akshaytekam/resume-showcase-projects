# Quince Data Platform — Business Requirements

**Project:** Retail Finance and Supply Chain Analytics Platform  
**Document version:** 1.0  
**Status:** Draft — assumptions require stakeholder validation

## 1. Business Background

Quince operates a manufacturer-to-consumer retail model. For this portfolio project, we will design a data platform that consolidates data from order management, product, factory, inventory, shipment, payment, accounting, and banking systems.

The platform will provide reliable, validated, and analytics-ready datasets for Supply Chain, Finance, and Treasury teams.

The proposed solution will use Azure Data Factory (ADF), Azure Data Lake Storage Gen2 (ADLS), Azure Databricks, PySpark, SQL, Delta Lake, Power BI, and machine learning.

## 2. Business Problems

1. Business data is distributed across multiple source systems.
2. Order, shipment, inventory, and financial records may not reconcile.
3. Stakeholders need consistent KPI definitions across reports.
4. Finance needs visibility into revenue, costs, profitability, and budget variances.
5. Treasury needs to reconcile payment settlements with bank transactions.
6. Supply Chain needs visibility into inventory, factory production, delivery performance, and returns.
7. Data quality issues must be detected, recorded, and investigated.
8. Historical data must be retained to support trend analysis and auditing.

These are the problems our proposed project will demonstrate; they are not verified statements about Quince's internal systems.

## 3. Project Objectives

- Build automated batch ingestion pipelines.
- Simulate incremental and event-based ingestion.
- Implement Bronze, Silver, and Gold data layers.
- Validate schemas, remove duplicates, handle missing values, and standardize data.
- Implement incremental processing, watermarks, and CDC/SCD patterns where applicable.
- Build centralized pipeline audit logging and data quality monitoring.
- Reconcile payment settlements against bank transactions.
- Create certified business KPI datasets.
- Build Power BI dashboards for Supply Chain, Finance, and Treasury.
- Develop a demand forecasting model using historical sales.
- Implement access controls and protect sensitive financial and customer data.

## 4. Project Scope

### In scope

- Orders, order lines, products, customers, and factories.
- Factory production, purchase orders, and inventory snapshots.
- Shipment records, tracking events, and returns.
- Payment transactions, gateway settlements, and bank transactions.
- General ledger entries, finance budgets, and foreign exchange rates.
- Data ingestion, transformation, reconciliation, monitoring, and reporting.
- Demand forecasting and optional shipment-delay prediction.

### Out of scope for the initial release

- Integration with Quince's actual internal systems.
- Real payment execution or banking operations.
- Production deployment into Quince infrastructure.
- Automated financial posting or accounting adjustments.
- Financial advice or autonomous business decisions.

## 5. Stakeholders and Responsibilities

| Stakeholder | Responsibility |
|---|---|
| Business/Product Owner | Define business objectives and priorities |
| Supply Chain Analyst | Validate inventory, production, and delivery KPIs |
| Finance Analyst | Approve revenue, cost, profit, and budget definitions |
| Treasury Analyst | Define cash and settlement reconciliation rules |
| Source System Owner | Explain source schemas, access, and data limitations |
| Data Engineer | Design ingestion, transformations, validation, and monitoring |
| Data Architect | Review architecture, scalability, and integration patterns |
| BI Developer | Develop semantic models, DAX, and Power BI reports |
| Security/Governance Team | Approve access, retention, masking, and audit requirements |

## 6. Functional Requirements

**FR-01 — Data Ingestion:** Ingest source data through scheduled file, database, or API extracts where supported.

**FR-02 — Raw Storage:** Preserve source records in ADLS with source identifiers and ingestion metadata.

**FR-03 — Data Validation:** Check required columns, data types, nulls, duplicate keys, and invalid business values.

**FR-04 — Data Transformation:** Create standardized Silver datasets using PySpark and SQL.

**FR-05 — Incremental Processing:** Use watermarks or CDC where the source supports reliable change detection.

**FR-06 — Audit Logging:** Record pipeline run identifiers, source, processing status, timestamps, record counts, and error details.

**FR-07 — Reconciliation:** Match payment settlements with bank transactions and classify exceptions.

**FR-08 — Business Data Modeling:** Create reusable Gold fact and dimension tables.

**FR-09 — Reporting:** Publish curated datasets for Supply Chain, Finance, and Treasury dashboards.

**FR-10 — Machine Learning:** Build and evaluate a demand forecasting baseline using historical sales.

**FR-11 — Security:** Apply least-privilege access and appropriate protection for sensitive fields.

**FR-12 — Reliability:** Support retries, safe reruns, failure alerts, and documented recovery procedures.

## 7. Non-Functional Requirements

- **Data integrity:** Repeated processing must not create unintended duplicate records.
- **Traceability:** Processed records should be traceable to their source and pipeline run where feasible.
- **Reliability:** Failed loads must be detectable and recoverable.
- **Performance:** Transformations should be measured and optimized against agreed data volumes and service targets.
- **Security:** Access must be restricted according to role and business need.
- **Maintainability:** Reusable transformations and configuration should be separated from business-specific logic.
- **Observability:** Pipeline failures, data quality issues, and reconciliation exceptions must be queryable.
- **Scalability:** The design should accommodate growth in data volume and processing frequency.

## 8. Initial Refresh and Grain Assumptions

These are proposed development targets, not approved business SLAs.

| Dataset | Initial processing target | Expected grain |
|---|---|---|
| Orders and order lines | Hourly batch | One order / one order line |
| Product and factory masters | Daily batch | One record per business key |
| Inventory snapshots | Daily batch | SKU × location × snapshot time |
| Shipment tracking events | Near-real-time simulation or micro-batch | One tracking event |
| Payment transactions and settlements | Hourly batch | One transaction / one settlement record |
| Bank statement transactions | Daily batch | One bank transaction |
| General ledger and budget | Daily or monthly, depending on source | One journal line / budget period and reporting dimension |
| FX rates | Daily batch | Currency pair × effective date |

## 9. Business Rules Requiring Approval

- Whether revenue includes or excludes taxes, discounts, shipping charges, and cancelled orders.
- How refunds and returns affect net revenue and the reporting period.
- Which costs form part of COGS and gross profit.
- How freight, duties, and fulfilment costs are allocated.
- Which currency is used for reporting and which FX conversion date applies.
- How late deliveries and promised delivery dates are defined.
- How inventory turnover and stockout rates are calculated.
- How bank transactions are matched to gateway settlements.
- How timing differences, fees, refunds, reserves, and chargebacks are treated.
- How late-arriving records and historical corrections are handled.

## 10. Deliverables

1. Approved requirements and KPI catalogue.
2. Source inventory and source-to-target mappings.
3. ADLS storage layout and ADF ingestion pipelines.
4. Databricks Bronze, Silver, and Gold processing.
5. Data quality, audit, and reconciliation framework.
6. Power BI semantic models and dashboards.
7. Demand forecasting prototype and evaluation report.
8. Security, operational, and deployment documentation.

## 11. Acceptance and Sign-Off

The project will proceed to implementation once the requirements, KPI formulas, source mappings, refresh targets, security expectations, and reconciliation rules are documented.
