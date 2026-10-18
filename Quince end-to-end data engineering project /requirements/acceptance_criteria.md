# Quince Data Platform — Acceptance Criteria

**Version:** 1.0  
**Status:** Draft for portfolio implementation  
**Environment:** Synthetic datasets  
**Related documents:** business_requirements.md, kpi_catalogue.xlsx, source_inventory.xlsx

## 1. Purpose

Define measurable pass/fail conditions for ingestion, data quality, transformation, incremental processing, reconciliation, reporting, and operational reliability.

All thresholds below are initial project targets. Business owners must approve production thresholds in a real implementation.

## 2. Source Ingestion

### AC-ING-01: File ingestion completeness
- Every expected input file must be accounted for in the ingestion manifest.
- Each file must have a recorded source name, file path, ingestion timestamp, and processing status.
- **Pass:** All required files are ingested or explicitly marked as missing/failed.
- **Fail:** A required file is silently skipped.

### AC-ING-02: Row-count reconciliation
- Compare source row counts with the number of records landed in Bronze.
- Document expected differences caused by headers, rejected records, or source-specific filtering.
- **Pass:** Counts reconcile, with every difference explained and recorded.
- **Fail:** Records are missing or duplicated without an explanation.

### AC-ING-03: Idempotent reruns
- Reprocess the same input file or source batch.
- **Pass:** Rerunning does not create unintended duplicate business records.
- **Fail:** Reprocessing changes record counts or financial totals unexpectedly.

### AC-ING-04: Incremental load
- Process a first batch, then a second batch containing new and updated records.
- **Pass:** New records are inserted, eligible updates are applied, and previously committed data is retained.
- **Fail:** Existing records are lost or changes are not captured.

## 3. Schema and Data Quality

### AC-DQ-01: Required columns
- Validate that each input contains its required columns.
- **Pass:** Valid schema is accepted.
- **Fail:** Missing or incompatible required columns cause a controlled failure or quarantine.

### AC-DQ-02: Primary-key uniqueness
- Check uniqueness at the documented source grain.
- **Pass:** No unexplained duplicate keys remain in the curated dataset.
- **Fail:** Conflicting duplicate records are silently selected.

### AC-DQ-03: Null validation
- Validate required identifiers, dates, and amounts.
- **Pass:** Invalid records are rejected or quarantined with reasons.
- **Fail:** Invalid records enter curated tables without an approved rule.

### AC-DQ-04: Referential integrity
- Validate order lines against orders and other documented parent-child relationships.
- **Pass:** Unmatched references are identified and recorded.
- **Fail:** Invalid references are silently dropped or treated as valid.

### AC-DQ-05: Numeric and date rules
- Check quantities, monetary values, timestamps, and effective dates against documented business rules.
- **Pass:** Invalid values are handled according to an explicit rule.
- **Fail:** Invalid values silently distort business calculations.

## 4. Bronze, Silver, and Gold

### AC-LAY-01: Bronze traceability
- Bronze records must retain source identifiers and available ingestion metadata.
- **Pass:** A record can be traced to its input source and ingestion run.
- **Fail:** Source lineage is lost.

### AC-LAY-02: Silver standardization
- Standardize data types, timestamps, identifiers, and approved business values.
- **Pass:** Silver tables meet their documented schemas and validation rules.
- **Fail:** Unresolved data inconsistencies enter Silver without being flagged.

### AC-LAY-03: Gold business correctness
- Gold datasets must use approved business definitions and documented grain.
- **Pass:** KPI outputs reconcile to independently calculated test results.
- **Fail:** Aggregations double-count orders, settlements, journal lines, or inventory observations.

## 5. Financial Reconciliation

### AC-REC-01: Settlement matching
- Match gateway payouts with bank deposits using approved identifiers, amount rules, currency, and timing tolerances.
- **Pass:** Each eligible payout is classified as matched, pending, unmatched, or mismatched.
- **Fail:** Ambiguous matches are silently accepted.

### AC-REC-02: No duplicate allocation
- A bank transaction must not be allocated to multiple payouts unless a documented split/combined settlement rule explicitly permits it.
- **Pass:** Every allocation follows the approved matching rules.
- **Fail:** The same deposit is counted more than once.

### AC-REC-03: Reconciliation exceptions
- Record exception type, relevant source identifiers, amount variance, detection timestamp, and status.
- **Pass:** Exceptions can be queried and investigated.
- **Fail:** Unmatched transactions disappear from reporting.

### AC-REC-04: Financial totals
- Compare source and curated totals by currency and reporting period.
- **Pass:** Differences are zero or explained by approved adjustments and timing rules.
- **Fail:** Unexplained differences remain.

## 6. KPI Validation

### AC-KPI-01: On-time delivery
- Test with a known set of eligible delivered shipments.
- **Pass:** The percentage matches the independently calculated expected result.

### AC-KPI-02: Net revenue and gross profit
- Validate against approved deductions, COGS rules, and reporting currency.
- **Pass:** The calculations match the approved examples and period totals.

### AC-KPI-03: Budget variance
- Validate actual and budget values at matching periods, accounts, and currencies.
- **Pass:** Variance equals actual minus budget under the documented sign convention.

### AC-KPI-04: Cash and settlement metrics
- Verify closing balances, matched payout counts, unmatched amounts, and settlement variances.
- **Pass:** Each metric reconciles to the relevant source records and approved rules.

## 7. Audit and Operational Reliability

### AC-OPS-01: Pipeline audit
- Record pipeline/run ID, start and end time, status, source, row counts, and error details where applicable.
- **Pass:** Successful and failed runs are queryable.
- **Fail:** A run cannot be investigated from its audit records.

### AC-OPS-02: Failure handling
- Simulate an ingestion or transformation failure.
- **Pass:** Failure is visible, error details are recorded, and retry or recovery is documented.
- **Fail:** The pipeline appears successful despite incomplete processing.

### AC-OPS-03: Watermark safety
- Simulate a failed incremental run and subsequent retry.
- **Pass:** The committed watermark advances only when the corresponding data processing is safely committed.
- **Fail:** Records are skipped because the watermark advanced prematurely.

### AC-OPS-04: Late-arriving updates
- Submit an update for an older business event.
- **Pass:** The record is captured through the approved overlap, CDC, or correction process.
- **Fail:** Late-arriving changes are permanently missed.

## 8. Security

### AC-SEC-01: Least privilege
- Verify that identities have only the permissions required for their responsibilities.
- **Pass:** Unauthorized access is denied.

### AC-SEC-02: Sensitive fields
- Verify masking or restricted access for designated sensitive columns.
- **Pass:** Users without permission cannot access protected values through supported query paths.

## 9. Power BI Reporting

### AC-PBI-01: KPI consistency
- Compare report measures with the certified Gold datasets and approved KPI definitions.
- **Pass:** Dashboard totals and filters produce expected results.

### AC-PBI-02: Refresh reliability
- Validate refresh completion and report the last successful refresh time.
- **Pass:** Failed refreshes are visible and stale data is not presented as current.

### AC-PBI-03: Reconciliation visibility
- Display matched, pending, unmatched, and mismatched records with appropriate filters.
- **Pass:** Users can investigate exceptions down to the relevant source identifiers, subject to access permissions.

## 10. Sign-Off

For a production project, the relevant business owners, data owners, architecture team, and security team must approve their applicable requirements and thresholds before release.
