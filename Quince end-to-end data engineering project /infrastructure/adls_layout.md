# Quince Data Platform — ADLS Gen2 Layout

## 1. Storage Account

- Resource group: `rg-quince-data-dev`
- Storage account: `stquince<uniqueid>dev`
- Hierarchical namespace: Enabled
- Environment: Development
- Access model: Microsoft Entra ID and Azure RBAC; use least privilege

## 2. Containers

- `landing`: Source extracts and event files
- `lakehouse`: Bronze, Silver, and Gold Delta data
- `control`: Ingestion manifests and processing control data
- `quarantine`: Invalid records and rejected files

## 3. Landing Container

landing/
  orders/
    extract_date=YYYY-MM-DD/
  order_lines/
    extract_date=YYYY-MM-DD/
  products/
    extract_date=YYYY-MM-DD/
  customers/
    extract_date=YYYY-MM-DD/
  factories/
    extract_date=YYYY-MM-DD/
  factory_production_orders/
    extract_date=YYYY-MM-DD/
  purchase_orders/
    extract_date=YYYY-MM-DD/
  inventory_snapshots/
    snapshot_date=YYYY-MM-DD/
  shipments/
    extract_date=YYYY-MM-DD/
  shipment_tracking_events/
    event_date=YYYY-MM-DD/
  returns/
    extract_date=YYYY-MM-DD/
  payment_transactions/
    extract_date=YYYY-MM-DD/
  gateway_settlements/
    settlement_date=YYYY-MM-DD/
  bank_statement_transactions/
    statement_date=YYYY-MM-DD/
  general_ledger_entries/
    posting_date=YYYY-MM-DD/
  finance_budget/
    budget_period=YYYY-MM/
  fx_rates/
    effective_date=YYYY-MM-DD/

## 4. Lakehouse Container

lakehouse/
  bronze/
    orders/
    order_lines/
    products/
    customers/
    factories/
    factory_production_orders/
    purchase_orders/
    inventory_snapshots/
    shipments/
    shipment_tracking_events/
    returns/
    payment_transactions/
    gateway_settlements/
    bank_statement_transactions/
    general_ledger_entries/
    finance_budget/
    fx_rates/

  silver/
    orders/
    order_lines/
    products/
    customers/
    factories/
    factory_production_orders/
    purchase_orders/
    inventory_snapshots/
    shipments/
    shipment_tracking_events/
    returns/
    payment_transactions/
    gateway_settlements/
    bank_statement_transactions/
    general_ledger_entries/
    finance_budget/
    fx_rates/

  gold/
    supply_chain/
    finance/
    treasury/
    shared_dimensions/

## 5. Control Container

control/
  manifests/
  watermarks/
  pipeline_runs/
  reconciliation/

## 6. Quarantine Container

quarantine/
  schema_errors/
  invalid_records/
  rejected_files/

## 7. Storage Rules

- Preserve original source files in landing according to the approved retention policy.
- Use immutable or uniquely named ingestion files to support replay and audit.
- Store source ingestion timestamps, source identifiers, and run identifiers where applicable.
- Use Delta tables for curated Bronze, Silver, and Gold datasets.
- Do not store credentials, access keys, or secrets in data files or notebooks.
- Restrict access to Treasury and sensitive financial data using least privilege.
- Record quarantine reasons and source lineage for rejected records.
- Apply lifecycle and retention policies only after business and audit requirements are agreed.
