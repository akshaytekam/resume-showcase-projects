# Complete KPI & Dashboard Catalogue

****Our project has three business dashboards:****
1. Supply Chain Control Tower — orders, production, inventory, shipments, demand and returns.
2. Finance Performance — revenue, costs, profitability and budget performance.
3. Treasury Reconciliation — payment settlements, bank credits, cash reconciliation and financial exceptions.

## 1. Supply Chain Control Tower:

**1. On-Time Delivery (OTD) %**
Delivery performance
Measures the percentage of eligible delivered shipments that arrived on or before the promised delivery date.

OTD % = (On-time delivered shipments) / (Eligible delivered shipments) * 100

Visual: KPI card, monthly trend line and carrier comparison bar chart.
Data: shipments.csv — promised delivery date, actual delivery date, shipment ID, carrier and status.

**2. Average Order-to-Delivery Days:**

Measures the average time between order placement and delivery.

Average days = AVG(Delivery date - Order date)

Visual: KPI card and monthly line chart.
Data: orders.csv and shipments.csv — order date and actual delivery date. Join using order_id. define how to handle multiple shipments per order.

**3. Production Lead Time:**

Measures the average time required to produce an item or complete a production order.

Lead time = AVG(Completion - Start)

Visual: KPI card, monthly trend and factory comparison bar chart.
Data: factory_production_orders.csv — production start and completion timestamps, production order ID and factory ID.

**4. Inventory Turnover:**

Indicates how efficiently inventory is sold or consumed during a period.

Turnover = (Period COGS / Average inventory value)

Visual: KPI card, product-category bar chart and monthly trend.
Data: general_ledger_entries.csv for mapped COGS and inventory_snapshots.csv for inventory value or quantity plus a valid unit-cost source.
Important: Quantity alone is insufficient to calculate value-based turnover unless a defensible unit cost is available.

**5. Stockout Rate %**

Measures how often a product-location combination has no available stock.

Stockout = (Eligible observations with zero available stock / Eligible product-location observations) * 100

Visual: KPI card, product/warehouse heatmap and ranked bar chart.
Data: inventory_snapshots.csv — snapshot date, product, warehouse/location and available quantity. Agree on whether negative inventory also counts as a stockout.

**6. Defect Rate %**

Measures the proportion of inspected units classified as defective.

Defect = (Defective units / Inspected units) * 100

Visual: KPI card and factory/product-category bar chart.
Data: A quality inspection or defect dataset is needed. returns.csv may provide a return reason, but returns alone do not reliably measure manufacturing defects.

**7. Return Rate %**

Measures the share of sold units subsequently returned, using a consistent sales cohort or period definition.

Return rate = (Returned units / Eligible sold units) * 100

Visual: KPI card, monthly trend and product-category bar chart.
Data: returns.csv and order_lines.csv — returned quantity, order/line IDs, sold quantity and relevant dates. Prevent duplicate counting when a line has multiple return events.

**8. Demand Forecast WAPE %**

Measures forecast error relative to actual demand; lower values generally indicate better forecasts.

WAPE % = (sum(Actual - Forecast) / sum(Actual)) * 100

Visual: Actual-versus-forecast line chart, WAPE KPI card and product-level error bar chart.
Data: Actual sales from order_lines.csv aggregated by product and date, plus forecast outputs from the forecasting pipeline. The forecast dataset must be generated before this KPI can be calculated.

## 2. Finance Performance dashboard:

This dashboard focuses on financial performance, profitability and budget control. In our current design, accounting actuals come from the general ledger, while budget targets come from finance_budget.csv.

**9. General Ledger Revenue**

Measures revenue posted to ledger accounts mapped to REVENUE.

GL Revenue = sum(Mapped revenue actuals)

Visual: KPI card, monthly line chart and currency breakdown.
Data: general_ledger_entries.csv → silver.general_ledger_entries → gold.fact_finance_actuals. In our mapping, account 4000 is Revenue.

**10. Cost of Goods Sold (COGS)**

Measures the accounting cost assigned to goods sold during the period.

COGS = sum(Mapped COGS actuals)

Visual: KPI card, monthly trend and category comparison.
Data: General ledger entries mapped to COGS; account 5000 in our current synthetic ledger mapping.

**11. Gross Profit**

Measures revenue remaining after the cost of goods sold.

Gross Profit = (GL Revenue - COGS)

Visual: KPI card and monthly revenue-versus-COGS chart.
Data: Derived from the revenue and COGS measures in fact_finance_actuals or agg_finance_monthly.

**12. Gross Margin %**

Shows how much of revenue remains after COGS, expressed as a percentage.

Gross Margin = (Gross Profit / GL Revenue) * 100

Visual: KPI card, monthly line chart and comparison against target.
Data: Derived from mapped ledger revenue and COGS. Return or contra-revenue accounting must be handled consistently before interpreting this as a net-revenue margin.

**13. Operating Expenses (OPEX)**

Measures expenses classified as operating expenses under the agreed accounting policy.

OPEX = sum(Mapped operating expense actuals)

Visual: KPI card, expense-category bar chart and monthly trend.
Data: General ledger entries. Our synthetic mapping currently classifies account 6100 (Freight Expense) and 6200 (Payment Processing Fees) as OPEX. Freight classification should follow the accounting policy.

**14. Budget Amount**

Represents the approved financial target for a month, currency and category.

Visual: KPI card and clustered column chart comparing budget with actual.
Data: finance_budget.csv → silver.finance_budget → planned Gold monthly aggregate. This is the next integration step; we must verify the actual budget schema first.

**15. Budget Variance and Variance %**

Shows the difference between actuals and the approved budget.

Variance= (Actual - Budget)

Variance % = ((Actual - Budget) / Budget) * 100

Visual: Diverging variance bar chart, monthly trend and conditional-formatting table.
Data: Monthly actuals plus category-matched budget records. For expenses, a positive variance generally means overspending; for revenue, it generally means performance above budget.

**16. Net Revenue**

Measures revenue after applicable discounts, returns and allowances, according to the approved accounting definition.

Visual: KPI card and gross-to-net waterfall chart.
Data: Revenue ledger accounts, returns.csv, and relevant discount/refund accounting entries. Do not simply subtract return records if their refunds are already reflected in the ledger; that would double-count them.

**17. Cost per Order**

Measures the selected operating or fulfillment cost per eligible order.

Cost per Order = (Defined eligible costs) / (Distinct eligible orders)

Visual: KPI card and monthly trend, with category or fulfillment-channel breakdown where supported.
Data: orders.csv, relevant ledger expenses and, if available, freight/payment allocations. Define the included cost components before calculating.


## 3. Treasury Reconciliation dashboard:
This dashboard checks whether gateway settlements agree with bank statement credits. Our existing reconciliation design uses gateway_settlements.csv and bank_statement_transactions.csv.

**18. Settlement Match Rate %**

Measures the share of eligible settlements successfully matched to bank credits.

Match Rate % = (Matched settlements) / (Eligible settlements) * 100

Visual: KPI card, daily trend and matched-versus-unmatched bar chart.
Data: gateway_settlements.csv, bank_statement_transactions.csv and silver.settlement_bank_reconciliation. Define eligible settlement statuses consistently.

**19. Unmatched Expected Payout**

Measures the expected payout amount for eligible settlements without an assigned bank transaction.

Visual: KPI card, aging/exception table and daily trend.
Data: Gateway settlement net payout and settlement_bank_reconciliation, summarized in agg_treasury_daily. Confirm whether the figure includes only unmatched settlements, not variance cases.


**20. Settlement Variance**

Measures the difference between expected settlement payout and the assigned bank credit.

Variance = (Bank Credit) - (Expected Payout)

Visual: Variance bar chart, exception table and trend by settlement date.
Data: Gateway settlements, bank statement transactions and the reconciliation output. The sign convention must match the chosen business definition.

**21. Closing Cash Balance**

Measures the cash balance at the end of a day or reporting period.

Visual: KPI card, daily closing-cash line chart and bank-account breakdown.
Data: Bank statement transactions with opening balance or a verified starting balance, signed transactions and posting dates. A ledger cash account balance may provide a separate accounting view. Transaction amounts alone do not establish closing balance without a starting balance.

**22. Unmatched Bank Credits**

Measures incoming bank credits that cannot be assigned to a settlement.

Visual: Exception-count KPI, bank-credit amount card and searchable exception table.
Data: bank_statement_transactions.csv and gold.fact_bank_unmatched. Our current design considers positive bank amounts; debit-side exceptions require a separate rule.

**23. FX Exposure**

Measures eligible foreign-currency balances or cash flows exposed to exchange-rate movements.

Visual: Currency exposure bar chart, currency composition chart and FX trend.
Data: Foreign-currency bank balances, settlement/ledger amounts and fx_rates.csv. Choose a reporting currency, valuation date and exposure definition before calculating.

**24. Cash Forecast Accuracy**

Compares forecast cash balances or flows against actual results.

Visual: Forecast-versus-actual line chart and absolute error KPI.
Data: A cash forecast dataset plus bank/ledger actuals aligned to the same dates and definition. This requires forecast outputs; it cannot be calculated from bank statements alone.

