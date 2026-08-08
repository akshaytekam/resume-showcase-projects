Enterprise Data Governance Practice Dataset

Purpose:
Realistic retail datasets for practicing Databricks, Delta Lake, Unity Catalog,
RBAC, masking, row-level security, metadata, lineage, auditing and data quality.

Intentional issues:
- customers: null/invalid emails, invalid phone/postal/country codes, duplicates
- employees: null salaries
- orders: orphan customer/product IDs, negative quantity/discount, invalid status, duplicates
- payments: sensitive financial attributes
- all datasets contain fields suitable for data classification and masking exercises

Suggested load order:
1. countries, stores, products
2. customers, employees
3. orders, payments, customer_segments

Governance classification examples:
PII: customer_name, email_address, phone_number, date_of_birth, address_line1
Restricted: salary, bank_account_last4
Financial: transaction_amount, payment_method, card_last4
Internal: customer_id, employee_id, store_id, product_id
