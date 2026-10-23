
# Databricks notebook: 12_gold_finance.py
# Purpose: Validate inputs for the Gold Finance layer.

from pyspark.sql import functions as F

SILVER_SCHEMA = "quince_dev.silver"

REQUIRED_TABLES = [
    "payment_transactions",
    "returns",
    "general_ledger_entries",
    "finance_budget",
]

# 1. Check which required tables exist
existing_tables = {
    row.tableName.lower()
    for row in spark.sql(f"SHOW TABLES IN {SILVER_SCHEMA}").collect()
}

missing_tables = [
    table
    for table in REQUIRED_TABLES
    if table.lower() not in existing_tables
]

print("Required Silver tables:", REQUIRED_TABLES)
print("Missing Silver tables:", missing_tables)

# 2. Stop early if dependencies are missing
if missing_tables:
    raise ValueError(
        "Gold Finance cannot run yet. Create these Silver tables first: "
        + ", ".join(missing_tables)
    )

# 3. Inspect each table's schema and sample records
for table in REQUIRED_TABLES:
    full_name = f"{SILVER_SCHEMA}.{table}"
    df = spark.table(full_name)

    print(f"\n{'=' * 70}")
    print(f"TABLE: {full_name}")
    print(f"ROW COUNT: {df.count()}")
    print("COLUMNS:", df.columns)

    df.printSchema()
    display(df.limit(5))

print("\nPreflight completed. Review the columns before KPI calculations.")
