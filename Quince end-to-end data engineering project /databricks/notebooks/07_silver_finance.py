
# 07_silver_finance.py
# Phase 3: Silver Finance - Payment Transactions
#
# Source: quince_dev.bronze.payment_transactions
# Target: quince_dev.silver.payment_transactions
# Quarantine: quince_dev.audit.payment_transactions_invalid

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from functools import reduce

SOURCE_TABLE = "quince_dev.bronze.payment_transactions"
TARGET_TABLE = "quince_dev.silver.payment_transactions"
INVALID_TABLE = "quince_dev.audit.payment_transactions_invalid"
ORDERS_TABLE = "quince_dev.silver.orders"

# --------------------------------------------------
# 1. Read source
# --------------------------------------------------
df = spark.table(SOURCE_TABLE)

if not df.columns:
    raise ValueError(f"No columns found in {SOURCE_TABLE}")

# Normalize names, e.g. Transaction ID -> transaction_id
def normalize_name(name):
    import re
    name = name.strip().lower()
    return re.sub(r"[^a-z0-9]+", "_", name).strip("_")

normalized_names = [normalize_name(c) for c in df.columns]

if len(normalized_names) != len(set(normalized_names)):
    raise ValueError(
        "Column-name collision after normalization. "
        "Inspect the source headers before proceeding."
    )

df = df.toDF(*normalized_names)

# Trim strings and convert blanks to null.
for name, dtype in df.dtypes:
    if dtype == "string":
        df = df.withColumn(
            name,
            F.when(
                F.trim(F.col(name)) == "",
                F.lit(None)
            ).otherwise(F.trim(F.col(name)))
        )

# --------------------------------------------------
# 2. Detect columns using common aliases
# --------------------------------------------------
def find_column(candidates, required=True):
    for name in candidates:
        if name in df.columns:
            return name

    if required:
        raise ValueError(
            f"Could not find any of {candidates}. "
            f"Available columns: {df.columns}"
        )
    return None

transaction_key = find_column([
    "payment_transaction_id",
    "transaction_id",
    "payment_id",
    "gateway_transaction_id",
    "transaction_reference",
])

amount_col = find_column([
    "amount",
    "transaction_amount",
    "payment_amount",
    "captured_amount",
])

order_key = find_column([
    "order_id",
    "orderid",
    "order_number",
    "order_no",
], required=False)

currency_col = find_column([
    "currency",
    "currency_code",
], required=False)

status_col = find_column([
    "status",
    "payment_status",
    "transaction_status",
], required=False)

type_col = find_column([
    "transaction_type",
    "payment_type",
    "type",
], required=False)

timestamp_col = find_column([
    "transaction_timestamp",
    "transaction_date",
    "payment_timestamp",
    "payment_date",
    "created_at",
], required=False)

print("Transaction key:", transaction_key)
print("Amount column:", amount_col)
print("Order key:", order_key)
print("Currency column:", currency_col)
print("Status column:", status_col)
print("Transaction type:", type_col)
print("Timestamp column:", timestamp_col)

# --------------------------------------------------
# 3. Create standardized columns
# --------------------------------------------------
df = (
    df
    .withColumn(
        "payment_transaction_id_clean",
        F.trim(F.col(transaction_key).cast("string"))
    )
    .withColumn(
        "amount_clean",
        F.expr(
            f"try_cast(`{amount_col}` AS DECIMAL(18,2))"
        )
    )
)

if order_key:
    df = df.withColumn(
        "order_id_clean",
        F.trim(F.col(order_key).cast("string"))
    )

if currency_col:
    df = df.withColumn(
        "currency_clean",
        F.upper(F.trim(F.col(currency_col).cast("string")))
    )

if status_col:
    df = df.withColumn(
        "payment_status_clean",
        F.upper(F.trim(F.col(status_col).cast("string")))
    )

if type_col:
    df = df.withColumn(
        "transaction_type_clean",
        F.upper(F.trim(F.col(type_col).cast("string")))
    )

if timestamp_col:
    raw_timestamp = F.col(timestamp_col).cast("string")

    parsed_timestamp = F.coalesce(
        F.to_timestamp(raw_timestamp),
        F.to_timestamp(raw_timestamp, "yyyy-MM-dd HH:mm:ss"),
        F.to_timestamp(raw_timestamp, "yyyy-MM-dd'T'HH:mm:ss"),
        F.to_timestamp(raw_timestamp, "yyyy-MM-dd"),
        F.to_timestamp(raw_timestamp, "MM/dd/yyyy HH:mm:ss"),
    )

    df = df.withColumn(
        "transaction_timestamp_clean",
        parsed_timestamp
    )

# Preserve negative amounts when the source represents refunds
# using negative values. Do not reject them automatically.

# --------------------------------------------------
# 4. Identify invalid records
# --------------------------------------------------
reason = F.when(
    F.col("payment_transaction_id_clean").isNull()
    | (F.col("payment_transaction_id_clean") == ""),
    F.lit("MISSING_TRANSACTION_KEY")
).when(
    F.col("amount_clean").isNull(),
    F.lit("MISSING_OR_INVALID_AMOUNT")
)

# Validate order references when the source has an order key
# and the Silver orders table is available.
if order_key and spark.catalog.tableExists(ORDERS_TABLE):
    orders = (
        spark.table(ORDERS_TABLE)
        .select(F.col("order_id").cast("string").alias("_valid_order_id"))
        .distinct()
    )

    df = df.join(
        F.broadcast(orders),
        df["order_id_clean"] == orders["_valid_order_id"],
        "left"
    )

    reason = reason.when(
        F.col("order_id_clean").isNotNull()
        & (F.col("order_id_clean") != "")
        & F.col("_valid_order_id").isNull(),
        F.lit("ORPHAN_ORDER_REFERENCE")
    )

invalid_df = (
    df.withColumn("_dq_reason", reason)
      .filter(F.col("_dq_reason").isNotNull())
      .withColumn("_dq_detected_at_utc", F.current_timestamp())
)

valid_df = df.filter(
    F.col("payment_transaction_id_clean").isNotNull()
    & (F.col("payment_transaction_id_clean") != "")
    & F.col("amount_clean").isNotNull()
)

if "_valid_order_id" in valid_df.columns:
    valid_df = valid_df.drop("_valid_order_id")

# Remove helper column from quarantine output as well.
if "_valid_order_id" in invalid_df.columns:
    invalid_df = invalid_df.drop("_valid_order_id")

invalid_count = invalid_df.count()

if invalid_count > 0:
    (
        invalid_df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(INVALID_TABLE)
    )

# --------------------------------------------------
# 5. Deduplicate by transaction key
# --------------------------------------------------
# Prefer the latest ingestion timestamp if available.
if "_ingested_at_utc" in valid_df.columns:
    window_spec = (
        Window.partitionBy("payment_transaction_id_clean")
        .orderBy(F.col("_ingested_at_utc").desc_nulls_last())
    )
else:
    window_spec = Window.partitionBy(
        "payment_transaction_id_clean"
    ).orderBy(F.lit(1))

valid_df = (
    valid_df
    .withColumn("_dedup_rank", F.row_number().over(window_spec))
    .filter(F.col("_dedup_rank") == 1)
    .drop("_dedup_rank")
)

# Remove the temporary join column if still present.
if "_valid_order_id" in valid_df.columns:
    valid_df = valid_df.drop("_valid_order_id")

valid_df = valid_df.withColumn(
    "_silver_processed_at_utc",
    F.current_timestamp()
)

# --------------------------------------------------
# 6. Write Silver output
# --------------------------------------------------
(
    valid_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

print(f"Silver table: {TARGET_TABLE}")
print(f"Silver rows: {spark.table(TARGET_TABLE).count()}")
print(f"Quarantined rows in this run: {invalid_count}")
