
# 08_silver_treasury.py
# Phase 3: Silver Treasury - Bank Statement Transactions

from pyspark.sql import functions as F
from pyspark.sql.window import Window
import re

SOURCE_TABLE = "quince_dev.bronze.bank_statement_transactions"
TARGET_TABLE = "quince_dev.silver.bank_statement_transactions"
INVALID_TABLE = "quince_dev.audit.bank_statement_transactions_invalid"


# --------------------------------------------------
# 1. Read and normalize source columns
# --------------------------------------------------
df = spark.table(SOURCE_TABLE)

def normalize_name(name):
    return re.sub(
        r"[^a-z0-9]+", "_", name.strip().lower()
    ).strip("_")

normalized_columns = [normalize_name(c) for c in df.columns]

if len(normalized_columns) != len(set(normalized_columns)):
    raise ValueError(
        "Column-name collision after normalization. "
        "Inspect the source headers."
    )

df = df.toDF(*normalized_columns)

for name, dtype in df.dtypes:
    if dtype == "string":
        df = df.withColumn(
            name,
            F.when(
                F.trim(F.col(name)) == "",
                F.lit(None)
            ).otherwise(F.trim(F.col(name)))
        )

columns = df.columns


# --------------------------------------------------
# 2. Detect source fields
# --------------------------------------------------
def find_column(candidates, required=False):
    for candidate in candidates:
        if candidate in columns:
            return candidate

    if required:
        raise ValueError(
            f"Could not find any of {candidates}. "
            f"Available columns: {columns}"
        )
    return None


bank_id_col = find_column([
    "bank_transaction_id",
    "transaction_id",
    "statement_line_id",
    "bank_entry_id",
    "entry_id",
    "transaction_reference",
])

date_col = find_column([
    "transaction_date",
    "posting_date",
    "value_date",
    "booking_date",
    "transaction_timestamp",
])

description_col = find_column([
    "description",
    "transaction_description",
    "narrative",
    "reference_text",
    "memo",
])

amount_col = find_column([
    "amount",
    "transaction_amount",
    "signed_amount",
])

debit_col = find_column([
    "debit_amount",
    "debit",
    "withdrawal_amount",
])

credit_col = find_column([
    "credit_amount",
    "credit",
    "deposit_amount",
])

currency_col = find_column([
    "currency",
    "currency_code",
])

type_col = find_column([
    "transaction_type",
    "debit_credit_indicator",
    "dr_cr",
    "direction",
])

account_col = find_column([
    "bank_account_id",
    "account_id",
    "account_number",
    "iban",
])

if not any([amount_col, debit_col, credit_col]):
    raise ValueError(
        "No amount, debit, or credit column found. "
        "Inspect the source schema."
    )

print("Bank transaction ID:", bank_id_col)
print("Date:", date_col)
print("Amount:", amount_col)
print("Debit:", debit_col)
print("Credit:", credit_col)
print("Currency:", currency_col)
print("Transaction type:", type_col)
print("Account:", account_col)


# --------------------------------------------------
# 3. Build standardized columns
# --------------------------------------------------
if bank_id_col:
    df = df.withColumn(
        "bank_transaction_id_clean",
        F.trim(F.col(bank_id_col).cast("string"))
    )
else:
    # Do not fabricate a unique ID from row order.
    # The composite fallback below is only a provisional
    # identity and must be validated against the bank export.
    df = df.withColumn(
        "bank_transaction_id_clean",
        F.lit(None).cast("string")
    )

if date_col:
    raw_date = F.col(date_col).cast("string")
    df = df.withColumn(
        "bank_transaction_timestamp_clean",
        F.coalesce(
            F.to_timestamp(raw_date),
            F.to_timestamp(raw_date, "yyyy-MM-dd HH:mm:ss"),
            F.to_timestamp(raw_date, "yyyy-MM-dd'T'HH:mm:ss"),
            F.to_timestamp(raw_date, "yyyy-MM-dd"),
            F.to_timestamp(raw_date, "MM/dd/yyyy HH:mm:ss"),
            F.to_timestamp(raw_date, "MM/dd/yyyy"),
        )
    )
else:
    df = df.withColumn(
        "bank_transaction_timestamp_clean",
        F.lit(None).cast("timestamp")
    )

if description_col:
    df = df.withColumn(
        "bank_description_clean",
        F.trim(F.col(description_col).cast("string"))
    )

if currency_col:
    df = df.withColumn(
        "currency_clean",
        F.upper(F.trim(F.col(currency_col).cast("string")))
    )

if account_col:
    df = df.withColumn(
        "bank_account_clean",
        F.trim(F.col(account_col).cast("string"))
    )

# Parse monetary columns without silently treating
# invalid numeric values as zero.
def decimal_col(name):
    if name:
        return F.expr(f"try_cast(`{name}` AS DECIMAL(18,2))")
    return F.lit(None).cast("decimal(18,2)")

if amount_col:
    df = df.withColumn("source_amount_clean", decimal_col(amount_col))

if debit_col:
    df = df.withColumn("debit_amount_clean", decimal_col(debit_col))

if credit_col:
    df = df.withColumn("credit_amount_clean", decimal_col(credit_col))

# Prefer the signed amount when present. Otherwise calculate
# credits minus debits, assuming both are stored as positive
# magnitudes. Confirm this assumption against the source.
if amount_col:
    df = df.withColumn(
        "signed_amount_clean",
        F.col("source_amount_clean")
    )
else:
    debit_expr = (
        F.col("debit_amount_clean")
        if debit_col else F.lit(0).cast("decimal(18,2)")
    )
    credit_expr = (
        F.col("credit_amount_clean")
        if credit_col else F.lit(0).cast("decimal(18,2)")
    )

    df = df.withColumn(
        "signed_amount_clean",
        credit_expr - debit_expr
    )

if type_col:
    df = df.withColumn(
        "transaction_type_clean",
        F.upper(F.trim(F.col(type_col).cast("string")))
    )


# --------------------------------------------------
# 4. Data quality checks
# --------------------------------------------------
# Require an amount and a transaction date.
# A stable source ID is preferred. If absent, the notebook
# fails rather than inventing a false unique identifier.
if not bank_id_col:
    raise ValueError(
        "No stable bank transaction identifier was detected. "
        "Inspect the export and define a reliable source key "
        "or a documented composite key before deduplication."
    )

df = df.withColumn(
    "_dq_reason",
    F.when(
        F.col("bank_transaction_id_clean").isNull()
        | (F.col("bank_transaction_id_clean") == ""),
        F.lit("MISSING_BANK_TRANSACTION_KEY")
    ).when(
        F.col("signed_amount_clean").isNull(),
        F.lit("MISSING_OR_INVALID_AMOUNT")
    ).when(
        F.col("bank_transaction_timestamp_clean").isNull(),
        F.lit("MISSING_OR_INVALID_TRANSACTION_DATE")
    )
)

invalid_df = (
    df.filter(F.col("_dq_reason").isNotNull())
      .withColumn("_dq_detected_at_utc", F.current_timestamp())
)

valid_df = (
    df.filter(F.col("_dq_reason").isNull())
      .drop("_dq_reason")
)

invalid_count = invalid_df.count()

if invalid_count > 0:
    (
        invalid_df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(INVALID_TABLE)
    )

print("Quarantined bank rows:", invalid_count)


# --------------------------------------------------
# 5. Deduplicate
# --------------------------------------------------
# If source updates exist, prefer the latest ingestion version.
# A stable tie-breaker should be added if ingestion timestamps
# can be identical for multiple versions.
if "_ingested_at_utc" in valid_df.columns:
    dedup_window = (
        Window.partitionBy("bank_transaction_id_clean")
        .orderBy(F.col("_ingested_at_utc").desc_nulls_last())
    )
else:
    dedup_window = (
        Window.partitionBy("bank_transaction_id_clean")
        .orderBy(F.lit(1))
    )

valid_df = (
    valid_df
    .withColumn("_dedup_rank", F.row_number().over(dedup_window))
    .filter(F.col("_dedup_rank") == 1)
    .drop("_dedup_rank")
    .withColumn("_silver_processed_at_utc", F.current_timestamp())
)


# --------------------------------------------------
# 6. Write Silver table
# --------------------------------------------------
(
    valid_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

print("Silver table:", TARGET_TABLE)
print("Silver row count:", spark.table(TARGET_TABLE).count())
