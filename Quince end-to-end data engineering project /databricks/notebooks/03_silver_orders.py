from pyspark.sql import functions as F
from pyspark.sql.window import Window

BRONZE_TABLE = "quince_dev.bronze.orders"
SILVER_TABLE = "quince_dev.silver.orders"
QUARANTINE_TABLE = "quince_dev.audit.orders_invalid"

spark.sql("CREATE SCHEMA IF NOT EXISTS quince_dev.silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS quince_dev.audit")

# 1. Read Bronze
bronze_df = spark.table(BRONZE_TABLE)

print("Bronze columns:", bronze_df.columns)
print("Bronze row count:", bronze_df.count())

# 2. Identify the order business key
key_candidates = [
    "order_id",
    "orderid",
    "order_number",
    "order_no",
]

column_lookup = {c.lower(): c for c in bronze_df.columns}

order_key = next(
    (
        column_lookup[candidate]
        for candidate in key_candidates
        if candidate in column_lookup
    ),
    None,
)

if order_key is None:
    raise ValueError(
        "Could not identify the order key. "
        f"Available columns: {bronze_df.columns}. "
        "Set order_key to the correct source column."
    )

print(f"Using business key: {order_key}")

# 3. Standardize text values:
#    trim leading/trailing spaces and turn empty strings into NULL.
df = bronze_df

for field in df.schema.fields:
    if field.dataType.simpleString() == "string":
        df = df.withColumn(
            field.name,
            F.when(
                F.trim(F.col(field.name)) == "",
                F.lit(None).cast("string"),
            ).otherwise(F.trim(F.col(field.name))),
        )

# 4. Quarantine records without a business key.
invalid_df = (
    df.filter(
        F.col(order_key).isNull()
        | (F.trim(F.col(order_key).cast("string")) == "")
    )
    .withColumn("_dq_reason", F.lit("MISSING_ORDER_KEY"))
    .withColumn("_dq_detected_at_utc", F.current_timestamp())
)

invalid_count = invalid_df.count()

if invalid_count > 0:
    (
        invalid_df.write
        .format("delta")
        .mode("append")
        .saveAsTable(QUARANTINE_TABLE)
    )

valid_df = df.filter(
    F.col(order_key).isNotNull()
    & (F.trim(F.col(order_key).cast("string")) != "")
)

# 5. Deduplicate by order key.
#    Prefer the most recently ingested record when metadata exists.
if "_ingested_at_utc" in valid_df.columns:
    ordering = F.col("_ingested_at_utc").desc_nulls_last()
else:
    ordering = F.lit(1)

window_spec = (
    Window.partitionBy(order_key)
    .orderBy(ordering)
)

deduplicated_df = (
    valid_df
    .withColumn("_row_number", F.row_number().over(window_spec))
    .filter(F.col("_row_number") == 1)
    .drop("_row_number")
)

# 6. Add a parsed date helper when order_date exists.
#    Keep the original column so that source values remain inspectable.
if "order_date" in deduplicated_df.columns:
    raw_date = F.col("order_date").cast("string")

    deduplicated_df = deduplicated_df.withColumn(
        "order_date_parsed",
        F.coalesce(
            F.to_date(raw_date, "yyyy-MM-dd"),
            F.to_date(raw_date, "MM/dd/yyyy"),
            F.to_date(raw_date, "dd/MM/yyyy"),
            F.to_date(raw_date, "yyyy-MM-dd'T'HH:mm:ss"),
        ),
    )

# 7. Standardize common categorical fields if present.
for column_name in ["order_status", "currency"]:
    if column_name in deduplicated_df.columns:
        deduplicated_df = deduplicated_df.withColumn(
            column_name,
            F.upper(F.col(column_name)),
        )

# 8. Add a processing timestamp.
deduplicated_df = deduplicated_df.withColumn(
    "_silver_processed_at_utc",
    F.current_timestamp(),
)

# 9. Write Silver.
#    Initial/demo full refresh. Production incremental MERGE comes later.
(
    deduplicated_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_TABLE)
)

# 10. Report quality metrics.
source_count = bronze_df.count()
valid_count = valid_df.count()
silver_count = spark.table(SILVER_TABLE).count()

print(f"Source rows: {source_count}")
print(f"Rows quarantined: {invalid_count}")
print(f"Rows with a usable key: {valid_count}")
print(f"Silver rows after deduplication: {silver_count}")
print(f"Duplicate rows removed: {valid_count - silver_count}")
print(f"Silver table created: {SILVER_TABLE}")
