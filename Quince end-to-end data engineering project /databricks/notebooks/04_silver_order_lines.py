from pyspark.sql import functions as F
from pyspark.sql.window import Window

BRONZE_TABLE = "quince_dev.bronze.order_lines"
ORDERS_TABLE = "quince_dev.silver.orders"
SILVER_TABLE = "quince_dev.silver.order_lines"
QUARANTINE_TABLE = "quince_dev.audit.order_lines_invalid"

spark.sql("CREATE SCHEMA IF NOT EXISTS quince_dev.silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS quince_dev.audit")

# 1. Read Bronze
df = spark.table(BRONZE_TABLE)

print("Source columns:", df.columns)
print("Source row count:", df.count())

# 2. Normalize simple column names.
# This assumes normalization does not create duplicate names.
for old_name in df.columns:
    new_name = old_name.strip().lower().replace(" ", "_")
    if old_name != new_name:
        df = df.withColumnRenamed(old_name, new_name)

if len(df.columns) != len(set(df.columns)):
    raise ValueError(
        "Column-name normalization created duplicate names. "
        "Resolve the source headers before continuing."
    )

# 3. Trim text and convert empty strings to NULL.
for field in df.schema.fields:
    if field.dataType.simpleString() == "string":
        df = df.withColumn(
            field.name,
            F.when(
                F.trim(F.col(field.name)) == "",
                F.lit(None).cast("string"),
            ).otherwise(F.trim(F.col(field.name))),
        )

# 4. Detect common order and line key names.
def find_column(candidates, available_columns):
    for candidate in candidates:
        if candidate in available_columns:
            return candidate
    return None

available = set(df.columns)

order_key = find_column(
    ["order_id", "orderid", "order_number", "order_no"],
    available,
)

line_key = find_column(
    [
        "order_line_id",
        "line_id",
        "order_item_id",
        "line_item_id",
    ],
    available,
)

if order_key is None:
    raise ValueError(
        "Order key not found. Configure order_key using "
        f"the actual source columns: {df.columns}"
    )

# Prefer a real source line identifier. If none exists, use a
# provisional composite key only after confirming business grain.
if line_key:
    dedup_keys = [line_key]
elif "line_number" in available:
    dedup_keys = [order_key, "line_number"]
else:
    raise ValueError(
        "No reliable order-line key detected. Configure dedup_keys "
        "after inspecting the source data. Do not deduplicate by "
        "order_id alone because an order can contain multiple items."
    )

print("Order key:", order_key)
print("Line deduplication key:", dedup_keys)

# 5. Split records with missing required keys.
missing_key_condition = F.lit(False)

for key in set([order_key] + dedup_keys):
    missing_key_condition = (
        missing_key_condition
        | F.col(key).isNull()
        | (F.trim(F.col(key).cast("string")) == "")
    )

invalid_keys = (
    df.filter(missing_key_condition)
    .withColumn("_dq_reason", F.lit("MISSING_ORDER_OR_LINE_KEY"))
    .withColumn("_dq_detected_at_utc", F.current_timestamp())
)

if invalid_keys.limit(1).count() > 0:
    (
        invalid_keys.write
        .format("delta")
        .mode("append")
        .saveAsTable(QUARANTINE_TABLE)
    )

valid = df.filter(~missing_key_condition)

# 6. Validate quantity if present.
quantity_candidates = ["quantity", "qty", "ordered_quantity"]
quantity_col = find_column(quantity_candidates, available)

if quantity_col:
    valid = valid.withColumn(
        "_quantity_numeric",
        F.expr(
            f"try_cast(`{quantity_col}` AS DECIMAL(18,4))"
        ),
    )

    invalid_quantity = (
        valid.filter(
            F.col(quantity_col).isNotNull()
            & (
                F.col("_quantity_numeric").isNull()
                | (F.col("_quantity_numeric") <= 0)
            )
        )
        .withColumn("_dq_reason", F.lit("INVALID_QUANTITY"))
        .withColumn("_dq_detected_at_utc", F.current_timestamp())
    )

    if invalid_quantity.limit(1).count() > 0:
        (
            invalid_quantity.write
            .format("delta")
            .mode("append")
            .saveAsTable(QUARANTINE_TABLE)
        )

    valid = valid.filter(
        F.col("_quantity_numeric").isNotNull()
        & (F.col("_quantity_numeric") > 0)
    ).drop("_quantity_numeric")

# 7. Check order references against Silver orders.
orders = spark.table(ORDERS_TABLE)

orders_lookup = {c.lower(): c for c in orders.columns}
silver_order_key = next(
    (
        orders_lookup[candidate]
        for candidate in [
            "order_id", "orderid", "order_number", "order_no"
        ]
        if candidate in orders_lookup
    ),
    None,
)

if silver_order_key is None:
    raise ValueError(
        "Could not identify the order key in Silver orders."
    )

valid = valid.withColumn(
    "_join_order_key",
    F.col(order_key).cast("string"),
)

order_keys = (
    orders.select(
        F.col(silver_order_key).cast("string").alias("_valid_order_key")
    )
    .distinct()
)

checked = valid.join(
    order_keys,
    valid["_join_order_key"] == order_keys["_valid_order_key"],
    "left",
)

orphan_lines = (
    checked.filter(F.col("_valid_order_key").isNull())
    .drop("_valid_order_key", "_join_order_key")
    .withColumn("_dq_reason", F.lit("ORDER_NOT_FOUND"))
    .withColumn("_dq_detected_at_utc", F.current_timestamp())
)

if orphan_lines.limit(1).count() > 0:
    (
        orphan_lines.write
        .format("delta")
        .mode("append")
        .saveAsTable(QUARANTINE_TABLE)
    )

valid = (
    checked.filter(F.col("_valid_order_key").isNotNull())
    .drop("_valid_order_key", "_join_order_key")
)

# 8. Deduplicate only using the configured line key.
# Prefer the latest ingestion timestamp when available.
if "_ingested_at_utc" in valid.columns:
    order_expr = F.col("_ingested_at_utc").desc_nulls_last()
else:
    order_expr = F.lit(1)

dedup_window = (
    Window.partitionBy(*dedup_keys)
    .orderBy(order_expr)
)

silver_df = (
    valid
    .withColumn("_row_number", F.row_number().over(dedup_window))
    .filter(F.col("_row_number") == 1)
    .drop("_row_number")
    .withColumn("_silver_processed_at_utc", F.current_timestamp())
)

# 9. Initial demonstration full refresh.
# Production incremental MERGE will be implemented later.
(
    silver_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_TABLE)
)

print("Silver table:", SILVER_TABLE)
print("Silver rows:", spark.table(SILVER_TABLE).count())
print("Configured line key:", dedup_keys)
