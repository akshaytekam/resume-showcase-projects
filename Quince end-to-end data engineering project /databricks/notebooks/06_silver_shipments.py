
# Databricks notebook source
# 06_silver_shipments.py

from pyspark.sql import functions as F
from pyspark.sql.window import Window

BRONZE_TABLE = "quince_dev.bronze.shipments"
SILVER_TABLE = "quince_dev.silver.shipments"
QUARANTINE_TABLE = "quince_dev.audit.shipments_invalid"

spark.sql("CREATE SCHEMA IF NOT EXISTS quince_dev.silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS quince_dev.audit")

# 1. Read Bronze and inspect its schema.
df = spark.table(BRONZE_TABLE)

print("Source columns:", df.columns)
print("Source row count:", df.count())

# 2. Normalize simple column names and detect collisions.
original_columns = df.columns
normalized_columns = [
    c.strip().lower().replace(" ", "_")
    for c in original_columns
]

if len(normalized_columns) != len(set(normalized_columns)):
    raise ValueError(
        "Column normalization caused duplicate names. "
        f"Original columns: {original_columns}"
    )

for old_name, new_name in zip(original_columns, normalized_columns):
    if old_name != new_name:
        df = df.withColumnRenamed(old_name, new_name)

columns = set(df.columns)


def find_column(candidates, available_columns):
    for candidate in candidates:
        if candidate in available_columns:
            return candidate
    return None


# 3. Standardize text values.
for field in df.schema.fields:
    if field.dataType.simpleString() == "string":
        df = df.withColumn(
            field.name,
            F.when(
                F.trim(F.col(field.name)) == "",
                F.lit(None).cast("string"),
            ).otherwise(F.trim(F.col(field.name))),
        )

# 4. Identify important shipment fields.
shipment_key = find_column(
    ["shipment_id", "shipmentid", "shipment_number"],
    columns,
)

order_key = find_column(
    ["order_id", "orderid", "order_number", "order_no"],
    columns,
)

status_col = find_column(
    ["shipment_status", "status", "delivery_status"],
    columns,
)

ship_date_col = find_column(
    ["ship_date", "shipped_at", "dispatch_date", "shipment_date"],
    columns,
)

promised_date_col = find_column(
    [
        "promised_delivery_date",
        "expected_delivery_date",
        "estimated_delivery_date",
    ],
    columns,
)

actual_date_col = find_column(
    [
        "actual_delivery_date",
        "delivered_at",
        "delivery_date",
    ],
    columns,
)

if shipment_key is None:
    raise ValueError(
        "Shipment key not found. Configure the correct business key. "
        f"Available columns: {df.columns}"
    )

if order_key is None:
    raise ValueError(
        "Order key not found. Configure the correct source column. "
        f"Available columns: {df.columns}"
    )

print("Shipment key:", shipment_key)
print("Order key:", order_key)
print("Status column:", status_col)
print("Ship date column:", ship_date_col)
print("Promised delivery column:", promised_date_col)
print("Actual delivery column:", actual_date_col)

# 5. Parse date/timestamp fields into standard columns.
# Original source columns are retained for traceability.
date_mappings = {
    "ship_date_clean": ship_date_col,
    "promised_delivery_date_clean": promised_date_col,
    "actual_delivery_date_clean": actual_date_col,
}

def parse_date(column_name):
    raw = F.col(column_name).cast("string")

    return F.coalesce(
        F.to_timestamp(raw, "yyyy-MM-dd'T'HH:mm:ssXXX"),
        F.to_timestamp(raw, "yyyy-MM-dd HH:mm:ss"),
        F.to_timestamp(raw, "yyyy-MM-dd"),
        F.to_timestamp(raw, "MM/dd/yyyy"),
        F.to_timestamp(raw, "dd/MM/yyyy"),
    )


for output_col, source_col in date_mappings.items():
    if source_col:
        df = df.withColumn(output_col, parse_date(source_col))

# 6. Normalize shipment status if available.
if status_col:
    df = df.withColumn(
        "shipment_status_clean",
        F.upper(F.trim(F.col(status_col))),
    )

# 7. Quarantine rows missing shipment or order identifiers.
missing_key = (
    F.col(shipment_key).isNull()
    | (F.trim(F.col(shipment_key).cast("string")) == "")
    | F.col(order_key).isNull()
    | (F.trim(F.col(order_key).cast("string")) == "")
)

invalid_df = (
    df.filter(missing_key)
    .withColumn("_dq_reason", F.lit("MISSING_SHIPMENT_OR_ORDER_KEY"))
    .withColumn("_dq_detected_at_utc", F.current_timestamp())
)

if invalid_df.limit(1).count() > 0:
    (
        invalid_df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(QUARANTINE_TABLE)
    )

valid_df = df.filter(~missing_key)

# 8. Quarantine invalid date sequences when dates are available.
date_error = F.lit(False)

if ship_date_col and actual_date_col:
    date_error = (
        F.col("ship_date_clean").isNotNull()
        & F.col("actual_delivery_date_clean").isNotNull()
        & (
            F.col("actual_delivery_date_clean")
            < F.col("ship_date_clean")
        )
    )

date_invalid_df = (
    valid_df.filter(date_error)
    .withColumn(
        "_dq_reason",
        F.lit("DELIVERY_BEFORE_SHIPMENT"),
    )
    .withColumn("_dq_detected_at_utc", F.current_timestamp())
)

if date_invalid_df.limit(1).count() > 0:
    (
        date_invalid_df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(QUARANTINE_TABLE)
    )

valid_df = valid_df.filter(~date_error)

# 9. Validate order references against Silver orders when available.
if spark.catalog.tableExists("quince_dev.silver.orders"):
    orders = spark.table("quince_dev.silver.orders")
    order_columns = {c.lower(): c for c in orders.columns}

    silver_order_key = next(
        (
            order_columns[c]
            for c in ["order_id", "orderid", "order_number", "order_no"]
            if c in order_columns
        ),
        None,
    )

    if silver_order_key is None:
        raise ValueError(
            "Could not identify the order key in Silver orders."
        )

    order_keys = (
        orders.select(
            F.col(silver_order_key)
            .cast("string")
            .alias("_valid_order_id")
        )
        .distinct()
    )

    checked = valid_df.join(
        order_keys,
        valid_df[order_key].cast("string")
        == order_keys["_valid_order_id"],
        "left",
    )

    orphan_df = (
        checked.filter(F.col("_valid_order_id").isNull())
        .drop("_valid_order_id")
        .withColumn("_dq_reason", F.lit("ORDER_NOT_FOUND"))
        .withColumn("_dq_detected_at_utc", F.current_timestamp())
    )

    if orphan_df.limit(1).count() > 0:
        (
            orphan_df.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(QUARANTINE_TABLE)
        )

    valid_df = (
        checked.filter(F.col("_valid_order_id").isNotNull())
        .drop("_valid_order_id")
    )

# 10. Deduplicate by shipment ID.
# Prefer the most recently ingested record where available.
if "_ingested_at_utc" in valid_df.columns:
    ordering = F.col("_ingested_at_utc").desc_nulls_last()
else:
    ordering = F.lit(1)

dedup_window = (
    Window.partitionBy(shipment_key)
    .orderBy(ordering)
)

silver_df = (
    valid_df
    .withColumn("_row_number", F.row_number().over(dedup_window))
    .filter(F.col("_row_number") == 1)
    .drop("_row_number")
    .withColumn("_silver_processed_at_utc", F.current_timestamp())
)

# 11. Initial/demo full refresh.
# Production incremental MERGE and CDC will be added later.
(
    silver_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_TABLE)
)

print("Silver table:", SILVER_TABLE)
print("Rows written:", spark.table(SILVER_TABLE).count())
print("Rows quarantined for missing keys:", invalid_df.count())
print("Rows quarantined for invalid dates:", date_invalid_df.count())
