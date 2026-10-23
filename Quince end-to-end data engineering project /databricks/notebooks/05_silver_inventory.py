
# Databricks notebook source
# 05_silver_inventory.py

from pyspark.sql import functions as F
from pyspark.sql.window import Window

BRONZE_TABLE = "quince_dev.bronze.inventory_snapshots"
SILVER_TABLE = "quince_dev.silver.inventory"
QUARANTINE_TABLE = "quince_dev.audit.inventory_invalid"

spark.sql("CREATE SCHEMA IF NOT EXISTS quince_dev.silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS quince_dev.audit")

# 1. Read Bronze and normalize column names.
df = spark.table(BRONZE_TABLE)

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

print("Inventory source columns:", df.columns)
print("Source row count:", df.count())

# 2. Standardize text: trim spaces and convert blanks to NULL.
for field in df.schema.fields:
    if field.dataType.simpleString() == "string":
        df = df.withColumn(
            field.name,
            F.when(
                F.trim(F.col(field.name)) == "",
                F.lit(None).cast("string"),
            ).otherwise(F.trim(F.col(field.name))),
        )

# 3. Identify common source column names.
def find_column(candidates, columns):
    for candidate in candidates:
        if candidate in columns:
            return candidate
    return None


columns = set(df.columns)

product_col = find_column(
    ["product_id", "productid", "sku", "product_code"],
    columns,
)

date_col = find_column(
    [
        "snapshot_date",
        "inventory_date",
        "snapshot_timestamp",
        "snapshot_time",
        "as_of_date",
    ],
    columns,
)

if product_col is None or date_col is None:
    raise ValueError(
        "Could not identify product key and snapshot date. "
        f"Available columns: {df.columns}. "
        "Configure product_col and date_col to match the source."
    )

# 4. Create standardized fields without deleting source columns.
df = (
    df
    .withColumn(
        "product_id_clean",
        F.trim(F.col(product_col).cast("string")),
    )
    .withColumn(
        "snapshot_date_clean",
        F.coalesce(
            F.to_date(F.col(date_col).cast("string"), "yyyy-MM-dd"),
            F.to_date(F.col(date_col).cast("string"), "MM/dd/yyyy"),
            F.to_date(F.col(date_col).cast("string"), "dd/MM/yyyy"),
            F.to_date(F.col(date_col).cast("string")),
        ),
    )
)

# If the source is a timestamp, the date is retained in snapshot_date_clean.
# If hourly snapshots are required, use the full timestamp as the grain instead.

# 5. Identify available quantity columns.
quantity_candidates = {
    "quantity_on_hand": [
        "quantity_on_hand", "qty_on_hand", "on_hand_qty",
        "on_hand", "stock_on_hand",
    ],
    "quantity_reserved": [
        "quantity_reserved", "reserved_qty", "qty_reserved",
    ],
    "quantity_available": [
        "quantity_available", "available_qty", "qty_available",
    ],
}

found_quantity_columns = {}

for canonical_name, candidates in quantity_candidates.items():
    source_col = find_column(candidates, columns)

    if source_col:
        found_quantity_columns[canonical_name] = source_col

        # Safe conversion: nonnumeric source values become NULL.
        df = df.withColumn(
            f"{canonical_name}_clean",
            F.expr(
                f"try_cast(`{source_col}` AS DECIMAL(18,4))"
            ),
        )

print("Quantity mappings:", found_quantity_columns)

# 6. Build quarantine reason conditions.
missing_product = (
    F.col("product_id_clean").isNull()
    | (F.col("product_id_clean") == "")
)

missing_snapshot_date = F.col("snapshot_date_clean").isNull()

invalid_quantity = F.lit(False)

for canonical_name, source_col in found_quantity_columns.items():
    clean_col = f"{canonical_name}_clean"

    # Only reject a nonblank source quantity that failed conversion.
    invalid_quantity = (
        invalid_quantity
        | (
            F.col(source_col).isNotNull()
            & (F.trim(F.col(source_col).cast("string")) != "")
            & F.col(clean_col).isNull()
        )
    )

invalid_condition = (
    missing_product
    | missing_snapshot_date
    | invalid_quantity
)

invalid_df = (
    df.filter(invalid_condition)
    .withColumn(
        "_dq_reason",
        F.when(missing_product, "MISSING_PRODUCT_KEY")
        .when(missing_snapshot_date, "INVALID_SNAPSHOT_DATE")
        .when(invalid_quantity, "INVALID_QUANTITY")
        .otherwise("OTHER_VALIDATION_ERROR"),
    )
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

valid_df = df.filter(~invalid_condition)

# 7. Validate product references if the Silver products dimension exists.
if spark.catalog.tableExists("quince_dev.silver.products"):
    products = spark.table("quince_dev.silver.products")
    product_columns = {c.lower(): c for c in products.columns}

    product_dimension_key = next(
        (
            product_columns[c]
            for c in ["product_id", "productid", "sku", "product_code"]
            if c in product_columns
        ),
        None,
    )

    if product_dimension_key is None:
        raise ValueError(
            "Cannot find the product key in Silver products."
        )

    product_keys = (
        products.select(
            F.col(product_dimension_key)
            .cast("string")
            .alias("_valid_product_id")
        )
        .distinct()
    )

    checked_df = valid_df.join(
        product_keys,
        valid_df["product_id_clean"] == product_keys["_valid_product_id"],
        "left",
    )

    orphan_df = (
        checked_df.filter(F.col("_valid_product_id").isNull())
        .drop("_valid_product_id")
        .withColumn("_dq_reason", F.lit("PRODUCT_NOT_FOUND"))
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
        checked_df.filter(F.col("_valid_product_id").isNotNull())
        .drop("_valid_product_id")
    )

# 8. Define the inventory snapshot grain.
# Add a location key when one exists.
location_col = find_column(
    ["warehouse_id", "location_id", "warehouse_code", "location_code"],
    columns,
)

grain_columns = ["product_id_clean", "snapshot_date_clean"]

if location_col:
    df_with_location = valid_df.withColumn(
        "location_id_clean",
        F.col(location_col).cast("string"),
    )
    valid_df = df_with_location
    grain_columns.append("location_id_clean")

# If multiple snapshots per day are legitimate, use the full timestamp
# in the grain instead of snapshot_date_clean.

# 9. Deduplicate at the chosen snapshot grain.
if "_ingested_at_utc" in valid_df.columns:
    ordering = F.col("_ingested_at_utc").desc_nulls_last()
else:
    ordering = F.lit(1)

dedup_window = (
    Window.partitionBy(*grain_columns)
    .orderBy(ordering)
)

silver_df = (
    valid_df
    .withColumn("_row_number", F.row_number().over(dedup_window))
    .filter(F.col("_row_number") == 1)
    .drop("_row_number")
    .withColumn("_silver_processed_at_utc", F.current_timestamp())
)

# 10. Initial/demo full refresh.
# Incremental MERGE will be implemented in a later step.
(
    silver_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_TABLE)
)

print("Silver table:", SILVER_TABLE)
print("Snapshot grain:", grain_columns)
print("Source rows:", df.count())
print("Quarantined rows:", invalid_df.count())
print("Silver rows:", spark.table(SILVER_TABLE).count())
