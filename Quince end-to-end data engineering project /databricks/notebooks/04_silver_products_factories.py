from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark.sql("CREATE SCHEMA IF NOT EXISTS quince_dev.silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS quince_dev.audit")


def normalize_columns(df):
    """Normalize simple headers and reject name collisions."""
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

    for old_name, new_name in zip(
        original_columns, normalized_columns
    ):
        if old_name != new_name:
            df = df.withColumnRenamed(old_name, new_name)

    return df


def find_key(columns, candidates, dataset_name):
    for candidate in candidates:
        if candidate in columns:
            return candidate

    raise ValueError(
        f"No key found for {dataset_name}. "
        f"Actual columns: {columns}. "
        "Configure the correct business key."
    )


def clean_dimension(
    source_table,
    target_table,
    quarantine_table,
    key_candidates,
    dataset_name,
):
    print(f"\nProcessing {dataset_name}")

    df = spark.table(source_table)
    print("Source columns:", df.columns)

    df = normalize_columns(df)

    if df.limit(1).count() == 0:
        raise ValueError(
            f"{source_table} contains no data rows."
        )

    # Trim string fields and normalize blank values.
    for field in df.schema.fields:
        if field.dataType.simpleString() == "string":
            df = df.withColumn(
                field.name,
                F.when(
                    F.trim(F.col(field.name)) == "",
                    F.lit(None).cast("string"),
                ).otherwise(F.trim(F.col(field.name))),
            )

    key = find_key(
        set(df.columns),
        key_candidates,
        dataset_name,
    )
    print("Business key:", key)

    # Quarantine records with no usable key.
    invalid = (
        df.filter(
            F.col(key).isNull()
            | (F.trim(F.col(key).cast("string")) == "")
        )
        .withColumn(
            "_dq_reason",
            F.lit(f"MISSING_{key.upper()}"),
        )
        .withColumn(
            "_dq_detected_at_utc",
            F.current_timestamp(),
        )
    )

    invalid_count = invalid.count()

    if invalid_count > 0:
        (
            invalid.write
            .format("delta")
            .mode("append")
            .saveAsTable(quarantine_table)
        )

    valid = df.filter(
        F.col(key).isNotNull()
        & (F.trim(F.col(key).cast("string")) != "")
    )

    # For repeated keys, prefer the most recently ingested record.
    # Without source update timestamps, this is only a provisional rule.
    if "_ingested_at_utc" in valid.columns:
        order_expr = F.col(
            "_ingested_at_utc"
        ).desc_nulls_last()
    else:
        order_expr = F.lit(1)

    dedup_window = (
        Window.partitionBy(key)
        .orderBy(order_expr)
    )

    cleaned = (
        valid
        .withColumn(
            "_row_number",
            F.row_number().over(dedup_window),
        )
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
        .withColumn(
            "_silver_processed_at_utc",
            F.current_timestamp(),
        )
    )

    # Initial/demo full refresh. Incremental MERGE will come later.
    (
        cleaned.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )

    print("Target:", target_table)
    print("Rows written:", spark.table(target_table).count())
    print("Rows quarantined:", invalid_count)


# Product master data
clean_dimension(
    source_table="quince_dev.bronze.dim_products",
    target_table="quince_dev.silver.products",
    quarantine_table="quince_dev.audit.products_invalid",
    key_candidates=[
        "product_id",
        "productid",
        "sku",
        "product_code",
    ],
    dataset_name="products",
)

# Factory master data
clean_dimension(
    source_table="quince_dev.bronze.dim_factories",
    target_table="quince_dev.silver.factories",
    quarantine_table="quince_dev.audit.factories_invalid",
    key_candidates=[
        "factory_id",
        "factoryid",
        "factory_code",
        "plant_id",
    ],
    dataset_name="factories",
)

print("\nSilver product and factory processing completed.")
