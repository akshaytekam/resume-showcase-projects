
# Databricks notebook source
# 02_bronze_ingestion.py

from datetime import datetime, timezone
from uuid import uuid4
from pyspark.sql import functions as F

storage_account = "<storage-account-name>"
catalog_name = "quince_dev"
schema_name = "bronze"

landing_base = (
    f"abfss://landing@{storage_account}.dfs.core.windows.net"
)

datasets = {
    "orders": (
        f"{landing_base}/orders/"
        "extract_date=2026-10-19/orders.csv"
    ),
    "order_lines": (
        f"{landing_base}/order_lines/"
        "extract_date=2026-10-19/order_lines.csv"
    ),
}

# Shared metadata for this notebook execution.
run_id = str(uuid4())
ingested_at = datetime.now(timezone.utc).replace(tzinfo=None)

for dataset_name, file_path in datasets.items():

    print(f"Ingesting: {dataset_name}")

    # Preserve source values as strings in Bronze.
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .csv(file_path)
    )

    # Normalize column names for reliable downstream SQL.
    # Review source headers first if preserving exact names is required.
    for old_name in df.columns:
        new_name = old_name.strip().lower().replace(" ", "_")
        if old_name != new_name:
            df = df.withColumnRenamed(old_name, new_name)

    # Add ingestion lineage.
    df = (
        df
        .withColumn("_source_file", F.lit(file_path))
        .withColumn("_ingestion_run_id", F.lit(run_id))
        .withColumn("_ingested_at_utc", F.lit(ingested_at))
        .withColumn("_batch_id", F.lit(
            "demo_2026-10-19_" + dataset_name
        ))
    )

    target_table = f"{catalog_name}.{schema_name}.{dataset_name}"

    # FIRST-RUN DEMO ONLY:
    # overwrite replaces the target table's existing contents.
    # Do not use this mode for routine production ingestion.
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )

    print(f"Created/loaded table: {target_table}")
    print(f"Rows loaded: {spark.table(target_table).count()}")

print(f"Completed ingestion run: {run_id}")
