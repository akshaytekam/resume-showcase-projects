from datetime import datetime, timezone
from uuid import uuid4

from delta.tables import DeltaTable
from pyspark.sql import functions as F

storage_account = "<storage-account-name>"
catalog_name = "quince_dev"

audit_table = f"{catalog_name}.audit.ingestion_audit"
manifest_table = f"{catalog_name}.audit.ingestion_manifest"

landing_base = (
    f"abfss://landing@{storage_account}.dfs.core.windows.net"
)

# Example paths: update extract_date to match your actual files.
datasets = {
    "orders": {
        "path": (
            f"{landing_base}/orders/"
            "extract_date=2026-10-19/orders.csv"
        ),
        "batch_id": "orders_2026-10-19",
    },
    "order_lines": {
        "path": (
            f"{landing_base}/order_lines/"
            "extract_date=2026-10-19/order_lines.csv"
        ),
        "batch_id": "order_lines_2026-10-19",
    },
    "dim_products": {
        "path": (
            f"{landing_base}/dim_products/"
            "extract_date=2026-10-19/dim_products.csv"
        ),
        "batch_id": "dim_products_2026-10-19",
    },
    "dim_factories": {
        "path": (
            f"{landing_base}/dim_factories/"
            "extract_date=2026-10-19/dim_factories.csv"
        ),
        "batch_id": "dim_factories_2026-10-19",
    },
    "inventory_snapshots": {
        "path": (
            f"{landing_base}/inventory_snapshots/"
            "extract_date=2026-10-19/inventory_snapshots.csv"
        ),
        "batch_id": "inventory_snapshots_2026-10-19",
    },
    "shipments": {
        "path": (
            f"{landing_base}/shipments/"
            "extract_date=2026-10-19/shipments.csv"
        ),
        "batch_id": "shipments_2026-10-19",
    },
    "payment_transactions": {
        "path": (
            f"{landing_base}/payment_transactions/"
            "extract_date=2026-10-19/payment_transactions.csv"
        ),
        "batch_id": "payment_transactions_2026-10-19",
    },
    "gateway_settlements": {
        "path": (
            f"{landing_base}/gateway_settlements/"
            "extract_date=2026-10-19/gateway_settlements.csv"
        ),
        "batch_id": "gateway_settlements_2026-10-19",
    },
    "bank_statement_transactions": {
        "path": (
            f"{landing_base}/bank_statement_transactions/"
            "extract_date=2026-10-19/bank_statement_transactions.csv"
        ),
        "batch_id": "bank_statement_transactions_2026-10-19",
    },
    "returns": {
        "path": (
            f"{landing_base}/returns/"
            "extract_date=2026-10-19/returns.csv"
        ),
        "batch_id": "returns_2026-10-19",
    },
    "general_ledger_entries": {
        "path": (
            f"{landing_base}/general_ledger_entries/"
            "extract_date=2026-10-19/general_ledger_entries.csv"
        ),
        "batch_id": "general_ledger_entries_2026-10-19",
    },
    "finance_budget": {
        "path": (
            f"{landing_base}/finance_budget/"
            "extract_date=2026-10-19/finance_budget.csv"
        ),
        "batch_id": "finance_budget_2026-10-19",
    },
}


def utc_now():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def update_audit(run_id, status, rows_loaded=0, error_message=None):
    updates = spark.createDataFrame(
        [(run_id, status, int(rows_loaded), error_message, utc_now())],
        """
        run_id STRING,
        status STRING,
        rows_loaded BIGINT,
        error_message STRING,
        ended_at_utc TIMESTAMP
        """
    )
    updates.createOrReplaceTempView("audit_update")

    spark.sql(f"""
        MERGE INTO {audit_table} AS target
        USING audit_update AS source
        ON target.run_id = source.run_id
        WHEN MATCHED THEN UPDATE SET
            target.status = source.status,
            target.rows_loaded = source.rows_loaded,
            target.error_message = source.error_message,
            target.ended_at_utc = source.ended_at_utc
    """)


def update_manifest(dataset_name, batch_id, source_file,
                    status, run_id, rows_loaded):
    updates = spark.createDataFrame(
        [(
            dataset_name, batch_id, source_file, status,
            run_id, int(rows_loaded), utc_now()
        )],
        """
        dataset_name STRING,
        batch_id STRING,
        source_file STRING,
        status STRING,
        run_id STRING,
        rows_loaded BIGINT,
        completed_at_utc TIMESTAMP
        """
    )
    updates.createOrReplaceTempView("manifest_update")

    spark.sql(f"""
        MERGE INTO {manifest_table} AS target
        USING manifest_update AS source
        ON target.dataset_name = source.dataset_name
           AND target.batch_id = source.batch_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


for dataset_name, config in datasets.items():

    source_file = config["path"]
    batch_id = config["batch_id"]
    run_id = str(uuid4())
    started_at = utc_now()

    # A completed batch should not be loaded again on a normal retry.
    already_completed = (
        spark.table(manifest_table)
        .filter(
            (F.col("dataset_name") == dataset_name)
            & (F.col("batch_id") == batch_id)
            & (F.col("status") == "SUCCESS")
        )
        .limit(1)
        .count() > 0
    )

    spark.createDataFrame(
        [(
            run_id, dataset_name, source_file, batch_id,
            "STARTED", 0, started_at, None, None
        )],
        """
        run_id STRING,
        dataset_name STRING,
        source_file STRING,
        batch_id STRING,
        status STRING,
        rows_loaded BIGINT,
        started_at_utc TIMESTAMP,
        ended_at_utc TIMESTAMP,
        error_message STRING
        """
    ).write.format("delta").mode("append").saveAsTable(audit_table)

    if already_completed:
        update_audit(run_id, "SKIPPED")
        print(f"Skipping completed batch: {dataset_name}/{batch_id}")
        continue

    try:
        # Preserve source values as strings in Bronze.
        df = (
            spark.read
            .option("header", True)
            .option("inferSchema", False)
            .csv(source_file)
        )

        # Normalize simple column names.
        for old_name in df.columns:
            new_name = old_name.strip().lower().replace(" ", "_")
            if old_name != new_name:
                df = df.withColumnRenamed(old_name, new_name)

        df = (
            df
            .withColumn("_source_file", F.lit(source_file))
            .withColumn("_ingestion_run_id", F.lit(run_id))
            .withColumn("_ingested_at_utc", F.lit(started_at))
            .withColumn("_batch_id", F.lit(batch_id))
        )

        target_table = f"{catalog_name}.bronze.{dataset_name}"
        rows_loaded = df.count()

        if spark.catalog.tableExists(target_table):
            # Atomically replace only this batch's slice in the Delta table.
            # Other batches remain unchanged.
            (
                df.write
                .format("delta")
                .mode("overwrite")
                .option(
                    "replaceWhere",
                    f"_batch_id = '{batch_id}'"
                )
                .saveAsTable(target_table)
            )
        else:
            # First load creates the table.
            df.write.format("delta").mode("overwrite").saveAsTable(
                target_table
            )

        update_manifest(
            dataset_name, batch_id, source_file,
            "SUCCESS", run_id, rows_loaded
        )
        update_audit(run_id, "SUCCESS", rows_loaded)

        print(
            f"SUCCESS: {target_table}; "
            f"rows={rows_loaded}; batch={batch_id}"
        )

    except Exception as exc:
        error_message = str(exc)[:4000]

        update_audit(
            run_id, "FAILED",
            rows_loaded=0,
            error_message=error_message
        )

        update_manifest(
            dataset_name, batch_id, source_file,
            "FAILED", run_id, 0
        )

        raise

print("Bronze ingestion processing finished.")
