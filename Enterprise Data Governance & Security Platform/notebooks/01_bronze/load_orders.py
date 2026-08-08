# ============================================================
# File        : load_orders.py
# Project     : Enterprise Data Governance Platform
# Purpose     : Ingest order data into Bronze Delta
# ============================================================

from datetime import datetime
import uuid

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    input_file_name,
    lit,
    trim,
    upper
)

from schemas.order_schema import order_schema


# ============================================================
# Configuration
# ============================================================

SOURCE_PATH = (
    "s3://enterprise-data-platform/raw/orders/"
)

TARGET_TABLE = (
    "dev_catalog.bronze.orders"
)

QUARANTINE_TABLE = (
    "dev_catalog.bronze.orders_quarantine"
)

AUDIT_TABLE = (
    "dev_catalog.governance.ingestion_audit"
)

CUSTOMER_TABLE = (
    "dev_catalog.bronze.customers"
)

STORE_TABLE = (
    "dev_catalog.bronze.stores"
)

PRODUCT_TABLE = (
    "dev_catalog.bronze.products"
)

PIPELINE_NAME = "order_ingestion"

BATCH_ID = str(uuid.uuid4())


# ============================================================
# Spark Session
# ============================================================

spark = (
    SparkSession.builder
    .appName("OrderDataIngestion")
    .getOrCreate()
)


pipeline_start_time = datetime.now()


try:

    print(
        "Starting order ingestion pipeline"
    )

    # ========================================================
    # STEP 1 — Read Source Data
    # ========================================================

    orders_df = (
        spark.read
        .option("header", "true")
        .schema(order_schema)
        .csv(SOURCE_PATH)
    )

    source_count = orders_df.count()

    print(
        f"Source order records: {source_count}"
    )


    # ========================================================
    # STEP 2 — Add Technical Metadata
    # ========================================================

    orders_df = (
        orders_df
        .withColumn(
            "source_file",
            input_file_name()
        )
        .withColumn(
            "ingestion_timestamp",
            current_timestamp()
        )
        .withColumn(
            "batch_id",
            lit(BATCH_ID)
        )
    )


    # ========================================================
    # STEP 3 — Standardize Values
    # ========================================================

    orders_df = (
        orders_df
        .withColumn(
            "order_id",
            trim(col("order_id"))
        )
        .withColumn(
            "customer_id",
            trim(col("customer_id"))
        )
        .withColumn(
            "store_id",
            trim(col("store_id"))
        )
        .withColumn(
            "order_status",
            upper(trim(col("order_status")))
        )
        .withColumn(
            "payment_method",
            upper(trim(col("payment_method")))
        )
    )


    # ========================================================
    # STEP 4 — Required Field Validation
    # ========================================================

    invalid_required = (
        col("order_id").isNull()
        | (trim(col("order_id")) == "")
        | col("customer_id").isNull()
        | (trim(col("customer_id")) == "")
        | col("store_id").isNull()
        | (trim(col("store_id")) == "")
        | col("order_date").isNull()
        | col("total_amount").isNull()
    )


    required_invalid_df = (
        orders_df.filter(invalid_required)
    )

    valid_required_df = (
        orders_df.filter(~invalid_required)
    )


    # ========================================================
    # STEP 5 — Validate Order Amount
    # ========================================================

    invalid_amount_df = (
        valid_required_df
        .filter(
            col("total_amount") < 0
        )
        .withColumn(
            "quarantine_reason",
            lit("NEGATIVE_ORDER_AMOUNT")
        )
    )


    valid_amount_df = (
        valid_required_df
        .filter(
            col("total_amount") >= 0
        )
    )


    # ========================================================
    # STEP 6 — Validate Order Status
    # ========================================================

    valid_statuses = [
        "CREATED",
        "PROCESSING",
        "COMPLETED",
        "CANCELLED",
        "REFUNDED"
    ]


    invalid_status_df = (
        valid_amount_df
        .filter(
            ~col("order_status").isin(
                valid_statuses
            )
        )
        .withColumn(
            "quarantine_reason",
            lit("INVALID_ORDER_STATUS")
        )
    )


    valid_status_df = (
        valid_amount_df
        .filter(
            col("order_status").isin(
                valid_statuses
            )
        )
    )


    # ========================================================
    # STEP 7 — Validate Payment Method
    # ========================================================

    valid_payment_methods = [
        "CREDIT_CARD",
        "DEBIT_CARD",
        "UPI",
        "NET_BANKING",
        "WALLET",
        "CASH"
    ]


    invalid_payment_df = (
        valid_status_df
        .filter(
            ~col("payment_method").isin(
                valid_payment_methods
            )
        )
        .withColumn(
            "quarantine_reason",
            lit("INVALID_PAYMENT_METHOD")
        )
    )


    valid_payment_df = (
        valid_status_df
        .filter(
            col("payment_method").isin(
                valid_payment_methods
            )
        )
    )


    # ========================================================
    # STEP 8 — Duplicate Order Validation
    # ========================================================

    duplicate_order_ids = (
        valid_payment_df
        .groupBy("order_id")
        .count()
        .filter(
            col("count") > 1
        )
        .select("order_id")
    )


    duplicate_orders_df = (
        valid_payment_df
        .join(
            duplicate_order_ids,
            on="order_id",
            how="inner"
        )
        .withColumn(
            "quarantine_reason",
            lit("DUPLICATE_ORDER_ID")
        )
    )


    valid_orders_df = (
        valid_payment_df
        .dropDuplicates(["order_id"])
    )


    # ========================================================
    # STEP 9 — Customer Referential Integrity
    # ========================================================

    customer_reference_df = (
        spark.table(CUSTOMER_TABLE)
        .select(
            "customer_id"
        )
        .dropDuplicates()
    )


    invalid_customer_orders = (
        valid_orders_df
        .join(
            customer_reference_df,
            on="customer_id",
            how="left_anti"
        )
        .withColumn(
            "quarantine_reason",
            lit("CUSTOMER_NOT_FOUND")
        )
    )


    valid_customer_orders = (
        valid_orders_df
        .join(
            customer_reference_df,
            on="customer_id",
            how="inner"
        )
    )


    # ========================================================
    # STEP 10 — Store Referential Integrity
    # ========================================================

    store_reference_df = (
        spark.table(STORE_TABLE)
        .select(
            "store_id"
        )
        .dropDuplicates()
    )


    invalid_store_orders = (
        valid_customer_orders
        .join(
            store_reference_df,
            on="store_id",
            how="left_anti"
        )
        .withColumn(
            "quarantine_reason",
            lit("STORE_NOT_FOUND")
        )
    )


    valid_store_orders = (
        valid_customer_orders
        .join(
            store_reference_df,
            on="store_id",
            how="inner"
        )
    )


    # ========================================================
    # STEP 11 — Counts
    # ========================================================

    required_invalid_count = (
        required_invalid_df.count()
    )

    invalid_amount_count = (
        invalid_amount_df.count()
    )

    invalid_status_count = (
        invalid_status_df.count()
    )

    invalid_payment_count = (
        invalid_payment_df.count()
    )

    duplicate_count = (
        duplicate_orders_df.count()
    )

    invalid_customer_count = (
        invalid_customer_orders.count()
    )

    invalid_store_count = (
        invalid_store_orders.count()
    )


    # ========================================================
    # STEP 12 — Quarantine Records
    # ========================================================

    quarantine_dfs = [
        required_invalid_df.withColumn(
            "quarantine_reason",
            lit("REQUIRED_FIELD_MISSING")
        ),

        invalid_amount_df,

        invalid_status_df,

        invalid_payment_df,

        duplicate_orders_df,

        invalid_customer_orders,

        invalid_store_orders
    ]


    for quarantine_df in quarantine_dfs:

        if quarantine_df.limit(1).count() > 0:

            (
                quarantine_df
                .write
                .format("delta")
                .mode("append")
                .saveAsTable(
                    QUARANTINE_TABLE
                )
            )


    # ========================================================
    # STEP 13 — Write Valid Orders
    # ========================================================

    target_count = (
        valid_store_orders.count()
    )


    (
        valid_store_orders
        .write
        .format("delta")
        .mode("append")
        .saveAsTable(
            TARGET_TABLE
        )
    )


    # ========================================================
    # STEP 14 — Audit Counts
    # ========================================================

    total_invalid = (
        required_invalid_count
        + invalid_amount_count
        + invalid_status_count
        + invalid_payment_count
        + duplicate_count
        + invalid_customer_count
        + invalid_store_count
    )


    pipeline_end_time = datetime.now()


    execution_time_seconds = (
        pipeline_end_time
        - pipeline_start_time
    ).total_seconds()


    # ========================================================
    # STEP 15 — Audit Record
    # ========================================================

    audit_data = [

        (
            PIPELINE_NAME,
            BATCH_ID,
            "SUCCESS",
            source_count,
            target_count,
            total_invalid,
            duplicate_count,
            execution_time_seconds,
            pipeline_start_time,
            pipeline_end_time
        )
    ]


    audit_df = spark.createDataFrame(
        audit_data,
        [
            "pipeline_name",
            "batch_id",
            "status",
            "source_count",
            "target_count",
            "invalid_count",
            "duplicate_count",
            "execution_time_seconds",
            "start_time",
            "end_time"
        ]
    )


    (
        audit_df
        .write
        .format("delta")
        .mode("append")
        .saveAsTable(
            AUDIT_TABLE
        )
    )


    print(
        "Order ingestion completed successfully"
    )


except Exception as e:

    # ========================================================
    # FAILURE HANDLING
    # ========================================================

    pipeline_end_time = datetime.now()

    execution_time_seconds = (
        pipeline_end_time
        - pipeline_start_time
    ).total_seconds()


    print(
        f"Order ingestion failed: {str(e)}"
    )


    audit_data = [

        (
            PIPELINE_NAME,
            BATCH_ID,
            "FAILED",
            0,
            0,
            0,
            0,
            execution_time_seconds,
            pipeline_start_time,
            pipeline_end_time
        )
    ]


    audit_df = spark.createDataFrame(
        audit_data,
        [
            "pipeline_name",
            "batch_id",
            "status",
            "source_count",
            "target_count",
            "invalid_count",
            "duplicate_count",
            "execution_time_seconds",
            "start_time",
            "end_time"
        ]
    )


    (
        audit_df
        .write
        .format("delta")
        .mode("append")
        .saveAsTable(
            AUDIT_TABLE
        )
    )


    raise


finally:

    spark.stop()
