# ============================================================
# File        : load_products.py
# Project     : Enterprise Data Governance Platform
# Purpose     : Ingest product data into Bronze Delta
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

from schemas.product_schema import product_schema


# ============================================================
# Configuration
# ============================================================

SOURCE_PATH = (
    "s3://enterprise-data-platform/raw/products/"
)

TARGET_TABLE = (
    "dev_catalog.bronze.products"
)

QUARANTINE_TABLE = (
    "dev_catalog.bronze.products_quarantine"
)

AUDIT_TABLE = (
    "dev_catalog.governance.ingestion_audit"
)

PIPELINE_NAME = "product_ingestion"

BATCH_ID = str(uuid.uuid4())


# ============================================================
# Spark Session
# ============================================================

spark = (
    SparkSession.builder
    .appName("ProductDataIngestion")
    .getOrCreate()
)


pipeline_start_time = datetime.now()


try:

    print(
        "Starting product ingestion pipeline"
    )

    # ========================================================
    # STEP 1 — Read Product Files
    # ========================================================

    products_df = (
        spark.read
        .option("header", "true")
        .schema(product_schema)
        .csv(SOURCE_PATH)
    )

    source_count = products_df.count()

    print(
        f"Source product records: {source_count}"
    )


    # ========================================================
    # STEP 2 — Add Technical Metadata
    # ========================================================

    products_df = (
        products_df
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
    # STEP 3 — Standardize String Values
    # ========================================================

    products_df = (
        products_df
        .withColumn(
            "product_id",
            trim(col("product_id"))
        )
        .withColumn(
            "product_name",
            trim(col("product_name"))
        )
        .withColumn(
            "category",
            upper(trim(col("category")))
        )
        .withColumn(
            "subcategory",
            upper(trim(col("subcategory")))
        )
        .withColumn(
            "brand",
            trim(col("brand"))
        )
        .withColumn(
            "supplier_id",
            trim(col("supplier_id"))
        )
        .withColumn(
            "product_status",
            upper(trim(col("product_status")))
        )
    )


    # ========================================================
    # STEP 4 — Required Field Validation
    # ========================================================

    invalid_required_condition = (
        col("product_id").isNull()
        | (trim(col("product_id")) == "")
        | col("product_name").isNull()
        | (trim(col("product_name")) == "")
        | col("category").isNull()
        | (trim(col("category")) == "")
        | col("unit_cost").isNull()
        | col("selling_price").isNull()
        | col("product_status").isNull()
    )


    invalid_required_df = (
        products_df
        .filter(invalid_required_condition)
        .withColumn(
            "quarantine_reason",
            lit("REQUIRED_FIELD_MISSING")
        )
    )


    valid_required_df = (
        products_df
        .filter(~invalid_required_condition)
    )


    # ========================================================
    # STEP 5 — Validate Product Prices
    # ========================================================

    invalid_price_df = (
        valid_required_df
        .filter(
            (col("unit_cost") < 0)
            |
            (col("selling_price") < 0)
        )
        .withColumn(
            "quarantine_reason",
            lit("NEGATIVE_PRICE")
        )
    )


    valid_price_df = (
        valid_required_df
        .filter(
            (col("unit_cost") >= 0)
            &
            (col("selling_price") >= 0)
        )
    )


    # ========================================================
    # STEP 6 — Validate Tax Rate
    # ========================================================

    invalid_tax_df = (
        valid_price_df
        .filter(
            (col("tax_rate") < 0)
            |
            (col("tax_rate") > 100)
        )
        .withColumn(
            "quarantine_reason",
            lit("INVALID_TAX_RATE")
        )
    )


    valid_tax_df = (
        valid_price_df
        .filter(
            (col("tax_rate").isNull())
            |
            (
                (col("tax_rate") >= 0)
                &
                (col("tax_rate") <= 100)
            )
        )
    )


    # ========================================================
    # STEP 7 — Validate Product Status
    # ========================================================

    valid_statuses = [
        "ACTIVE",
        "INACTIVE",
        "DISCONTINUED",
        "COMING_SOON"
    ]


    invalid_status_df = (
        valid_tax_df
        .filter(
            ~col("product_status").isin(
                valid_statuses
            )
        )
        .withColumn(
            "quarantine_reason",
            lit("INVALID_PRODUCT_STATUS")
        )
    )


    valid_status_df = (
        valid_tax_df
        .filter(
            col("product_status").isin(
                valid_statuses
            )
        )
    )


    # ========================================================
    # STEP 8 — Duplicate Product Validation
    # ========================================================

    duplicate_product_ids = (
        valid_status_df
        .groupBy("product_id")
        .count()
        .filter(
            col("count") > 1
        )
        .select("product_id")
    )


    duplicate_products_df = (
        valid_status_df
        .join(
            duplicate_product_ids,
            on="product_id",
            how="inner"
        )
        .withColumn(
            "quarantine_reason",
            lit("DUPLICATE_PRODUCT_ID")
        )
    )


    valid_products_df = (
        valid_status_df
        .dropDuplicates(
            ["product_id"]
        )
    )


    # ========================================================
    # STEP 9 — Product Margin Validation
    # ========================================================

    # Negative gross margin is not always a data-quality error.
    # However, it should be flagged for business review.

    negative_margin_df = (
        valid_products_df
        .filter(
            col("selling_price")
            < col("unit_cost")
        )
        .withColumn(
            "quarantine_reason",
            lit("SELLING_PRICE_BELOW_COST")
        )
    )


    # For this project, we treat negative-margin products
    # as business validation failures.

    valid_margin_df = (
        valid_products_df
        .filter(
            col("selling_price")
            >= col("unit_cost")
        )
    )


    # ========================================================
    # STEP 10 — Calculate Counts
    # ========================================================

    required_invalid_count = (
        invalid_required_df.count()
    )

    invalid_price_count = (
        invalid_price_df.count()
    )

    invalid_tax_count = (
        invalid_tax_df.count()
    )

    invalid_status_count = (
        invalid_status_df.count()
    )

    duplicate_count = (
        duplicate_products_df.count()
    )

    negative_margin_count = (
        negative_margin_df.count()
    )


    total_invalid_count = (
        required_invalid_count
        + invalid_price_count
        + invalid_tax_count
        + invalid_status_count
        + duplicate_count
        + negative_margin_count
    )


    # ========================================================
    # STEP 11 — Quarantine Invalid Records
    # ========================================================

    quarantine_dfs = [

        invalid_required_df,

        invalid_price_df,

        invalid_tax_df,

        invalid_status_df,

        duplicate_products_df,

        negative_margin_df
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
    # STEP 12 — Write Valid Products
    # ========================================================

    target_count = (
        valid_margin_df.count()
    )


    (
        valid_margin_df
        .write
        .format("delta")
        .mode("append")
        .saveAsTable(
            TARGET_TABLE
        )
    )


    # ========================================================
    # STEP 13 — Calculate Execution Time
    # ========================================================

    pipeline_end_time = datetime.now()

    execution_time_seconds = (
        pipeline_end_time
        - pipeline_start_time
    ).total_seconds()


    # ========================================================
    # STEP 14 — Write Audit Record
    # ========================================================

    audit_data = [

        (
            PIPELINE_NAME,
            BATCH_ID,
            "SUCCESS",
            source_count,
            target_count,
            total_invalid_count,
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
        "Product ingestion completed successfully"
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
        f"Product ingestion failed: {str(e)}"
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
