# ============================================================
# File        : load_customers.py
# Project     : Enterprise Data Governance Platform
# Purpose     : Ingest customer data from S3 into Bronze Delta
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

from schemas.customer_schema import customer_schema


# ============================================================
# Configuration
# ============================================================

SOURCE_PATH = "s3://enterprise-data-platform/raw/customers/"

TARGET_TABLE = "dev_catalog.bronze.customers"

QUARANTINE_TABLE = "dev_catalog.bronze.customers_quarantine"

AUDIT_TABLE = "dev_catalog.governance.ingestion_audit"


PIPELINE_NAME = "customer_ingestion"

BATCH_ID = str(uuid.uuid4())


# ============================================================
# Spark Session
# ============================================================

spark = SparkSession.builder \
    .appName("CustomerDataIngestion") \
    .getOrCreate()


# ============================================================
# Start Time
# ============================================================

pipeline_start_time = datetime.now()


try:

    # ========================================================
    # STEP 1 — Read Source File
    # ========================================================

    print("Starting customer ingestion pipeline")

    df = (
        spark.read
        .option("header", "true")
        .schema(customer_schema)
        .csv(SOURCE_PATH)
    )

    source_count = df.count()

    print(f"Source records received: {source_count}")


    # ========================================================
    # STEP 2 — Add Technical Metadata
    # ========================================================

    df = (
        df
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
    # STEP 3 — Basic Data Standardization
    # ========================================================

    df = (
        df
        .withColumn(
            "first_name",
            trim(col("first_name"))
        )
        .withColumn(
            "last_name",
            trim(col("last_name"))
        )
        .withColumn(
            "email",
            trim(col("email"))
        )
        .withColumn(
            "city",
            trim(col("city"))
        )
        .withColumn(
            "state",
            upper(trim(col("state")))
        )
        .withColumn(
            "customer_type",
            upper(trim(col("customer_type")))
        )
        .withColumn(
            "status",
            upper(trim(col("status")))
        )
    )


    # ========================================================
    # STEP 4 — Required Field Validation
    # ========================================================

    invalid_required = (
        col("customer_id").isNull()
        | (trim(col("customer_id")) == "")
        | col("first_name").isNull()
        | (trim(col("first_name")) == "")
        | col("last_name").isNull()
        | (trim(col("last_name")) == "")
        | col("registration_date").isNull()
    )


    invalid_df = df.filter(invalid_required)

    valid_df = df.filter(~invalid_required)


    invalid_count = invalid_df.count()
    valid_count = valid_df.count()


    print(f"Valid records: {valid_count}")
    print(f"Invalid records: {invalid_count}")


    # ========================================================
    # STEP 5 — Duplicate Customer Validation
    # ========================================================

    duplicate_customer_ids = (
        valid_df
        .groupBy("customer_id")
        .count()
        .filter(col("count") > 1)
        .select("customer_id")
    )


    duplicate_df = (
        valid_df
        .join(
            duplicate_customer_ids,
            on="customer_id",
            how="inner"
        )
    )


    duplicate_count = duplicate_df.count()


    print(f"Duplicate customer records: {duplicate_count}")


    # Remove duplicates from valid data

    valid_df = (
        valid_df
        .dropDuplicates(["customer_id"])
    )


    # ========================================================
    # STEP 6 — Write Invalid Records to Quarantine
    # ========================================================

    if invalid_count > 0:

        (
            invalid_df
            .write
            .format("delta")
            .mode("append")
            .saveAsTable(QUARANTINE_TABLE)
        )

        print(
            f"Written {invalid_count} records "
            f"to quarantine table"
        )


    # ========================================================
    # STEP 7 — Write Duplicate Records to Quarantine
    # ========================================================

    if duplicate_count > 0:

        (
            duplicate_df
            .withColumn(
                "quarantine_reason",
                lit("DUPLICATE_CUSTOMER_ID")
            )
            .write
            .format("delta")
            .mode("append")
            .saveAsTable(QUARANTINE_TABLE)
        )

        print(
            f"Written {duplicate_count} duplicate "
            f"records to quarantine"
        )


    # ========================================================
    # STEP 8 — Write Valid Data to Bronze Delta
    # ========================================================

    (
        valid_df
        .write
        .format("delta")
        .mode("append")
        .saveAsTable(TARGET_TABLE)
    )


    # ========================================================
    # STEP 9 — Calculate Final Counts
    # ========================================================

    final_count = valid_df.count()

    pipeline_end_time = datetime.now()

    execution_time_seconds = (
        pipeline_end_time - pipeline_start_time
    ).total_seconds()


    # ========================================================
    # STEP 10 — Write Successful Audit Record
    # ========================================================

    audit_data = [

        (
            PIPELINE_NAME,
            BATCH_ID,
            "SUCCESS",
            source_count,
            final_count,
            invalid_count,
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
        .saveAsTable(AUDIT_TABLE)
    )


    print("Customer ingestion completed successfully")


except Exception as e:

    # ========================================================
    # FAILURE HANDLING
    # ========================================================

    pipeline_end_time = datetime.now()

    execution_time_seconds = (
        pipeline_end_time - pipeline_start_time
    ).total_seconds()


    print(
        f"Customer ingestion failed: {str(e)}"
    )


    # --------------------------------------------------------
    # Write Failure Audit Record
    # --------------------------------------------------------

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
        .saveAsTable(AUDIT_TABLE)
    )


    raise


finally:

    spark.stop()
