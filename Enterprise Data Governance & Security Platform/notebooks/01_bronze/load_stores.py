# ============================================================
# File        : load_stores.py
# Project     : Enterprise Data Governance Platform
# Purpose     : Ingest store master data into Bronze Delta
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

from schemas.store_schema import store_schema


# ============================================================
# Configuration
# ============================================================

SOURCE_PATH = (
    "s3://enterprise-data-platform/raw/stores/"
)

TARGET_TABLE = (
    "dev_catalog.bronze.stores"
)

QUARANTINE_TABLE = (
    "dev_catalog.bronze.stores_quarantine"
)

AUDIT_TABLE = (
    "dev_catalog.governance.ingestion_audit"
)

EMPLOYEE_TABLE = (
    "dev_catalog.bronze.employees"
)

PIPELINE_NAME = "store_ingestion"

BATCH_ID = str(uuid.uuid4())


# ============================================================
# Spark Session
# ============================================================

spark = (
    SparkSession.builder
    .appName("StoreDataIngestion")
    .getOrCreate()
)


pipeline_start_time = datetime.now()


try:

    print(
        "Starting store ingestion pipeline"
    )

    # ========================================================
    # STEP 1 — Read Store Source
    # ========================================================

    stores_df = (
        spark.read
        .option("header", "true")
        .schema(store_schema)
        .csv(SOURCE_PATH)
    )

    source_count = stores_df.count()

    print(
        f"Source store records: {source_count}"
    )


    # ========================================================
    # STEP 2 — Add Technical Metadata
    # ========================================================

    stores_df = (
        stores_df
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

    stores_df = (
        stores_df

        .withColumn(
            "store_id",
            trim(col("store_id"))
        )

        .withColumn(
            "store_name",
            trim(col("store_name"))
        )

        .withColumn(
            "store_type",
            upper(trim(col("store_type")))
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
            "postal_code",
            trim(col("postal_code"))
        )

        .withColumn(
            "region",
            upper(trim(col("region")))
        )

        .withColumn(
            "manager_id",
            trim(col("manager_id"))
        )

        .withColumn(
            "store_status",
            upper(trim(col("store_status")))
        )
    )


    # ========================================================
    # STEP 4 — Required Field Validation
    # ========================================================

    invalid_required_condition = (
        col("store_id").isNull()
        | (trim(col("store_id")) == "")

        | col("store_name").isNull()
        | (trim(col("store_name")) == "")

        | col("store_type").isNull()

        | col("city").isNull()
        | (trim(col("city")) == "")

        | col("state").isNull()
        | (trim(col("state")) == "")

        | col("region").isNull()
        | (trim(col("region")) == "")

        | col("store_status").isNull()
    )


    invalid_required_df = (
        stores_df
        .filter(invalid_required_condition)
        .withColumn(
            "quarantine_reason",
            lit("REQUIRED_FIELD_MISSING")
        )
    )


    valid_required_df = (
        stores_df
        .filter(~invalid_required_condition)
    )


    # ========================================================
    # STEP 5 — Validate Store Type
    # ========================================================

    valid_store_types = [
        "RETAIL",
        "OUTLET",
        "EXPRESS",
        "WAREHOUSE",
        "FRANCHISE"
    ]


    invalid_store_type_df = (
        valid_required_df
        .filter(
            ~col("store_type").isin(
                valid_store_types
            )
        )
        .withColumn(
            "quarantine_reason",
            lit("INVALID_STORE_TYPE")
        )
    )


    valid_store_type_df = (
        valid_required_df
        .filter(
            col("store_type").isin(
                valid_store_types
            )
        )
    )


    # ========================================================
    # STEP 6 — Validate Region
    # ========================================================

    valid_regions = [
        "NORTH",
        "SOUTH",
        "EAST",
        "WEST",
        "CENTRAL"
    ]


    invalid_region_df = (
        valid_store_type_df
        .filter(
            ~col("region").isin(
                valid_regions
            )
        )
        .withColumn(
            "quarantine_reason",
            lit("INVALID_REGION")
        )
    )


    valid_region_df = (
        valid_store_type_df
        .filter(
            col("region").isin(
                valid_regions
            )
        )
    )


    # ========================================================
    # STEP 7 — Validate Store Status
    # ========================================================

    valid_store_statuses = [
        "ACTIVE",
        "INACTIVE",
        "TEMPORARILY_CLOSED",
        "UNDER_RENOVATION"
    ]


    invalid_status_df = (
        valid_region_df
        .filter(
            ~col("store_status").isin(
                valid_store_statuses
            )
        )
        .withColumn(
            "quarantine_reason",
            lit("INVALID_STORE_STATUS")
        )
    )


    valid_status_df = (
        valid_region_df
        .filter(
            col("store_status").isin(
                valid_store_statuses
            )
        )
    )


    # ========================================================
    # STEP 8 — Validate Postal Code
    # ========================================================

    invalid_postal_df = (
        valid_status_df
        .filter(
            col("postal_code").isNotNull()
            &
            (
                ~col("postal_code").rlike(
                    "^[0-9]{6}$"
                )
            )
        )
        .withColumn(
            "quarantine_reason",
            lit("INVALID_POSTAL_CODE")
        )
    )


    valid_postal_df = (
        valid_status_df
        .filter(
            col("postal_code").isNull()
            |
            col("postal_code").rlike(
                "^[0-9]{6}$"
            )
        )
    )


    # ========================================================
    # STEP 9 — Duplicate Store Validation
    # ========================================================

    duplicate_store_ids = (
        valid_postal_df
        .groupBy("store_id")
        .count()
        .filter(
            col("count") > 1
        )
        .select("store_id")
    )


    duplicate_stores_df = (
        valid_postal_df
        .join(
            duplicate_store_ids,
            on="store_id",
            how="inner"
        )
        .withColumn(
            "quarantine_reason",
            lit("DUPLICATE_STORE_ID")
        )
    )


    valid_stores_df = (
        valid_postal_df
        .dropDuplicates(
            ["store_id"]
        )
    )


    # ========================================================
    # STEP 10 — Employee Referential Integrity
    # ========================================================

    employee_reference_df = (
        spark.table(EMPLOYEE_TABLE)
        .select(
            "employee_id"
        )
        .dropDuplicates()
    )


    # Stores without a manager are allowed.
    # Stores with manager_id must reference an employee.

    invalid_manager_df = (
        valid_stores_df
        .filter(
            col("manager_id").isNotNull()
        )
        .join(
            employee_reference_df,
            on=(
                valid_stores_df.manager_id
                ==
                employee_reference_df.employee_id
            ),
            how="left_anti"
        )
        .withColumn(
            "quarantine_reason",
            lit("MANAGER_NOT_FOUND")
        )
    )


    valid_manager_df = (
        valid_stores_df
        .filter(
            col("manager_id").isNull()
        )
        .unionByName(
            valid_stores_df
            .filter(
                col("manager_id").isNotNull()
            )
            .join(
                employee_reference_df,
                on=(
                    valid_stores_df.manager_id
                    ==
                    employee_reference_df.employee_id
                ),
                how="inner"
            )
            .select(
                valid_stores_df["*"]
            )
        )
    )


    # ========================================================
    # STEP 11 — Opening Date Validation
    # ========================================================

    invalid_opening_date_df = (
        valid_manager_df
        .filter(
            col("opening_date")
            > current_timestamp()
        )
        .withColumn(
            "quarantine_reason",
            lit("FUTURE_OPENING_DATE")
        )
    )


    valid_opening_date_df = (
        valid_manager_df
        .filter(
            col("opening_date").isNull()
            |
            (
                col("opening_date")
                <= current_timestamp()
            )
        )
    )


    # ========================================================
    # STEP 12 — Calculate Validation Counts
    # ========================================================

    required_invalid_count = (
        invalid_required_df.count()
    )

    invalid_store_type_count = (
        invalid_store_type_df.count()
    )

    invalid_region_count = (
        invalid_region_df.count()
    )

    invalid_status_count = (
        invalid_status_df.count()
    )

    invalid_postal_count = (
        invalid_postal_df.count()
    )

    duplicate_count = (
        duplicate_stores_df.count()
    )

    invalid_manager_count = (
        invalid_manager_df.count()
    )

    invalid_opening_date_count = (
        invalid_opening_date_df.count()
    )


    total_invalid_count = (
        required_invalid_count
        + invalid_store_type_count
        + invalid_region_count
        + invalid_status_count
        + invalid_postal_count
        + duplicate_count
        + invalid_manager_count
        + invalid_opening_date_count
    )


    # ========================================================
    # STEP 13 — Quarantine Invalid Data
    # ========================================================

    quarantine_dfs = [

        invalid_required_df,

        invalid_store_type_df,

        invalid_region_df,

        invalid_status_df,

        invalid_postal_df,

        duplicate_stores_df,

        invalid_manager_df,

        invalid_opening_date_df
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
    # STEP 14 — Write Valid Stores
    # ========================================================

    target_count = (
        valid_opening_date_df.count()
    )


    (
        valid_opening_date_df
        .write
        .format("delta")
        .mode("append")
        .saveAsTable(
            TARGET_TABLE
        )
    )


    # ========================================================
    # STEP 15 — Write Audit Record
    # ========================================================

    pipeline_end_time = datetime.now()

    execution_time_seconds = (
        pipeline_end_time
        - pipeline_start_time
    ).total_seconds()


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
        "Store ingestion completed successfully"
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
        f"Store ingestion failed: {str(e)}"
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
