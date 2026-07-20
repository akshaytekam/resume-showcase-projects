"""
===============================================================================
Notebook Name : bronze_load_v2.py

Layer          : Bronze

Description
-----------
Enterprise Bronze Layer Ingestion Notebook

This notebook ingests raw CSV files from AWS S3 using
Databricks Auto Loader and stores them into Bronze Delta tables.

Features
--------
✓ Databricks Auto Loader
✓ Explicit Schema
✓ Schema Evolution
✓ Incremental Processing
✓ Audit Logging
✓ Metadata Columns
✓ Error Handling
✓ Delta Lake

Author
------
Enterprise Data Engineering Team

Version
-------
2.0
===============================================================================
"""

# =============================================================================
# Imports
# =============================================================================

from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType,
    DateType
)

from pyspark.sql.functions import (
    current_timestamp,
    input_file_name,
    lit,
    col
)

from common.spark_session import get_spark
from common.config import (
    get_config,
    get_pipeline_config
)

from common.audit_logger import AuditLogger

# =============================================================================
# Spark Session
# =============================================================================

spark = get_spark("Enterprise Bronze Load")

# =============================================================================
# Configuration
# =============================================================================

config = get_config()

pipeline_config = get_pipeline_config()

LANDING_PATH = config["landing_path"]

BRONZE_PATH = config["bronze_path"]

CHECKPOINT_PATH = config["checkpoint_path"]

DATABASE = config["database"]

# =============================================================================
# Notebook Parameters
# =============================================================================

PIPELINE_NAME = "Retail Batch Pipeline"

LAYER_NAME = "Bronze"

TABLE_NAME = "bronze_sales"

SOURCE_NAME = "sales"

CHECKPOINT_LOCATION = (
    CHECKPOINT_PATH +
    "/bronze_sales_checkpoint"
)

SCHEMA_LOCATION = (
    CHECKPOINT_PATH +
    "/bronze_sales_schema"
)

BAD_RECORD_PATH = (
    CHECKPOINT_PATH +
    "/bad_records"
)

# =============================================================================
# Audit Logger
# =============================================================================

audit = AuditLogger(

    spark=spark,

    config=config,

    pipeline_name=PIPELINE_NAME,

    layer=LAYER_NAME,

    notebook_name="bronze_load_v2"

)

# =============================================================================
# Explicit Schema
# =============================================================================

sales_schema = StructType([

    StructField(
        "sale_id",
        IntegerType(),
        False
    ),

    StructField(
        "store_id",
        StringType(),
        False
    ),

    StructField(
        "customer_id",
        StringType(),
        False
    ),

    StructField(
        "product_id",
        StringType(),
        False
    ),

    StructField(
        "sale_date",
        DateType(),
        False
    ),

    StructField(
        "quantity",
        IntegerType(),
        False
    ),

    StructField(
        "price",
        DoubleType(),
        False
    )

])

# =============================================================================
# Helper Functions
# =============================================================================

def get_source_path():

    """
    Returns Landing Zone path.
    """

    return f"{LANDING_PATH}/sales"


def get_bronze_path():

    """
    Returns Bronze Delta path.
    """

    return f"{BRONZE_PATH}/sales"


def get_table_name():

    """
    Returns Delta table name.
    """

    return f"{DATABASE}.{TABLE_NAME}"

# =============================================================================
# Auto Loader Reader
# =============================================================================

def read_sales_autoloader():

    """
    Reads new files from Landing Zone using
    Databricks Auto Loader.

    Auto Loader keeps track of processed files,
    therefore only NEW files are processed.
    """

    df = (

        spark.readStream

        .format("cloudFiles")

        .option(

            "cloudFiles.format",

            "csv"

        )

        .option(

            "cloudFiles.schemaLocation",

            SCHEMA_LOCATION

        )

        .option(

            "header",

            "true"

        )

        .option(

            "rescuedDataColumn",

            "_rescued_data"

        )

        .schema(

            sales_schema

        )

        .load(

            get_source_path()

        )

    )

    return df


# =============================================================================
# Metadata Enrichment
# =============================================================================

from pyspark.sql.functions import (
    sha2,
    concat_ws,
    expr,
    current_date
)


def enrich_metadata(df):
    """
    Adds enterprise metadata columns required for auditing,
    lineage and troubleshooting.
    """

    enriched_df = (

        df

        .withColumn(
            "ingestion_timestamp",
            current_timestamp()
        )

        .withColumn(
            "ingestion_date",
            current_date()
        )

        .withColumn(
            "source_file_name",
            input_file_name()
        )

        .withColumn(
            "pipeline_name",
            lit(PIPELINE_NAME)
        )

        .withColumn(
            "layer_name",
            lit(LAYER_NAME)
        )

        .withColumn(
            "record_hash",
            sha2(
                concat_ws(
                    "||",
                    col("sale_id"),
                    col("store_id"),
                    col("customer_id"),
                    col("product_id"),
                    col("sale_date"),
                    col("quantity"),
                    col("price")
                ),
                256
            )
        )

    )

    return enriched_df


# =============================================================================
# Data Quality Validation
# =============================================================================

def split_good_bad_records(df):
    """
    Splits incoming records into:

        Good Records
        Bad Records

    Bad records contain malformed data captured by
    Auto Loader in the _rescued_data column.
    """

    good_df = (

        df

        .filter(
            col("_rescued_data").isNull()
        )

    )

    bad_df = (

        df

        .filter(
            col("_rescued_data").isNotNull()
        )

    )

    return good_df, bad_df


# =============================================================================
# Write Bad Records
# =============================================================================

def write_bad_records(df):
    """
    Stores rejected records into a quarantine
    Delta table for investigation.
    """

    (

        df.write

        .format("delta")

        .mode("append")

        .save(

            BAD_RECORD_PATH

        )

    )


# =============================================================================
# Batch Processing
# =============================================================================

def process_batch(batch_df, batch_id):
    """
    Executes for every micro-batch produced by Auto Loader.
    """

    print("=" * 80)
    print(f"Processing Batch : {batch_id}")
    print("=" * 80)

    # ------------------------------------------------------------
    # Metadata
    # ------------------------------------------------------------

    enriched_df = enrich_metadata(batch_df)

    # ------------------------------------------------------------
    # Split Good / Bad
    # ------------------------------------------------------------

    good_df, bad_df = split_good_bad_records(
        enriched_df
    )

    # ------------------------------------------------------------
    # Persist DataFrames
    # ------------------------------------------------------------

    good_df.cache()

    bad_df.cache()

    records_read = batch_df.count()

    good_records = good_df.count()

    bad_records = bad_df.count()

    print(f"Records Read     : {records_read}")
    print(f"Good Records     : {good_records}")
    print(f"Rejected Records : {bad_records}")

    # ------------------------------------------------------------
    # Write Bad Records
    # ------------------------------------------------------------

    if bad_records > 0:

        write_bad_records(
            bad_df
        )

    # ------------------------------------------------------------
    # Return Good Records
    # ------------------------------------------------------------

    return good_df


# =============================================================================
# Write Bronze Delta Table
# =============================================================================

from delta.tables import DeltaTable


def write_bronze_table(df):
    """
    Writes good records into the Bronze Delta table.
    """

    bronze_path = get_bronze_path()

    (
        df.write
        .format("delta")
        .mode("append")
        .save(bronze_path)
    )


# =============================================================================
# Register Delta Table
# =============================================================================

def register_delta_table():
    """
    Registers the Bronze Delta table in the metastore
    if it does not already exist.
    """

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {get_table_name()}
        USING DELTA
        LOCATION '{get_bronze_path()}'
    """)


# =============================================================================
# Optimize Delta Table (Optional)
# =============================================================================

def optimize_table():
    """
    Optimizes the Delta table.
    Normally scheduled after ingestion,
    not necessarily after every batch.
    """

    try:

        spark.sql(f"""
            OPTIMIZE {get_table_name()}
            ZORDER BY (sale_id, sale_date)
        """)

        print("OPTIMIZE completed.")

    except Exception as ex:

        print(f"OPTIMIZE skipped: {ex}")


# =============================================================================
# Streaming Batch Processing
# =============================================================================

def process_batch(batch_df, batch_id):
    """
    Executes once for every Auto Loader micro-batch.
    """

    print("=" * 80)
    print(f"Processing Batch : {batch_id}")
    print("=" * 80)

    try:

        enriched_df = enrich_metadata(batch_df)

        good_df, bad_df = split_good_bad_records(
            enriched_df
        )

        good_df.cache()
        bad_df.cache()

        records_read = batch_df.count()
        records_written = good_df.count()
        rejected_records = bad_df.count()

        print(f"Records Read      : {records_read}")
        print(f"Records Written   : {records_written}")
        print(f"Rejected Records  : {rejected_records}")

        # ----------------------------------------------------------
        # Write Bad Records
        # ----------------------------------------------------------

        if rejected_records > 0:

            write_bad_records(
                bad_df
            )

        # ----------------------------------------------------------
        # Write Bronze Delta
        # ----------------------------------------------------------

        write_bronze_table(
            good_df
        )

        # ----------------------------------------------------------
        # Register Table
        # ----------------------------------------------------------

        register_delta_table()

        # ----------------------------------------------------------
        # Audit
        # ----------------------------------------------------------

        audit.success(

            records_read=records_read,

            records_written=records_written

        )

    except Exception as ex:

        audit.failure(ex)

        raise

    finally:

        good_df.unpersist()

        bad_df.unpersist()


# =============================================================================
# Start Streaming Query
# =============================================================================

def start_stream():

    source_df = read_sales_autoloader()

    query = (

        source_df.writeStream

        .foreachBatch(process_batch)

        .option(

            "checkpointLocation",

            CHECKPOINT_LOCATION

        )

        .trigger(availableNow=True)

        .start()

    )

    query.awaitTermination()


# =============================================================================
# Main Entry Point
# =============================================================================

def bronze_pipeline():

    print("=" * 80)
    print("Starting Enterprise Bronze Pipeline")
    print("=" * 80)

    audit.start()

    try:

        start_stream()

        optimize_table()

        print("=" * 80)
        print("Bronze Pipeline Completed Successfully")
        print("=" * 80)

    except Exception as ex:

        print("=" * 80)
        print("Bronze Pipeline Failed")
        print("=" * 80)

        raise ex


# =============================================================================
# Execute Notebook
# =============================================================================

if __name__ == "__main__":

    bronze_pipeline()
