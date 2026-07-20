"""
===============================================================================
Notebook Name : silver_transform.py

Layer          : Silver

Description
-----------
Reads Bronze Delta tables and applies cleansing,
standardization and business transformations before
loading into Silver Delta tables.

Features
--------
✓ Incremental Processing
✓ Delta Lake
✓ Audit Logging
✓ Data Cleansing
✓ Standardization
✓ Deduplication
✓ Data Validation

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

from pyspark.sql.functions import (
    col,
    trim,
    upper,
    lower,
    current_timestamp,
    current_date,
    lit
)

from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType,
    DateType,
    TimestampType
)

from common.spark_session import get_spark
from common.config import (
    get_config,
    get_pipeline_config
)

from common.audit_logger import AuditLogger

# =============================================================================
# Spark
# =============================================================================

spark = get_spark("Silver Transformation")

# =============================================================================
# Configuration
# =============================================================================

config = get_config()

pipeline_config = get_pipeline_config()

BRONZE_PATH = config["bronze_path"]

SILVER_PATH = config["silver_path"]

DATABASE = config["database"]

# =============================================================================
# Pipeline Parameters
# =============================================================================

PIPELINE_NAME = "Retail Batch Pipeline"

LAYER_NAME = "Silver"

SOURCE_TABLE = "bronze_sales"

TARGET_TABLE = "silver_sales"

# =============================================================================
# Audit
# =============================================================================

audit = AuditLogger(

    spark=spark,

    config=config,

    pipeline_name=PIPELINE_NAME,

    layer=LAYER_NAME,

    notebook_name="silver_transform"

)

# =============================================================================
# Silver Schema
# =============================================================================

silver_schema = StructType([

    StructField("sale_id", IntegerType(), False),

    StructField("store_id", StringType(), False),

    StructField("customer_id", StringType(), False),

    StructField("product_id", StringType(), False),

    StructField("sale_date", DateType(), False),

    StructField("quantity", IntegerType(), False),

    StructField("price", DoubleType(), False),

    StructField("ingestion_timestamp", TimestampType(), False),

    StructField("source_file_name", StringType(), True),

    StructField("pipeline_name", StringType(), True),

    StructField("layer_name", StringType(), True),

    StructField("record_hash", StringType(), True)

])

# =============================================================================
# Read Bronze Delta
# =============================================================================

def read_bronze():

    bronze_path = f"{BRONZE_PATH}/sales"

    df = (

        spark.read

        .format("delta")

        .load(bronze_path)

    )

    return df

# =============================================================================
# Helper Functions
# =============================================================================

def get_silver_path():

    return f"{SILVER_PATH}/sales"

def get_silver_table():

    return f"{DATABASE}.{TARGET_TABLE}"

def show_dataframe_info(df):

    print("=" * 80)

    print("Schema")

    df.printSchema()

    print("=" * 80)

    print("Record Count :", df.count())

    print("=" * 80)


# =============================================================================
# Remove Duplicate Records
# =============================================================================

from pyspark.sql.window import Window

from pyspark.sql.functions import (
    row_number,
    when,
    round
)


def remove_duplicates(df):
    """
    Keep the latest record for each sale_id
    based on ingestion_timestamp.
    """

    window_spec = (

        Window

        .partitionBy("sale_id")

        .orderBy(
            col("ingestion_timestamp").desc()
        )

    )

    deduplicated_df = (

        df

        .withColumn(
            "row_num",
            row_number().over(window_spec)
        )

        .filter(
            col("row_num") == 1
        )

        .drop("row_num")

    )

    return deduplicated_df


# =============================================================================
# Handle NULL Values
# =============================================================================

def handle_nulls(df):
    """
    Replace NULL values with business defaults.
    """

    cleaned_df = (

        df.fillna({

            "quantity": 0,

            "price": 0.0,

            "customer_id": "UNKNOWN",

            "product_id": "UNKNOWN"

        })

    )

    return cleaned_df


# =============================================================================
# Standardize Text Columns
# =============================================================================

def standardize_columns(df):
    """
    Standardize text columns.
    """

    standardized_df = (

        df

        .withColumn(
            "store_id",
            upper(trim(col("store_id")))
        )

        .withColumn(
            "customer_id",
            upper(trim(col("customer_id")))
        )

        .withColumn(
            "product_id",
            upper(trim(col("product_id")))
        )

    )

    return standardized_df


# =============================================================================
# Business Rule Validation
# =============================================================================

def apply_business_rules(df):
    """
    Remove invalid business records.
    """

    filtered_df = (

        df

        .filter(col("quantity") > 0)

        .filter(col("price") > 0)

        .filter(col("sale_date") <= current_date())

    )

    return filtered_df


# =============================================================================
# Derived Columns
# =============================================================================

def derive_columns(df):
    """
    Create calculated business columns.
    """

    transformed_df = (

        df

        .withColumn(

            "sales_amount",

            round(

                col("quantity") * col("price"),

                2

            )

        )

        .withColumn(

            "processing_timestamp",

            current_timestamp()

        )

        .withColumn(

            "record_status",

            lit("ACTIVE")

        )

    )

    return transformed_df


# =============================================================================
# Data Quality Metrics
# =============================================================================

def print_metrics(original_df, transformed_df):

    original_count = original_df.count()

    transformed_count = transformed_df.count()

    rejected = original_count - transformed_count

    print("=" * 80)

    print("Silver Transformation Metrics")

    print("=" * 80)

    print(f"Input Records      : {original_count}")

    print(f"Output Records     : {transformed_count}")

    print(f"Rejected Records   : {rejected}")

    print("=" * 80)


# =============================================================================
# Main Transformation Pipeline
# =============================================================================

def transform_to_silver(df):
    """
    Complete Silver Transformation Pipeline.
    """

    print("=" * 80)
    print("Starting Silver Transformations")
    print("=" * 80)

    original_df = df

    df = remove_duplicates(df)

    df = handle_nulls(df)

    df = standardize_columns(df)

    df = apply_business_rules(df)

    df = derive_columns(df)

    print_metrics(

        original_df,

        df

    )

    return df


# =============================================================================
# Delta Imports
# =============================================================================

from delta.tables import DeltaTable

from pyspark.sql.utils import AnalysisException


# =============================================================================
# Create Silver Table (First Run)
# =============================================================================

def create_silver_table(df):
    """
    Creates the Silver Delta table if it does not already exist.
    """

    silver_path = get_silver_path()

    (
        df.write

        .format("delta")

        .mode("overwrite")

        .save(silver_path)
    )

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {get_silver_table()}
        USING DELTA
        LOCATION '{silver_path}'
    """)

    print("Silver Delta table created.")


# =============================================================================
# Check Table Exists
# =============================================================================

def silver_table_exists():
    """
    Returns True if the Silver Delta table exists.
    """

    try:

        spark.read.format("delta").load(
            get_silver_path()
        )

        return True

    except AnalysisException:

        return False

    except Exception:

        return False


# =============================================================================
# Delta MERGE
# =============================================================================

def merge_into_silver(silver_df):
    """
    Incrementally MERGE records into Silver Delta table.

    Business Key:
        sale_id

    Update Condition:
        record_hash has changed
    """

    silver_path = get_silver_path()

    delta_table = DeltaTable.forPath(

        spark,

        silver_path

    )

    (

        delta_table.alias("target")

        .merge(

            silver_df.alias("source"),

            "target.sale_id = source.sale_id"

        )

        .whenMatchedUpdate(

            condition="""
                target.record_hash <> source.record_hash
            """,

            set={

                "store_id": "source.store_id",

                "customer_id": "source.customer_id",

                "product_id": "source.product_id",

                "sale_date": "source.sale_date",

                "quantity": "source.quantity",

                "price": "source.price",

                "sales_amount": "source.sales_amount",

                "processing_timestamp":
                    "source.processing_timestamp",

                "record_hash":
                    "source.record_hash"

            }

        )

        .whenNotMatchedInsertAll()

        .execute()

    )

    print("MERGE Completed Successfully.")


# =============================================================================
# Save Silver Data
# =============================================================================

def load_to_silver(df):
    """
    Handles first load and incremental load.
    """

    if not silver_table_exists():

        print("Silver table not found.")

        print("Creating Silver table...")

        create_silver_table(df)

    else:

        print("Silver table exists.")

        print("Starting Incremental MERGE...")

        merge_into_silver(df)


# =============================================================================
# Register Silver Table
# =============================================================================

def register_silver_table():
    """
    Registers the Silver Delta table in the metastore.
    Safe to execute multiple times.
    """

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {get_silver_table()}
        USING DELTA
        LOCATION '{get_silver_path()}'
    """)

    print("Silver table registered.")


# =============================================================================
# Optimize Silver Table
# =============================================================================

def optimize_silver():
    """
    Optimizes the Delta table.

    NOTE:
    Usually scheduled separately in production.
    Included here for demonstration.
    """

    try:

        spark.sql(f"""
            OPTIMIZE {get_silver_table()}
            ZORDER BY (sale_id, store_id)
        """)

        print("OPTIMIZE completed.")

    except Exception as ex:

        print(f"OPTIMIZE skipped : {ex}")


# =============================================================================
# Vacuum
# =============================================================================

def vacuum_silver():

    """
    Removes obsolete Delta files.

    NOTE:
    Normally executed weekly/daily
    by maintenance jobs.
    """

    try:

        spark.sql(f"""

            VACUUM {get_silver_table()}

            RETAIN 168 HOURS

        """)

        print("VACUUM completed.")

    except Exception as ex:

        print(f"VACUUM skipped : {ex}")


# =============================================================================
# Main Silver Pipeline
# =============================================================================

def silver_pipeline():

    audit.start()

    try:

        print("=" * 80)
        print("Reading Bronze Delta")
        print("=" * 80)

        bronze_df = read_bronze()

        input_records = bronze_df.count()

        print(f"Input Records : {input_records}")

        print("=" * 80)
        print("Running Transformations")
        print("=" * 80)

        silver_df = transform_to_silver(
            bronze_df
        )

        output_records = silver_df.count()

        print(f"Output Records : {output_records}")

        print("=" * 80)
        print("Loading Silver")
        print("=" * 80)

        load_to_silver(
            silver_df
        )

        register_silver_table()

        optimize_silver()

        vacuum_silver()

        audit.success(

            records_read=input_records,

            records_written=output_records

        )

        print("=" * 80)
        print("Silver Pipeline Completed")
        print("=" * 80)

    except Exception as ex:

        audit.failure(ex)

        raise


# =============================================================================
# Execute Notebook
# =============================================================================

if __name__ == "__main__":

    silver_pipeline()
