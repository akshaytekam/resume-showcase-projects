"""
===============================================================================
File Name : bronze_load.py

Description:
    Loads raw sales CSV files into the Bronze Delta Layer.

Business Purpose
----------------
Reads raw source files from Landing Zone and stores them in
Bronze Delta Tables without applying business transformations.

Features
--------
✓ Reads CSV files
✓ Adds ingestion metadata
✓ Writes Delta Tables
✓ Audit Logging
✓ Error Handling

Author:
Enterprise Data Engineering Team

Version:
1.0
===============================================================================
"""

from pyspark.sql.functions import (
    current_timestamp,
    input_file_name,
    lit
)

from common.spark_session import get_spark
from common.config import get_config
from common.audit_logger import AuditLogger


# =============================================================================
# Create Spark Session
# =============================================================================

spark = get_spark("Bronze Load")

config = get_config()

audit = AuditLogger(
    spark=spark,
    config=config,
    pipeline_name="Retail Batch Pipeline",
    layer="Bronze",
    notebook_name="bronze_load"
)


# =============================================================================
# Read Raw Sales Data
# =============================================================================

def read_sales():

    landing_path = (
        config["landing_path"]
        + "/sales/*.csv"
    )

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(landing_path)
    )

    return df


# =============================================================================
# Add Metadata Columns
# =============================================================================

def enrich_metadata(df):

    return (
        df

        .withColumn(
            "ingestion_timestamp",
            current_timestamp()
        )

        .withColumn(
            "source_file",
            input_file_name()
        )

        .withColumn(
            "pipeline_name",
            lit("Retail Batch Pipeline")
        )

    )


# =============================================================================
# Write Bronze Delta Table
# =============================================================================

def write_bronze(df):

    bronze_path = (
        config["bronze_path"]
        + "/sales"
    )

    (
        df.write

        .format("delta")

        .mode("append")

        .save(bronze_path)

    )


# =============================================================================
# Main Pipeline
# =============================================================================

def bronze_pipeline():

    audit.start()

    try:

        sales_df = read_sales()

        records_read = sales_df.count()

        bronze_df = enrich_metadata(sales_df)

        write_bronze(bronze_df)

        records_written = bronze_df.count()

        audit.success(

            records_read=records_read,

            records_written=records_written

        )

        print(
            "Bronze Load Completed Successfully."
        )

    except Exception as ex:

        audit.failure(ex)


# =============================================================================
# Execute
# =============================================================================

if __name__ == "__main__":

    bronze_pipeline()
