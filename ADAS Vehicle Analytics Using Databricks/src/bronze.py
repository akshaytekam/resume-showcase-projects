from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    current_timestamp,
    input_file_name
)

from config import (
    BATCH_RAW_PATH,
    STREAM_RAW_PATH,
    BRONZE_TABLE
)

from schema import ADAS_EVENT_SCHEMA


# ==============================================
# 1. Batch Bronze
# ============================================================

def read_batch_adas(spark) -> DataFrame:
    """
    Read historical ADAS CSV files as a batch.
    """

    df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .schema(ADAS_EVENT_SCHEMA)
        .load(BATCH_RAW_PATH)
    )

    return (
        df
        .withColumn("_ingestion_ts", current_timestamp())
        .withColumn("_source_file", input_file_name())
    )


# ============================================================
# 2. Streaming Bronze
# ============================================================

def read_streaming_adas(spark) -> DataFrame:
    """
    Read incoming ADAS JSON files using Auto Loader.
    """

    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema(ADAS_EVENT_SCHEMA)
        .load(STREAM_RAW_PATH)
    )

    return (
        df
        .withColumn("_ingestion_ts", current_timestamp())
        .withColumn("_source_file", input_file_name())
    )


# ============================================================
# 3. Write Batch Bronze
# ============================================================

def write_batch_bronze(
    df: DataFrame
) -> None:
    """
    Write batch ADAS records to Bronze Delta table.
    """

    (
        df.write
        .format("delta")
        .mode("append")
        .saveAsTable(BRONZE_TABLE)
    )


# ============================================================
# 4. Write Streaming Bronze
# ============================================================

def write_streaming_bronze(
    df: DataFrame,
    checkpoint_path: str
):
    """
    Write streaming ADAS records to Bronze.
    """

    return (
        df.writeStream
        .format("delta")
        .outputMode("append")
        .option(
            "checkpointLocation",
            checkpoint_path
        )
        .toTable(BRONZE_TABLE)
    )
