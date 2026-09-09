from pyspark import pipelines as dp
from pyspark.sql.functions import (
    current_timestamp,
    trim,
    upper,
    lower,
    col,
    to_date,
    to_timestamp,
    hour,
    expr
)


# =============================
# Configuration
# =================================================

CATALOG = "adas_catalog"

# Pipeline parameter can be configured as:
# dev / test / prod

ENV = spark.conf.get(
    "adas.environment",
    "dev"
)

VOLUME_PATH = (
    f"/Volumes/{CATALOG}/{ENV}/adas_volume"
)

BATCH_PATH = (
    f"{VOLUME_PATH}/raw/batch/"
)

STREAM_PATH = (
    f"{VOLUME_PATH}/raw/stream/"
)


# =====================================
# 1. BRONZE - Batch
# ==================================================

@dp.table(
    name="bronze_adas_batch",
    comment="Raw ADAS batch events"
)
def bronze_adas_batch():

    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(BATCH_PATH)

        .withColumn(
            "_ingestion_ts",
            current_timestamp()
        )

        .withColumn(
            "_source_file",
            col("_metadata.file_path")
        )
    )


# ===================================================
# 2. BRONZE - Streaming
# ============================================================

@dp.table(
    name="bronze_adas_stream",
    comment="Raw ADAS streaming events"
)
def bronze_adas_stream():

    return (
        spark.readStream
        .format("cloudFiles")
        .option(
            "cloudFiles.format",
            "json"
        )
        .option(
            "cloudFiles.inferColumnTypes",
            "true"
        )
        .load(STREAM_PATH)

        .withColumn(
            "_ingestion_ts",
            current_timestamp()
        )

        .withColumn(
            "_source_file",
            col("_metadata.file_path")
        )
    )


# ========================================
# 3. SILVER - Batch
# ============================================================

@dp.table(
    name="silver_adas_batch",
    comment="Validated and cleaned ADAS batch events"
)

@dp.expect_or_drop(
    "valid_event_id",
    "event_id IS NOT NULL"
)

@dp.expect_or_drop(
    "valid_vehicle_id",
    "vehicle_id IS NOT NULL"
)

@dp.expect_or_drop(
    "valid_event_timestamp",
    "event_ts IS NOT NULL"
)

@dp.expect_or_drop(
    "valid_speed",
    "speed_kmph IS NULL OR speed_kmph >= 0"
)

@dp.expect_or_drop(
    "valid_confidence",
    "confidence IS NULL OR "
    "(confidence >= 0 AND confidence <= 1)"
)

def silver_adas_batch():

    df = spark.read.table(
        "bronze_adas_batch"
    )

    return (
        df

        .withColumn(
            "event_id",
            trim(col("event_id"))
        )

        .withColumn(
            "vehicle_id",
            trim(col("vehicle_id"))
        )

        .withColumn(
            "vehicle_model",
            trim(col("vehicle_model"))
        )

        .withColumn(
            "oem",
            upper(trim(col("oem")))
        )

        .withColumn(
            "sensor_type",
            lower(trim(col("sensor_type")))
        )

        .withColumn(
            "event_type",
            lower(trim(col("event_type")))
        )

        .withColumn(
            "severity",
            upper(trim(col("severity")))
        )

        .withColumn(
            "event_date",
            to_date(col("event_ts"))
        )

        .withColumn(
            "event_hour",
            hour(col("event_ts"))
        )

        .dropDuplicates(
            ["event_id"]
        )
    )


# ======================================
# 4. SILVER - Streaming
# ============================================================

@dp.table(
    name="silver_adas_stream",
    comment="Validated and cleaned ADAS streaming events"
)

@dp.expect_or_drop(
    "valid_event_id",
    "event_id IS NOT NULL"
)

@dp.expect_or_drop(
    "valid_vehicle_id",
    "vehicle_id IS NOT NULL"
)

@dp.expect_or_drop(
    "valid_event_timestamp",
    "event_ts IS NOT NULL"
)

@dp.expect_or_drop(
    "valid_speed",
    "speed_kmph IS NULL OR speed_kmph >= 0"
)

@dp.expect_or_drop(
    "valid_confidence",
    "confidence IS NULL OR "
    "(confidence >= 0 AND confidence <= 1)"
)

def silver_adas_stream():

    df = spark.readStream.table(
        "bronze_adas_stream"
    )

    return (
        df

        .withColumn(
            "event_id",
            trim(col("event_id"))
        )

        .withColumn(
            "vehicle_id",
            trim(col("vehicle_id"))
        )

        .withColumn(
            "vehicle_model",
            trim(col("vehicle_model"))
        )

        .withColumn(
            "oem",
            upper(trim(col("oem")))
        )

        .withColumn(
            "sensor_type",
            lower(trim(col("sensor_type")))
        )

        .withColumn(
            "event_type",
            lower(trim(col("event_type")))
        )

        .withColumn(
            "severity",
            upper(trim(col("severity")))
        )

        .withColumn(
            "event_date",
            to_date(col("event_ts"))
        )

        .withColumn(
            "event_hour",
            hour(col("event_ts"))
        )

        .withColumn(
            "event_ts",
            to_timestamp(col("event_ts"))
        )

        .withWatermark(
            "event_ts",
            "30 minutes"
        )

        .dropDuplicates(
            ["event_id"]
        )
    )


# ============================================
# 5. GOLD - Combined Events
# ============================================================

@dp.materialized_view(
    name="gold_adas_events",
    comment="Gold ADAS event dataset"
)

def gold_adas_events():

    batch_df = (
        spark.read.table(
            "silver_adas_batch"
        )
    )

    stream_df = (
        spark.read.table(
            "silver_adas_stream"
        )
    )

    return (
        batch_df
        .unionByName(
            stream_df,
            allowMissingColumns=True
        )
        .dropDuplicates(
            ["event_id"]
        )
    )

# ===============================================
# 6. BRONZE - Vehicle CDC Source
# ============================================================

@dp.table(
    name="adas_vehicle_cdc",
    comment="Vehicle CDC events for SCD Type 2 tracking"
)
def adas_vehicle_cdc():
    return (
        spark.readStream
        .format("cloudFiles")
        .option(
            "cloudFiles.format",
            "json"
        )
        .option(
            "cloudFiles.inferColumnTypes",
            "true"
        )
        .load(
            f"{VOLUME_PATH}/raw/vehicle_cdc/"
        )
    )


# =====================================
# 7. SCD TYPE 2 TARGET
# ============================================================

dp.create_streaming_table(
    name="dim_vehicle_history",
    comment="ADAS vehicle dimension with SCD Type 2 history"
)

dp.create_auto_cdc_flow(
    target="dim_vehicle_history",

    source="adas_vehicle_cdc",

    keys=[
        "vehicle_id"
    ],

    sequence_by=col("sequence"),

    apply_as_deletes=expr(
        "operation = 'DELETE'"
    ),

    except_column_list=[
        "operation",
        "sequence"
    ],

    stored_as_scd_type="2"
)
