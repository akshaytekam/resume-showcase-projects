from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    lower,
    upper,
    trim,
    to_date,
    hour
)

from validations import (
    validate_required_columns,
    validate_speed,
    validate_confidence,
    validate_sensor,
    validate_severity,
    validate_event_type
)


# ============================================================
# 1. Clean ADAS Data
# ============================================================

def clean_adas_data(df: DataFrame) -> DataFrame:
    """
    Standardize and clean ADAS event data.
    """

    df = (
        df
        .withColumn("event_id", trim(col("event_id")))
        .withColumn("vehicle_id", trim(col("vehicle_id")))
        .withColumn("vehicle_model", trim(col("vehicle_model")))
        .withColumn("oem", upper(trim(col("oem"))))
        .withColumn("sensor_type", lower(trim(col("sensor_type"))))
        .withColumn("event_type", lower(trim(col("event_type"))))
        .withColumn("severity", upper(trim(col("severity"))))
    )

    return df


# ============================================================
# 2. Apply Data Quality Rules
# ============================================================

def apply_silver_validations(df: DataFrame) -> DataFrame:
    """
    Apply all ADAS business/data-quality validations.
    """

    df = validate_required_columns(df)

    df = validate_speed(df)

    df = validate_confidence(df)

    df = validate_sensor(df)

    df = validate_severity(df)

    df = validate_event_type(df)

    return df


# ============================================================
# 3. Add Analytical Columns
# ============================================================

def add_silver_columns(df: DataFrame) -> DataFrame:
    """
    Add commonly used analytical columns.
    """

    return (
        df
        .withColumn(
            "event_date",
            to_date(col("event_ts"))
        )
        .withColumn(
            "event_hour",
            hour(col("event_ts"))
        )
    )


# ============================================================
# 4. Batch Silver Transformation
# ============================================================

def transform_batch_to_silver(
    bronze_df: DataFrame
) -> DataFrame:
    """
    Transform batch Bronze data into Silver.
    """

    df = clean_adas_data(bronze_df)

    df = apply_silver_validations(df)

    df = add_silver_columns(df)

    # event_id uniquely identifies an ADAS event
    df = df.dropDuplicates(["event_id"])

    return df


# ============================================================
# 5. Streaming Silver Transformation
# ============================================================

def transform_stream_to_silver(
    bronze_df: DataFrame
) -> DataFrame:
    """
    Transform streaming Bronze data into Silver.
    """

    df = clean_adas_data(bronze_df)

    df = apply_silver_validations(df)

    # Allow events to arrive up to 30 minutes late.
    df = df.withWatermark(
        "event_ts",
        "30 minutes"
    )

    # Deduplicate events while respecting
    # the event-time watermark.
    df = df.dropDuplicates(
        ["event_id"]
    )

    df = add_silver_columns(df)

    return df
