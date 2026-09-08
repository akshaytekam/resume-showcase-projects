# the reusable data-quality validation module.
# The idea is to keep validation logic outside the Bronze/Silver notebooks so we can reuse the same rules for both batch and streaming pipelines.

from pyspark.sql import DataFrame
from pyspark.sql.functions import col


# -------------------------------
# 1. Required Column Validation
# --------------------

REQUIRED_COLUMNS = [
    "event_id",
    "vehicle_id",
    "event_ts"
]


def validate_required_columns(df: DataFrame) -> DataFrame:
    """
    Keep records where mandatory columns are not NULL.
    """

    condition = (
        col("event_id").isNotNull()
        & col("vehicle_id").isNotNull()
        & col("event_ts").isNotNull()
    )

    return df.filter(condition)


# ---------------------------------------------------------
# 2. Speed Validation
# ------------------------------------------------------------

def validate_speed(df: DataFrame) -> DataFrame:
    """
    ADAS vehicle speed must be >= 0.
    """

    return df.filter(
        col("speed_kmph").isNull()
        | (col("speed_kmph") >= 0)
    )


# ------------------------------------------------------------
# 3. Confidence Validation
# ------------------------------------------------------

def validate_confidence(df: DataFrame) -> DataFrame:
    """
    Confidence score must be between 0 and 1.
    """

    return df.filter(
        col("confidence").isNull()
        | (
            (col("confidence") >= 0)
            & (col("confidence") <= 1)
        )
    )


# ------------------------------------------------------------
# 4. Sensor Validation
# ---------------------------------------------------

VALID_SENSORS = [
    "camera",
    "radar",
    "lidar",
    "ultrasonic"
]


def validate_sensor(df: DataFrame) -> DataFrame:

    return df.filter(
        col("sensor_type").isNull()
        | col("sensor_type").isin(VALID_SENSORS)
    )


# ------------------------------------------------------------
# 5. Severity Validation
# ------------------------------------------------------------

VALID_SEVERITIES = [
    "LOW",
    "MEDIUM",
    "HIGH"
]


def validate_severity(df: DataFrame) -> DataFrame:

    return df.filter(
        col("severity").isNull()
        | col("severity").isin(VALID_SEVERITIES)
    )


# ------------------------------------------------------------
# 6. Event Type Validation
# ------------------------------------------------------------

VALID_EVENT_TYPES = [
    "lane_departure",
    "forward_collision",
    "pedestrian_detected",
    "blind_spot",
    "adaptive_cruise",
    "normal"
]


def validate_event_type(df: DataFrame) -> DataFrame:

    return df.filter(
        col("event_type").isNull()
        | col("event_type").isin(VALID_EVENT_TYPES)
    )


# ------------------------------------------------------------
# 7. Duplicate Validation
# ------------------------------------------------------------

def remove_duplicates(df: DataFrame) -> DataFrame:
    """
    event_id uniquely identifies an ADAS event.
    """

    return df.dropDuplicates(["event_id"])


# ------------------------------------------------------------
# 8. Apply All Validations
# ------------------------------------------------------------

def apply_validations(df: DataFrame) -> DataFrame:
    """
    Apply all reusable ADAS data-quality rules.
    """

    df = validate_required_columns(df)

    df = validate_speed(df)

    df = validate_confidence(df)

    df = validate_sensor(df)

    df = validate_severity(df)

    df = validate_event_type(df)

    df = remove_duplicates(df)

    return df
