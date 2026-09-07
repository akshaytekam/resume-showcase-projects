# We want Spark to know exactly what columns and data types we expect.

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    DoubleType
)


# ---------------------------------------
# ADAS Event Schema
# ---------------------

ADAS_EVENT_SCHEMA = StructType([

    StructField(
        "event_id",
        StringType(),
        nullable=False
    ),

    StructField(
        "vehicle_id",
        StringType(),
        nullable=False
    ),

    StructField(
        "vehicle_model",
        StringType(),
        nullable=True
    ),

    StructField(
        "oem",
        StringType(),
        nullable=True
    ),

    StructField(
        "event_ts",
        TimestampType(),
        nullable=False
    ),

    StructField(
        "sensor_type",
        StringType(),
        nullable=True
    ),

    StructField(
        "event_type",
        StringType(),
        nullable=True
    ),

    StructField(
        "speed_kmph",
        DoubleType(),
        nullable=True
    ),

    StructField(
        "confidence",
        DoubleType(),
        nullable=True
    ),

    StructField(
        "severity",
        StringType(),
        nullable=True
    )
])
