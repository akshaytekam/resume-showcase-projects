from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    sha2,
    concat_ws,
    date_format
)

from config import (
    FACT_TABLE,
    DIM_VEHICLE_TABLE,
    DIM_SENSOR_TABLE,
    DIM_EVENT_TABLE,
    DIM_DATE_TABLE
)


# ============================================================
# 1. Vehicle Dimension
# ============================================================

def build_dim_vehicle(silver_df: DataFrame) -> DataFrame:

    return (
        silver_df
        .select(
            "vehicle_id",
            "vehicle_model",
            "oem"
        )
        .dropDuplicates(["vehicle_id"])
        .withColumn(
            "vehicle_key",
            sha2(col("vehicle_id"), 256)
        )
        .select(
            "vehicle_key",
            "vehicle_id",
            "vehicle_model",
            "oem"
        )
    )


# ============================================================
# 2. Sensor Dimension
# ============================================================

def build_dim_sensor(silver_df: DataFrame) -> DataFrame:

    return (
        silver_df
        .select("sensor_type")
        .dropDuplicates(["sensor_type"])
        .withColumn(
            "sensor_key",
            sha2(col("sensor_type"), 256)
        )
        .select(
            "sensor_key",
            "sensor_type"
        )
    )


# ============================================================
# 3. Event Dimension
# ============================================================

def build_dim_event(silver_df: DataFrame) -> DataFrame:

    return (
        silver_df
        .select(
            "event_type",
            "severity"
        )
        .dropDuplicates()
        .withColumn(
            "event_key",
            sha2(
                concat_ws(
                    "|",
                    col("event_type"),
                    col("severity")
                ),
                256
            )
        )
        .select(
            "event_key",
            "event_type",
            "severity"
        )
    )


# ============================================================
# 4. Date Dimension
# ============================================================

def build_dim_date(silver_df: DataFrame) -> DataFrame:

    return (
        silver_df
        .select("event_date")
        .dropDuplicates()
        .withColumn(
            "date_key",
            date_format(
                col("event_date"),
                "yyyyMMdd"
            ).cast("int")
        )
        .select(
            "date_key",
            "event_date"
        )
    )


# ============================================================
# 5. Fact Table
# ============================================================

def build_fact_adas_event(
    silver_df: DataFrame,
    dim_vehicle: DataFrame,
    dim_sensor: DataFrame,
    dim_event: DataFrame,
    dim_date: DataFrame
) -> DataFrame:

    fact_df = (
        silver_df.alias("s")

        # Vehicle
        .join(
            dim_vehicle.alias("v"),
            col("s.vehicle_id") ==
            col("v.vehicle_id"),
            "left"
        )

        # Sensor
        .join(
            dim_sensor.alias("sen"),
            col("s.sensor_type") ==
            col("sen.sensor_type"),
            "left"
        )

        # Event
        .join(
            (
                dim_event.alias("e")
            ),
            (
                (col("s.event_type") ==
                 col("e.event_type"))
                &
                (col("s.severity") ==
                 col("e.severity"))
            ),
            "left"
        )

        # Date
        .join(
            dim_date.alias("d"),
            col("s.event_date") ==
            col("d.event_date"),
            "left"
        )

        .select(
            col("s.event_id"),

            col("v.vehicle_key"),
            col("sen.sensor_key"),
            col("e.event_key"),
            col("d.date_key"),

            col("s.event_ts"),
            col("s.speed_kmph"),
            col("s.confidence"),

            col("s._ingestion_ts"),
            col("s._source_file")
        )
    )

    return fact_df


# ============================================================
# 6. Write Gold Tables
# ============================================================

def write_gold_tables(
    fact_df: DataFrame,
    dim_vehicle: DataFrame,
    dim_sensor: DataFrame,
    dim_event: DataFrame,
    dim_date: DataFrame
):

    dim_vehicle.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(DIM_VEHICLE_TABLE)

    dim_sensor.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(DIM_SENSOR_TABLE)

    dim_event.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(DIM_EVENT_TABLE)

    dim_date.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(DIM_DATE_TABLE)

    fact_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(FACT_TABLE)
