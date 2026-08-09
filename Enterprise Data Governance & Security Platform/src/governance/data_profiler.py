# ============================================================
# File        : data_profiler.py
# Project     : Enterprise Data Governance Platform
# Purpose     : Profile Delta datasets and generate metadata
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType,
    IntegerType,
    LongType,
    DoubleType,
    FloatType,
    DecimalType,
    DateType,
    TimestampType
)

from typing import Dict, List


# ============================================================
# 1. Spark Session
# ============================================================

spark = SparkSession.builder.getOrCreate()

# ============================================================
# 2. CONFIGURATION
# ============================================================

GOVERNANCE_CATALOG = "dev_catalog"
GOVERNANCE_SCHEMA = "governance"

PROFILE_TABLE = (
    f"{GOVERNANCE_CATALOG}."
    f"{GOVERNANCE_SCHEMA}."
    f"data_profile_results"
)

# ============================================================
# 3. CREATE PROFILE TABLE
# ============================================================

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {PROFILE_TABLE}
(
    catalog_name STRING,
    schema_name STRING,
    table_name STRING,
    column_name STRING,
    data_type STRING,

    row_count BIGINT,
    null_count BIGINT,
    null_percentage DOUBLE,

    distinct_count BIGINT,
    uniqueness_percentage DOUBLE,

    min_value STRING,
    max_value STRING,

    contains_numeric BOOLEAN,
    contains_email_pattern BOOLEAN,
    contains_phone_pattern BOOLEAN,

    profiled_at TIMESTAMP
)
USING DELTA
""")

# ============================================================
# 4. PROFILE DATASET
# ============================================================

def profile_dataset(
    catalog_name: str,
    schema_name: str,
    table_name: str
):
    """
    Profile a Unity Catalog managed table.

    Returns profiling statistics for every column.
    """

    table_name_full = (
        f"`{catalog_name}`."
        f"`{schema_name}`."
        f"`{table_name}`"
    )

    print(f"Profiling dataset: {table_name_full}")

    df = spark.table(table_name_full)

    total_rows = df.count()

    print(f"Total rows: {total_rows}")

    results = []

    for field in df.schema.fields:

        column_name = field.name
        data_type = field.dataType.simpleString()

        print(
            f"Profiling column: "
            f"{column_name} ({data_type})"
        )

        column_df = df.select(F.col(column_name))

        null_count = (
            column_df
            .filter(F.col(column_name).isNull())
            .count()
        )

        distinct_count = (
            column_df
            .select(column_name)
            .distinct()
            .count()
        )

        if total_rows > 0:
            null_percentage = (
                null_count / total_rows
            ) * 100

            uniqueness_percentage = (
                distinct_count / total_rows
            ) * 100
        else:
            null_percentage = 0.0
            uniqueness_percentage = 0.0

        # ====================================================
        # Email pattern detection
        # ====================================================

        contains_email_pattern = False

        if isinstance(field.dataType, StringType):

            email_count = (
                df
                .filter(
                    F.col(column_name)
                    .rlike(
                        r"^[A-Za-z0-9._%+-]+@"
                        r"[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
                    )
                )
                .count()
            )

            contains_email_pattern = (
                email_count > 0
            )

        # ====================================================
        # Phone pattern detection
        # ====================================================

        contains_phone_pattern = False

        if isinstance(field.dataType, StringType):

            phone_count = (
                df
                .filter(
                    F.col(column_name)
                    .rlike(
                        r"^\+?[0-9]{10,15}$"
                    )
                )
                .count()
            )

            contains_phone_pattern = (
                phone_count > 0
            )

        # ====================================================
        # Min / Max
        # ====================================================

        min_value = None
        max_value = None

        if isinstance(
            field.dataType,
            (
                IntegerType,
                LongType,
                DoubleType,
                FloatType,
                DecimalType,
                DateType,
                TimestampType
            )
        ):

            stats = (
                df
                .select(
                    F.min(
                        F.col(column_name)
                    ).alias("min_value"),

                    F.max(
                        F.col(column_name)
                    ).alias("max_value")
                )
                .collect()[0]
            )

            if stats["min_value"] is not None:
                min_value = str(
                    stats["min_value"]
                )

            if stats["max_value"] is not None:
                max_value = str(
                    stats["max_value"]
                )

        # ====================================================
        # Build profiling result
        # ====================================================

        results.append(
            {
                "catalog_name": catalog_name,
                "schema_name": schema_name,
                "table_name": table_name,
                "column_name": column_name,
                "data_type": data_type,

                "row_count": total_rows,
                "null_count": null_count,
                "null_percentage": null_percentage,

                "distinct_count": distinct_count,
                "uniqueness_percentage":
                    uniqueness_percentage,

                "min_value": min_value,
                "max_value": max_value,

                "contains_numeric":
                    not isinstance(
                        field.dataType,
                        StringType
                    ),

                "contains_email_pattern":
                    contains_email_pattern,

                "contains_phone_pattern":
                    contains_phone_pattern
            }
        )

    return results

# ============================================================
# 5. SAVE PROFILE RESULTS
# ============================================================

def save_profile_results(results: List[Dict]):

    if not results:
        print("No profiling results generated.")
        return

    profile_df = spark.createDataFrame(results)

    profile_df = profile_df.withColumn(
        "profiled_at",
        F.current_timestamp()
    )

    (
        profile_df
        .write
        .format("delta")
        .mode("append")
        .saveAsTable(PROFILE_TABLE)
    )

    print(
        f"Saved {profile_df.count()} "
        f"column profiles."
    )

# ============================================================
# 6. EXECUTION
# ============================================================

if __name__ == "__main__":

    results = profile_dataset(
        catalog_name="dev_catalog",
        schema_name="silver",
        table_name="customers"
    )

    save_profile_results(results)
