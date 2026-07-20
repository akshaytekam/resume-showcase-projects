"""
===============================================================================

Notebook Name : gold_sales_summary.py

Layer         : Gold

Description
-----------
Creates Daily Sales Summary for Business Reporting.

Business Users
--------------
Finance
Sales
Management
Power BI

Features
--------
✓ Daily Revenue
✓ Total Orders
✓ Total Quantity
✓ Average Order Value
✓ Audit Logging
✓ Delta Lake
✓ Incremental Processing

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
    count,
    sum,
    avg,
    round,
    current_timestamp,
    current_date,
    lit
)

from pyspark.sql.types import (
    StructType,
    StructField,
    DateType,
    IntegerType,
    DoubleType,
    TimestampType,
    StringType
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

spark = get_spark("Gold Sales Summary")

# =============================================================================
# Configuration
# =============================================================================

config = get_config()

pipeline_config = get_pipeline_config()

SILVER_PATH = config["silver_path"]

GOLD_PATH = config["gold_path"]

DATABASE = config["database"]

# =============================================================================
# Pipeline Parameters
# =============================================================================

PIPELINE_NAME = "Retail Batch Pipeline"

LAYER_NAME = "Gold"

SOURCE_TABLE = "silver_sales"

TARGET_TABLE = "gold_sales_summary"

# =============================================================================
# Audit Logger
# =============================================================================

audit = AuditLogger(

    spark=spark,

    config=config,

    pipeline_name=PIPELINE_NAME,

    layer=LAYER_NAME,

    notebook_name="gold_sales_summary"

)

# =============================================================================
# Gold Schema
# =============================================================================

gold_schema = StructType([

    StructField(
        "sale_date",
        DateType(),
        False
    ),

    StructField(
        "total_orders",
        IntegerType(),
        False
    ),

    StructField(
        "total_quantity",
        IntegerType(),
        False
    ),

    StructField(
        "total_revenue",
        DoubleType(),
        False
    ),

    StructField(
        "average_order_value",
        DoubleType(),
        False
    ),

    StructField(
        "processing_timestamp",
        TimestampType(),
        False
    ),

    StructField(
        "pipeline_name",
        StringType(),
        True
    )

])

# =============================================================================
# Read Silver Table
# =============================================================================

def read_silver():

    silver_path = f"{SILVER_PATH}/sales"

    df = (

        spark.read

        .format("delta")

        .load(silver_path)

    )

    return df

# =============================================================================
# Helper Functions
# =============================================================================

def get_gold_path():

    return f"{GOLD_PATH}/sales_summary"

def get_gold_table():

    return f"{DATABASE}.{TARGET_TABLE}"

def print_dataframe_info(df):

    print("=" * 80)

    print("Schema")

    df.printSchema()

    print("=" * 80)

    print(f"Total Records : {df.count()}")

    print("=" * 80)

def validate_required_columns(df):

    """
    Ensures required columns exist
    before business aggregations.
    """

    required_columns = [

        "sale_date",

        "sale_id",

        "quantity",

        "sales_amount"

    ]

    missing_columns = [

        c

        for c in required_columns

        if c not in df.columns

    ]

    if len(missing_columns) > 0:

        raise Exception(

            f"Missing Columns : {missing_columns}"

        )

# =============================================================================
# Read Source Data
# =============================================================================

def load_source():

    df = read_silver()

    validate_required_columns(df)

    print_dataframe_info(df)

    return df


# =============================================================================
# Business KPI Aggregations
# =============================================================================

from pyspark.sql.functions import (
    countDistinct,
    expr
)


def calculate_daily_sales(df):
    """
    Creates daily business KPIs from Silver data.
    """

    gold_df = (

        df

        .groupBy("sale_date")

        .agg(

            countDistinct("sale_id").alias(
                "total_orders"
            ),

            sum("quantity").alias(
                "total_quantity"
            ),

            round(

                sum("sales_amount"),

                2

            ).alias(

                "total_revenue"

            ),

            round(

                avg("sales_amount"),

                2

            ).alias(

                "average_order_value"

            ),

            round(

                avg("price"),

                2

            ).alias(

                "average_selling_price"

            ),

            countDistinct(

                "customer_id"

            ).alias(

                "unique_customers"

            ),

            countDistinct(

                "product_id"

            ).alias(

                "unique_products"

            )

        )

    )

    return gold_df


# =============================================================================
# Derived Business KPIs
# =============================================================================

def derive_business_metrics(df):
    """
    Creates additional business metrics.
    """

    df = (

        df

        .withColumn(

            "average_items_per_order",

            round(

                col("total_quantity")

                /

                col("total_orders"),

                2

            )

        )

        .withColumn(

            "processing_timestamp",

            current_timestamp()

        )

        .withColumn(

            "pipeline_name",

            lit(PIPELINE_NAME)

        )

    )

    return df


# =============================================================================
# Data Quality Checks
# =============================================================================

def validate_gold_metrics(df):
    """
    Validate Gold KPI values.
    """

    invalid_rows = (

        df

        .filter(

            (col("total_revenue") < 0)

            |

            (col("total_orders") <= 0)

            |

            (col("total_quantity") <= 0)

        )

    )

    invalid_count = invalid_rows.count()

    if invalid_count > 0:

        raise Exception(

            f"Invalid KPI records found : {invalid_count}"

        )

    print("Gold KPI validation passed.")


# =============================================================================
# Print Business Summary
# =============================================================================

def print_business_summary(df):

    print("=" * 80)

    print("Business KPI Summary")

    print("=" * 80)

    df.orderBy(

        col("sale_date")

    ).show(

        truncate=False

    )

    print("=" * 80)

    print(f"Business Days : {df.count()}")

    print("=" * 80)


# =============================================================================
# Gold Transformation Pipeline
# =============================================================================

def transform_to_gold(df):
    """
    Executes Gold transformations.
    """

    print("=" * 80)

    print("Calculating Business KPIs")

    print("=" * 80)

    gold_df = calculate_daily_sales(df)

    gold_df = derive_business_metrics(gold_df)

    validate_gold_metrics(gold_df)

    print_business_summary(gold_df)

    return gold_df


# =============================================================================
# Delta Imports
# =============================================================================

from delta.tables import DeltaTable
from pyspark.sql.utils import AnalysisException


# =============================================================================
# Create Gold Table
# =============================================================================

def create_gold_table(df):
    """
    Creates the Gold Delta table on first execution.
    """

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .save(get_gold_path())
    )

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {get_gold_table()}
        USING DELTA
        LOCATION '{get_gold_path()}'
    """)

    print("Gold table created.")


# =============================================================================
# Check Gold Table Exists
# =============================================================================

def gold_table_exists():

    try:

        spark.read.format("delta").load(
            get_gold_path()
        )

        return True

    except AnalysisException:

        return False

    except Exception:

        return False


# =============================================================================
# Incremental MERGE
# =============================================================================

def merge_into_gold(gold_df):
    """
    Incrementally merges KPI data into Gold.
    Business Key:
        sale_date
    """

    delta_table = DeltaTable.forPath(

        spark,

        get_gold_path()

    )

    (

        delta_table.alias("target")

        .merge(

            gold_df.alias("source"),

            "target.sale_date = source.sale_date"

        )

        .whenMatchedUpdate(

            set={

                "total_orders":
                    "source.total_orders",

                "total_quantity":
                    "source.total_quantity",

                "total_revenue":
                    "source.total_revenue",

                "average_order_value":
                    "source.average_order_value",

                "average_selling_price":
                    "source.average_selling_price",

                "unique_customers":
                    "source.unique_customers",

                "unique_products":
                    "source.unique_products",

                "average_items_per_order":
                    "source.average_items_per_order",

                "processing_timestamp":
                    "source.processing_timestamp",

                "pipeline_name":
                    "source.pipeline_name"

            }

        )

        .whenNotMatchedInsertAll()

        .execute()

    )

    print("Gold MERGE completed.")


# =============================================================================
# Load Gold
# =============================================================================

def load_gold(df):

    if not gold_table_exists():

        print("Gold table not found.")

        create_gold_table(df)

    else:

        print("Incremental Gold MERGE")

        merge_into_gold(df)


# =============================================================================
# Register Gold Table
# =============================================================================

def register_gold_table():

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {get_gold_table()}
        USING DELTA
        LOCATION '{get_gold_path()}'
    """)

    print("Gold table registered.")


# =============================================================================
# Optimize
# =============================================================================

def optimize_gold():

    try:

        spark.sql(f"""

            OPTIMIZE {get_gold_table()}

            ZORDER BY (sale_date)

        """)

        print("OPTIMIZE completed.")

    except Exception as ex:

        print(f"OPTIMIZE skipped : {ex}")


# =============================================================================
# Vacuum
# =============================================================================

def vacuum_gold():

    try:

        spark.sql(f"""

            VACUUM {get_gold_table()}

            RETAIN 168 HOURS

        """)

        print("VACUUM completed.")

    except Exception as ex:

        print(f"VACUUM skipped : {ex}")


# =============================================================================
# Main Gold Pipeline
# =============================================================================

def gold_pipeline():

    audit.start()

    try:

        print("=" * 80)
        print("Reading Silver")
        print("=" * 80)

        silver_df = load_source()

        input_records = silver_df.count()

        print("=" * 80)
        print("Building Gold KPIs")
        print("=" * 80)

        gold_df = transform_to_gold(
            silver_df
        )

        output_records = gold_df.count()

        print("=" * 80)
        print("Loading Gold")
        print("=" * 80)

        load_gold(gold_df)

        register_gold_table()

        optimize_gold()

        vacuum_gold()

        audit.success(

            records_read=input_records,

            records_written=output_records

        )

        print("=" * 80)
        print("Gold Pipeline Completed")
        print("=" * 80)

    except Exception as ex:

        audit.failure(ex)

        raise


# =============================================================================
# Execute
# =============================================================================

if __name__ == "__main__":

    gold_pipeline()
