# ============================================================
# File        : lineage_tracker.py
# Project     : Enterprise Data Governance Platform
# Purpose     : Track dataset and column lineage
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from typing import List, Dict


# ============================================================
# 1. SPARK SESSION
# ============================================================

spark = SparkSession.builder.getOrCreate()

# ============================================================
# 2. CONFIGURATION
# ============================================================

GOVERNANCE_CATALOG = "dev_catalog"

GOVERNANCE_SCHEMA = "governance"

LINEAGE_TABLE = (
    f"{GOVERNANCE_CATALOG}."
    f"{GOVERNANCE_SCHEMA}."
    f"data_lineage_registry"
)

# ============================================================
# 3. CREATE LINEAGE TABLE
# ============================================================

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {LINEAGE_TABLE}
(
    lineage_id BIGINT
        GENERATED ALWAYS AS IDENTITY,

    source_catalog STRING,
    source_schema STRING,
    source_table STRING,
    source_column STRING,

    target_catalog STRING,
    target_schema STRING,
    target_table STRING,
    target_column STRING,

    relationship_type STRING,

    pipeline_name STRING,

    transformation_type STRING,

    transformation_logic STRING,

    data_classification STRING,

    is_active BOOLEAN,

    created_at TIMESTAMP,

    updated_at TIMESTAMP

)
USING DELTA
""")

# ============================================================
# 4. CREATE LINEAGE RECORD
# ============================================================

def create_lineage_record(
    source_catalog: str,
    source_schema: str,
    source_table: str,
    source_column: str,

    target_catalog: str,
    target_schema: str,
    target_table: str,
    target_column: str,

    relationship_type: str,
    pipeline_name: str,
    transformation_type: str,
    transformation_logic: str,
    data_classification: str
) -> Dict:

    return {

        "source_catalog": source_catalog,
        "source_schema": source_schema,
        "source_table": source_table,
        "source_column": source_column,

        "target_catalog": target_catalog,
        "target_schema": target_schema,
        "target_table": target_table,
        "target_column": target_column,

        "relationship_type": relationship_type,

        "pipeline_name": pipeline_name,

        "transformation_type":
            transformation_type,

        "transformation_logic":
            transformation_logic,

        "data_classification":
            data_classification,

        "is_active": True
    }

# ============================================================
# 5. REGISTER DATASET LINEAGE
# ============================================================

def register_dataset_lineage(
    source_catalog: str,
    source_schema: str,
    source_table: str,

    target_catalog: str,
    target_schema: str,
    target_table: str,

    pipeline_name: str,
    transformation_type: str,
    transformation_logic: str
):

    record = create_lineage_record(

        source_catalog=source_catalog,
        source_schema=source_schema,
        source_table=source_table,
        source_column="*",

        target_catalog=target_catalog,
        target_schema=target_schema,
        target_table=target_table,
        target_column="*",

        relationship_type="TRANSFORMED_FROM",

        pipeline_name=pipeline_name,

        transformation_type=
            transformation_type,

        transformation_logic=
            transformation_logic,

        data_classification="UNKNOWN"
    )

    save_lineage_records([record])

# ============================================================
# 6. SAVE LINEAGE RECORDS
# ============================================================

def save_lineage_records(
    records: List[Dict]
):

    if not records:

        print(
            "No lineage records to save."
        )

        return

    lineage_df = (
        spark.createDataFrame(records)
    )

    lineage_df = (
        lineage_df
        .withColumn(
            "created_at",
            F.current_timestamp()
        )
        .withColumn(
            "updated_at",
            F.current_timestamp()
        )
    )

    (
        lineage_df
        .write
        .format("delta")
        .mode("append")
        .saveAsTable(
            LINEAGE_TABLE
        )
    )

    print(
        f"Saved {lineage_df.count()} "
        f"lineage records."
    )

register_dataset_lineage(

    source_catalog="dev_catalog",
    source_schema="bronze",
    source_table="customers",

    target_catalog="dev_catalog",
    target_schema="silver",
    target_table="customers",

    pipeline_name="customer_silver_pipeline",

    transformation_type="CLEANING",

    transformation_logic=(
        "Removed duplicate customer records, "
        "standardized email values, "
        "trimmed string columns, "
        "validated mandatory customer_id."
    )
)

register_dataset_lineage(

    source_catalog="dev_catalog",
    source_schema="silver",
    source_table="customers",

    target_catalog="dev_catalog",
    target_schema="gold",
    target_table="customer_360",

    pipeline_name="customer_360_pipeline",

    transformation_type="ENRICHMENT",

    transformation_logic=(
        "Joined customer profile with "
        "transaction and loyalty data."
    )
)

# ============================================================
# 7. REGISTER COLUMN LINEAGE
# ============================================================

email_lineage = create_lineage_record(

    source_catalog="dev_catalog",
    source_schema="silver",
    source_table="customers",
    source_column="email",

    target_catalog="dev_catalog",
    target_schema="gold",
    target_table="customer_360",
    target_column="email",

    relationship_type="DIRECT",

    pipeline_name="customer_360_pipeline",

    transformation_type="STANDARDIZATION",

    transformation_logic=(
        "Lowercase and trim customer email."
    ),

    data_classification="PII"
)

save_lineage_records(
    [email_lineage]
)

masked_email_lineage = create_lineage_record(

    source_catalog="dev_catalog",
    source_schema="gold",
    source_table="customer_360",
    source_column="email",

    target_catalog="dev_catalog",
    target_schema="gold",
    target_table="customer_360_secure",
    target_column="email",

    relationship_type="MASKED",

    pipeline_name="customer_secure_view",

    transformation_type="MASKING",

    transformation_logic=(
        "Dynamic masking policy applied "
        "based on user role."
    ),

    data_classification="PII"
)

save_lineage_records(
    [masked_email_lineage]
)

snowflake_lineage = create_lineage_record(

    source_catalog="dev_catalog",
    source_schema="gold",
    source_table="customer_360",
    source_column="*",

    target_catalog="snowflake",
    target_schema="analytics",
    target_table="customer_360",
    target_column="*",

    relationship_type="PUBLISHED_TO",

    pipeline_name="snowflake_publish_pipeline",

    transformation_type="DATA_SHARE",

    transformation_logic=(
        "Published governed Gold dataset "
        "to Snowflake analytics layer."
    ),

    data_classification="MIXED"
)

save_lineage_records(
    [snowflake_lineage]
)

