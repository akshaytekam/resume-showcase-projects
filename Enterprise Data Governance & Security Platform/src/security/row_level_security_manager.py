# ============================================================
# File        : row_level_security_manager.py
# Project     : Enterprise Data Governance Platform
# Purpose     : Manage row-level security requirements
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

RLS_MAPPING_TABLE = (
    f"{GOVERNANCE_CATALOG}."
    f"{GOVERNANCE_SCHEMA}."
    f"row_level_access_mapping"
)

RLS_POLICY_REGISTRY = (
    f"{GOVERNANCE_CATALOG}."
    f"{GOVERNANCE_SCHEMA}."
    f"row_level_security_registry"
)

# ============================================================
# 3. CREATE RLS MAPPING TABLE
# ============================================================

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {RLS_MAPPING_TABLE}
(
    principal_type STRING,

    principal_name STRING,

    region STRING,

    country STRING,

    access_status STRING,

    approved_by STRING,

    effective_from DATE,

    effective_to DATE,

    created_at TIMESTAMP,

    updated_at TIMESTAMP

)
USING DELTA
""")

# ============================================================
# 4. CREATE RLS POLICY REGISTRY
# ============================================================

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {RLS_POLICY_REGISTRY}
(
    catalog_name STRING,

    schema_name STRING,

    table_name STRING,

    row_filter_column STRING,

    policy_name STRING,

    implementation_type STRING,

    mapping_table STRING,

    policy_status STRING,

    approval_required BOOLEAN,

    created_at TIMESTAMP,

    updated_at TIMESTAMP

)
USING DELTA
""")

# ============================================================
# 5. RLS DIMENSIONS
# ============================================================

RLS_REGION = "REGION"

RLS_COUNTRY = "COUNTRY"

RLS_DEPARTMENT = "DEPARTMENT"

RLS_BUSINESS_UNIT = "BUSINESS_UNIT"

# ============================================================
# 6. ADD REGION ACCESS
# ============================================================

def add_region_access(
    principal_name: str,
    region: str,
    country: str = "INDIA",
    approved_by: str = "data_owner"
):

    mapping = {

        "principal_type": "GROUP",

        "principal_name":
            principal_name,

        "region":
            region.upper(),

        "country":
            country.upper(),

        "access_status":
            "ACTIVE",

        "approved_by":
            approved_by
    }

    df = spark.createDataFrame(
        [mapping]
    )

    df = (
        df
        .withColumn(
            "effective_from",
            F.current_date()
        )
        .withColumn(
            "effective_to",
            F.lit(None).cast("date")
        )
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
        df.write
        .format("delta")
        .mode("append")
        .saveAsTable(
            RLS_MAPPING_TABLE
        )
    )

    print(
        f"Added region access: "
        f"{principal_name} -> {region}"
    )

# ============================================================
# 7. GENERATE POLICY NAME
# ============================================================

def generate_rls_policy_name(
    table_name: str,
    filter_column: str
) -> str:

    table_name = (
        table_name
        .lower()
        .replace("-", "_")
    )

    filter_column = (
        filter_column
        .lower()
        .replace("-", "_")
    )

    return (
        f"rls_{table_name}_{filter_column}"
    )

# ============================================================
# 8. GENERATE RLS EXPRESSION
# ============================================================

def generate_rls_expression(
    filter_column: str
) -> str:

    return f"""
EXISTS (
    SELECT 1
    FROM {RLS_MAPPING_TABLE} m
    WHERE
        lower(m.principal_name)
        IN (
            SELECT explode(
                transform(
                    current_user_groups(),
                    x -> lower(x)
                )
            )
        )

        AND (
            upper(m.region) = 'ALL'
            OR upper(m.region) =
               upper({filter_column})
        )

        AND m.access_status = 'ACTIVE'

        AND m.effective_from <= current_date()

        AND (
            m.effective_to IS NULL
            OR m.effective_to >= current_date()
        )
)
""".strip()

# ============================================================
# 9. REGISTER RLS POLICY
# ============================================================

def register_rls_policy(
    catalog_name: str,
    schema_name: str,
    table_name: str,
    filter_column: str
):

    policy_name = generate_rls_policy_name(
        table_name,
        filter_column
    )

    record = {

        "catalog_name":
            catalog_name,

        "schema_name":
            schema_name,

        "table_name":
            table_name,

        "row_filter_column":
            filter_column,

        "policy_name":
            policy_name,

        "implementation_type":
            "UNITY_CATALOG_RLS",

        "mapping_table":
            RLS_MAPPING_TABLE,

        "policy_status":
            "PROPOSED",

        "approval_required":
            True
    }

    df = spark.createDataFrame(
        [record]
    )

    df = (
        df
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
        df.write
        .format("delta")
        .mode("append")
        .saveAsTable(
            RLS_POLICY_REGISTRY
        )
    )

    print(
        f"Registered RLS policy: "
        f"{policy_name}"
    )

CREATE OR REPLACE VIEW
dev_catalog.gold.customer_360_governed
AS

SELECT
    c.customer_id,
    c.customer_name,
    c.email,
    c.phone_number,
    c.region,
    c.sales_amount

FROM dev_catalog.gold.customer_360 c

INNER JOIN
dev_catalog.governance.row_level_access_mapping m

ON upper(c.region) = upper(m.region)

WHERE
    m.principal_name =
        current_user();

# ============================================================
# 10. VALIDATE RLS REGISTRATION
# ============================================================

def validate_rls_registration(
    catalog_name: str,
    schema_name: str,
    table_name: str
) -> bool:

    df = (
        spark.table(
            RLS_POLICY_REGISTRY
        )
        .filter(
            (F.col("catalog_name") == catalog_name)
            &
            (F.col("schema_name") == schema_name)
            &
            (F.col("table_name") == table_name)
            &
            (F.col("policy_status") == "ACTIVE")
        )
    )

    exists = df.limit(1).count() > 0

    if exists:

        print(
            f"RLS validation PASSED: "
            f"{catalog_name}."
            f"{schema_name}."
            f"{table_name}"
        )

    else:

        print(
            f"RLS validation FAILED: "
            f"{catalog_name}."
            f"{schema_name}."
            f"{table_name}"
        )

    return exists

# ============================================================
# 11. COMPLIANCE CHECK
# ============================================================

def check_sensitive_table_rls(
    classification_df
):

    sensitive_tables = (
        classification_df
        .filter(
            F.col("classification")
            .isin(
                "PII",
                "PCI",
                "FINANCIAL"
            )
        )
        .select(
            "catalog_name",
            "schema_name",
            "table_name"
        )
        .distinct()
    )

    return sensitive_tables

# ============================================================
# 12. FIND MISSING RLS
# ============================================================

def find_tables_without_rls(
    sensitive_tables_df
):

    rls_df = (
        spark.table(
            RLS_POLICY_REGISTRY
        )
        .filter(
            F.col("policy_status") == "ACTIVE"
        )
        .select(
            "catalog_name",
            "schema_name",
            "table_name"
        )
        .distinct()
    )

    missing = (
        sensitive_tables_df
        .join(
            rls_df,
            on=[
                "catalog_name",
                "schema_name",
                "table_name"
            ],
            how="left_anti"
        )
    )

    return missing

# ============================================================
# 13. MAIN
# ============================================================

if __name__ == "__main__":

    register_rls_policy(
        catalog_name="dev_catalog",
        schema_name="gold",
        table_name="customer_360",
        filter_column="region"
    )

    register_rls_policy(
        catalog_name="dev_catalog",
        schema_name="gold",
        table_name="transactions",
        filter_column="region"
    )

    validate_rls_registration(
        catalog_name="dev_catalog",
        schema_name="gold",
        table_name="customer_360"
    )

