# ============================================================
# File        : masking_manager.py
# Project     : Enterprise Data Governance Platform
# Purpose     : Manage masking requirements for sensitive data
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

CLASSIFICATION_TABLE = (
    f"{GOVERNANCE_CATALOG}."
    f"{GOVERNANCE_SCHEMA}."
    f"column_classification_registry"
)

MASKING_REGISTRY_TABLE = (
    f"{GOVERNANCE_CATALOG}."
    f"{GOVERNANCE_SCHEMA}."
    f"masking_policy_registry"
)

# ============================================================
# 3. CREATE MASKING REGISTRY
# ============================================================

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {MASKING_REGISTRY_TABLE}
(
    catalog_name STRING,
    schema_name STRING,
    table_name STRING,
    column_name STRING,

    classification STRING,
    masking_type STRING,

    policy_name STRING,

    target_roles STRING,

    masking_status STRING,

    implementation_type STRING,

    policy_expression STRING,

    created_at TIMESTAMP,
    updated_at TIMESTAMP

)
USING DELTA
""")

# ============================================================
# 4. MASKING TYPES
# ============================================================

MASK_FULL = "FULL"

MASK_PARTIAL = "PARTIAL"

MASK_EMAIL = "EMAIL"

MASK_PHONE = "PHONE"

MASK_LAST4 = "LAST4"

# ============================================================
# 5. DETERMINE MASKING TYPE
# ============================================================

def determine_masking_type(
    column_name: str,
    classification: str
) -> str:

    column = column_name.lower()

    if classification == "PCI":

        if (
            "last4" in column
            or "last_4" in column
        ):
            return MASK_LAST4

        return MASK_FULL

    if "email" in column:

        return MASK_EMAIL

    if (
        "phone" in column
        or "mobile" in column
    ):

        return MASK_PHONE

    if classification == "PII":

        return MASK_PARTIAL

    if classification == "FINANCIAL":

        return MASK_PARTIAL

    return MASK_FULL

# ============================================================
# 6. POLICY NAME
# ============================================================

def generate_policy_name(
    table_name: str,
    column_name: str
) -> str:

    clean_table = (
        table_name
        .lower()
        .replace("-", "_")
    )

    clean_column = (
        column_name
        .lower()
        .replace("-", "_")
    )

    return (
        f"mask_{clean_table}_{clean_column}"
    )

# ============================================================
# 7. TARGET ROLES
# ============================================================

def determine_target_roles(
    classification: str
) -> str:

    if classification == "PCI":

        return (
            "security_admin,data_owner"
        )

    if classification == "PII":

        return (
            "data_engineer,data_owner"
        )

    if classification == "FINANCIAL":

        return (
            "finance_manager,data_owner"
        )

    return "data_owner"

# ============================================================
# 8. MASKING EXPRESSION
# ============================================================

def generate_masking_expression(
    column_name: str,
    masking_type: str
) -> str:

    column = column_name

    if masking_type == MASK_EMAIL:

        return (
            f"CASE "
            f"WHEN is_account_group_member('data_owner') "
            f"THEN {column} "
            f"ELSE regexp_replace("
            f"{column}, "
            f"'(^.).*(@.*$)', "
            f"'$1***$2'"
            f") "
            f"END"
        )

    if masking_type == MASK_PHONE:

        return (
            f"CASE "
            f"WHEN is_account_group_member('data_owner') "
            f"THEN {column} "
            f"ELSE concat("
            f"'******', "
            f"right({column}, 4)"
            f") "
            f"END"
        )

    if masking_type == MASK_LAST4:

        return (
            f"CASE "
            f"WHEN is_account_group_member('security_admin') "
            f"THEN {column} "
            f"ELSE concat("
            f"'****', "
            f"right({column}, 4)"
            f") "
            f"END"
        )

    if masking_type == MASK_PARTIAL:

        return (
            f"CASE "
            f"WHEN is_account_group_member('data_owner') "
            f"THEN {column} "
            f"ELSE '********' "
            f"END"
        )

    return (
        f"CASE "
        f"WHEN is_account_group_member('data_owner') "
        f"THEN {column} "
        f"ELSE '********' "
        f"END"
    )

# ============================================================
# 9. BUILD MASKING SPECIFICATION
# ============================================================

def build_masking_spec(row) -> Dict:

    column_name = row["column_name"]

    classification = row["classification"]

    masking_type = determine_masking_type(
        column_name,
        classification
    )

    policy_name = generate_policy_name(
        row["table_name"],
        column_name
    )

    target_roles = determine_target_roles(
        classification
    )

    expression = generate_masking_expression(
        column_name,
        masking_type
    )

    return {

        "catalog_name":
            row["catalog_name"],

        "schema_name":
            row["schema_name"],

        "table_name":
            row["table_name"],

        "column_name":
            column_name,

        "classification":
            classification,

        "masking_type":
            masking_type,

        "policy_name":
            policy_name,

        "target_roles":
            target_roles,

        "masking_status":
            "REQUIRED",

        "implementation_type":
            "UNITY_CATALOG_POLICY",

        "policy_expression":
            expression
    }

# ============================================================
# 10. GET MASKING CANDIDATES
# ============================================================

def get_masking_candidates(
    catalog_name: str,
    schema_name: str,
    table_name: str
):

    df = (
        spark.table(
            CLASSIFICATION_TABLE
        )
        .filter(
            (F.col("catalog_name") == catalog_name)
            &
            (F.col("schema_name") == schema_name)
            &
            (F.col("table_name") == table_name)
            &
            (F.col("masking_required") == True)
        )
    )

    return df

# ============================================================
# 11. GENERATE MASKING POLICIES
# ============================================================

def generate_masking_policies(
    catalog_name: str,
    schema_name: str,
    table_name: str
):

    candidates = get_masking_candidates(
        catalog_name,
        schema_name,
        table_name
    )

    rows = candidates.collect()

    print(
        f"Found {len(rows)} "
        f"columns requiring masking."
    )

    specifications = []

    for row in rows:

        specification = (
            build_masking_spec(row)
        )

        specifications.append(
            specification
        )

    return specifications

# ============================================================
# 12. SAVE MASKING METADATA
# ============================================================

def save_masking_metadata(
    specifications: List[Dict]
):

    if not specifications:

        print(
            "No masking policies generated."
        )

        return

    df = spark.createDataFrame(
        specifications
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
            MASKING_REGISTRY_TABLE
        )
    )

    print(
        f"Saved {df.count()} "
        f"masking policy specifications."
    )

# ============================================================
# 13. DISPLAY POLICIES
# ============================================================

def display_policies(
    specifications: List[Dict]
):

    if not specifications:

        print(
            "No policies generated."
        )

        return

    for policy in specifications:

        print("=" * 70)

        print(
            f"Table       : "
            f"{policy['table_name']}"
        )

        print(
            f"Column      : "
            f"{policy['column_name']}"
        )

        print(
            f"Classification : "
            f"{policy['classification']}"
        )

        print(
            f"Mask Type   : "
            f"{policy['masking_type']}"
        )

        print(
            f"Policy      : "
            f"{policy['policy_name']}"
        )

        print(
            f"Roles       : "
            f"{policy['target_roles']}"
        )

        print(
            f"Status      : "
            f"{policy['masking_status']}"
        )

        print(
            f"Expression  : "
            f"{policy['policy_expression']}"
        )


# ============================================================
# 14. MAIN
# ============================================================

if __name__ == "__main__":

    catalog = "dev_catalog"

    schema = "silver"

    table = "customers"

    specifications = (
        generate_masking_policies(
            catalog_name=catalog,
            schema_name=schema,
            table_name=table
        )
    )

    display_policies(
        specifications
    )

    save_masking_metadata(
        specifications
    )

CREATE OR REPLACE VIEW
dev_catalog.silver.customers_secure
AS

SELECT

    customer_id,

    CASE
        WHEN is_account_group_member('data_owner')
        THEN email
        ELSE regexp_replace(
            email,
            '(^.).*(@.*$)',
            '$1***$2'
        )
    END AS email,

    CASE
        WHEN is_account_group_member('data_owner')
        THEN phone_number
        ELSE concat(
            '******',
            right(phone_number, 4)
        )
    END AS phone_number,

    city

FROM dev_catalog.silver.customers;
