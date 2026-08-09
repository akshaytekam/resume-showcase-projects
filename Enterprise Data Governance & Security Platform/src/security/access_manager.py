# ============================================================
# File        : access_manager.py
# Project     : Enterprise Data Governance Platform
# Purpose     : Manage RBAC and access-control specifications
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

ACCESS_REGISTRY_TABLE = (
    f"{GOVERNANCE_CATALOG}."
    f"{GOVERNANCE_SCHEMA}."
    f"access_control_registry"
)

# ============================================================
# 3. CREATE ACCESS REGISTRY
# ============================================================

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {ACCESS_REGISTRY_TABLE}
(
    role_name STRING,

    principal_type STRING,

    principal_name STRING,

    catalog_name STRING,

    schema_name STRING,

    object_name STRING,

    object_type STRING,

    privilege STRING,

    access_level STRING,

    data_classification STRING,

    approval_required BOOLEAN,

    access_status STRING,

    environment STRING,

    created_at TIMESTAMP,

    updated_at TIMESTAMP

)
USING DELTA
""")

# ============================================================
# 4. ROLE DEFINITIONS
# ============================================================

ROLES = {

    "DATA_PLATFORM_ADMIN": {
        "description":
            "Platform and governance administrator"
    },

    "DATA_ENGINEER": {
        "description":
            "Builds and maintains data pipelines"
    },

    "DATA_ANALYST": {
        "description":
            "Analyzes governed business datasets"
    },

    "FINANCE_ANALYST": {
        "description":
            "Analyzes approved financial datasets"
    },

    "DATA_SCIENTIST": {
        "description":
            "Performs approved analytical workloads"
    },

    "BI_READER": {
        "description":
            "Consumes governed BI datasets"
    },

    "AUDITOR": {
        "description":
            "Reviews governance and audit metadata"
    }
}

# ============================================================
# 5. ACCESS LEVELS
# ============================================================

ACCESS_FULL = "FULL"

ACCESS_MASKED = "MASKED"

ACCESS_READ_ONLY = "READ_ONLY"

ACCESS_DENIED = "DENIED"

# ============================================================
# 6. ROLE ACCESS POLICIES
# ============================================================

ROLE_POLICIES = {

    "DATA_PLATFORM_ADMIN": {

        "bronze": {
            "privilege": "ALL",
            "access_level": ACCESS_FULL
        },

        "silver": {
            "privilege": "ALL",
            "access_level": ACCESS_FULL
        },

        "gold": {
            "privilege": "ALL",
            "access_level": ACCESS_FULL
        },

        "governance": {
            "privilege": "ALL",
            "access_level": ACCESS_FULL
        }
    },

    "DATA_ENGINEER": {

        "bronze": {
            "privilege": "SELECT,MODIFY",
            "access_level": ACCESS_FULL
        },

        "silver": {
            "privilege": "SELECT,MODIFY",
            "access_level": ACCESS_FULL
        },

        "gold": {
            "privilege": "SELECT",
            "access_level": ACCESS_READ_ONLY
        }
    },

    "DATA_ANALYST": {

        "bronze": {
            "privilege": "NONE",
            "access_level": ACCESS_DENIED
        },

        "silver": {
            "privilege": "SELECT",
            "access_level": ACCESS_MASKED
        },

        "gold": {
            "privilege": "SELECT",
            "access_level": ACCESS_MASKED
        }
    },

    "FINANCE_ANALYST": {

        "bronze": {
            "privilege": "NONE",
            "access_level": ACCESS_DENIED
        },

        "silver": {
            "privilege": "SELECT",
            "access_level": ACCESS_MASKED
        },

        "gold": {
            "privilege": "SELECT",
            "access_level": ACCESS_MASKED
        }
    },

    "DATA_SCIENTIST": {

        "bronze": {
            "privilege": "NONE",
            "access_level": ACCESS_DENIED
        },

        "silver": {
            "privilege": "SELECT",
            "access_level": ACCESS_MASKED
        },

        "gold": {
            "privilege": "SELECT",
            "access_level": ACCESS_MASKED
        }
    },

    "BI_READER": {

        "bronze": {
            "privilege": "NONE",
            "access_level": ACCESS_DENIED
        },

        "silver": {
            "privilege": "NONE",
            "access_level": ACCESS_DENIED
        },

        "gold": {
            "privilege": "SELECT",
            "access_level": ACCESS_READ_ONLY
        }
    },

    "AUDITOR": {

        "bronze": {
            "privilege": "NONE",
            "access_level": ACCESS_DENIED
        },

        "silver": {
            "privilege": "NONE",
            "access_level": ACCESS_DENIED
        },

        "gold": {
            "privilege": "NONE",
            "access_level": ACCESS_DENIED
        },

        "governance": {
            "privilege": "SELECT",
            "access_level": ACCESS_READ_ONLY
        }
    }
}

# ============================================================
# 7. BUILD ACCESS SPECIFICATION
# ============================================================

def build_access_specification(
    role_name: str,
    schema_name: str,
    privilege: str,
    access_level: str,
    object_name: str = "*",
    object_type: str = "SCHEMA",
    data_classification: str = "MIXED"
) -> Dict:

    return {

        "role_name": role_name,

        "principal_type": "GROUP",

        "principal_name":
            role_name.lower(),

        "catalog_name":
            GOVERNANCE_CATALOG,

        "schema_name":
            schema_name,

        "object_name":
            object_name,

        "object_type":
            object_type,

        "privilege":
            privilege,

        "access_level":
            access_level,

        "data_classification":
            data_classification,

        "approval_required": True,

        "access_status":
            "PROPOSED",

        "environment":
            "DEV"
    }

# ============================================================
# 8. GENERATE ROLE ACCESS
# ============================================================

def generate_role_access(
    role_name: str
) -> List[Dict]:

    if role_name not in ROLE_POLICIES:

        raise ValueError(
            f"Unknown role: {role_name}"
        )

    policies = ROLE_POLICIES[
        role_name
    ]

    results = []

    for schema_name, policy in policies.items():

        results.append(
            build_access_specification(

                role_name=role_name,

                schema_name=schema_name,

                privilege=policy["privilege"],

                access_level=
                    policy["access_level"]
            )
        )

    return results

# ============================================================
# 9. GENERATE ALL ROLE ACCESS
# ============================================================

def generate_all_access_policies():

    results = []

    for role_name in ROLE_POLICIES:

        role_results = (
            generate_role_access(
                role_name
            )
        )

        results.extend(
            role_results
        )

    return results

# ============================================================
# 10. SAVE ACCESS REGISTRY
# ============================================================

def save_access_policies(
    policies: List[Dict]
):

    if not policies:

        print(
            "No access policies generated."
        )

        return

    df = spark.createDataFrame(
        policies
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
            ACCESS_REGISTRY_TABLE
        )
    )

    print(
        f"Saved {df.count()} "
        f"access-control records."
    )

# ============================================================
# 11. DISPLAY ACCESS MODEL
# ============================================================

def display_access_model(
    policies: List[Dict]
):

    for policy in policies:

        print(
            "=" * 70
        )

        print(
            f"Role       : "
            f"{policy['role_name']}"
        )

        print(
            f"Schema     : "
            f"{policy['schema_name']}"
        )

        print(
            f"Privilege  : "
            f"{policy['privilege']}"
        )

        print(
            f"Access     : "
            f"{policy['access_level']}"
        )

        print(
            f"Status     : "
            f"{policy['access_status']}"
        )


# ============================================================
# 12. MAIN
# ============================================================

if __name__ == "__main__":

    policies = (
        generate_all_access_policies()
    )

    display_access_model(
        policies
    )

    save_access_policies(
        policies
    )

