# ============================================================
# File        : classification_engine.py
# Project     : Enterprise Data Governance Platform
# Purpose     : Automatically classify data columns
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from typing import Dict, List

# ============================================================
# 1. SPARK SESSION
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

CLASSIFICATION_TABLE = (
    f"{GOVERNANCE_CATALOG}."
    f"{GOVERNANCE_SCHEMA}."
    f"column_classification_registry"
)

# ============================================================
# 3. CLASSIFICATION LEVELS
# ============================================================

CLASSIFICATION_INTERNAL = "INTERNAL"

CLASSIFICATION_CONFIDENTIAL = "CONFIDENTIAL"

CLASSIFICATION_RESTRICTED = "RESTRICTED"

CLASSIFICATION_PII = "PII"

CLASSIFICATION_PCI = "PCI"

CLASSIFICATION_FINANCIAL = "FINANCIAL"

# ============================================================
# 4. CLASSIFICATION REGISTRY
# ============================================================

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CLASSIFICATION_TABLE}
(
    catalog_name STRING,
    schema_name STRING,
    table_name STRING,
    column_name STRING,

    classification STRING,
    sensitivity_level STRING,

    contains_pii BOOLEAN,
    contains_financial_data BOOLEAN,
    contains_pci_data BOOLEAN,

    masking_required BOOLEAN,
    encryption_required BOOLEAN,

    classification_reason STRING,

    confidence_score DOUBLE,

    classification_source STRING,

    classified_at TIMESTAMP

)
USING DELTA
""")

# ============================================================
# 5. CLASSIFICATION RULES
# ============================================================

CLASSIFICATION_RULES = {

    "email": {
        "classification": CLASSIFICATION_PII,
        "sensitivity": "CONFIDENTIAL",
        "masking_required": True,
        "encryption_required": True,
        "confidence": 0.98
    },

    "phone": {
        "classification": CLASSIFICATION_PII,
        "sensitivity": "CONFIDENTIAL",
        "masking_required": True,
        "encryption_required": True,
        "confidence": 0.95
    },

    "date_of_birth": {
        "classification": CLASSIFICATION_PII,
        "sensitivity": "CONFIDENTIAL",
        "masking_required": True,
        "encryption_required": True,
        "confidence": 0.97
    },

    "salary": {
        "classification": CLASSIFICATION_FINANCIAL,
        "sensitivity": "RESTRICTED",
        "masking_required": True,
        "encryption_required": True,
        "confidence": 0.95
    },

    "card": {
        "classification": CLASSIFICATION_PCI,
        "sensitivity": "RESTRICTED",
        "masking_required": True,
        "encryption_required": True,
        "confidence": 0.99
    }
}

# ============================================================
# 6. CLASSIFY COLUMN
# ============================================================

def classify_column(row) -> Dict:

    column_name = row["column_name"].lower()

    email_pattern = row["contains_email_pattern"]

    phone_pattern = row["contains_phone_pattern"]

    classification = CLASSIFICATION_INTERNAL

    sensitivity = "INTERNAL"

    contains_pii = False
    contains_financial_data = False
    contains_pci_data = False

    masking_required = False
    encryption_required = False

    confidence_score = 0.60

    reason = "No sensitive-data indicators detected."

    # ========================================================
    # EMAIL / PII
    # ========================================================

    if (
        "email" in column_name
        or email_pattern
    ):

        classification = CLASSIFICATION_PII

        sensitivity = "CONFIDENTIAL"

        contains_pii = True

        masking_required = True

        encryption_required = True

        confidence_score = 0.98

        reason = (
            "Email column name or email pattern detected."
        )


    # ========================================================
    # PHONE / PII
    # ========================================================

    elif (
        "phone" in column_name
        or "mobile" in column_name
        or phone_pattern
    ):

        classification = CLASSIFICATION_PII

        sensitivity = "CONFIDENTIAL"

        contains_pii = True

        masking_required = True

        encryption_required = True

        confidence_score = 0.95

        reason = (
            "Phone column name or phone pattern detected."
        )


    # ========================================================
    # DATE OF BIRTH / PII
    # ========================================================

    elif (
        "date_of_birth" in column_name
        or "dob" in column_name
        or "birth_date" in column_name
    ):

        classification = CLASSIFICATION_PII

        sensitivity = "CONFIDENTIAL"

        contains_pii = True

        masking_required = True

        encryption_required = True

        confidence_score = 0.97

        reason = (
            "Date-of-birth column detected."
        )


    # ========================================================
    # FINANCIAL DATA
    # ========================================================

    elif (
        "salary" in column_name
        or "income" in column_name
        or "transaction_amount" in column_name
        or "payment_amount" in column_name
        or "account_balance" in column_name
    ):

        classification = CLASSIFICATION_FINANCIAL

        sensitivity = "RESTRICTED"

        contains_financial_data = True

        masking_required = True

        encryption_required = True

        confidence_score = 0.95

        reason = (
            "Financial data column detected."
        )


    # ========================================================
    # PCI DATA
    # ========================================================

    elif (
        "card_number" in column_name
        or "credit_card" in column_name
        or "debit_card" in column_name
        or "pan" == column_name
        or "cvv" in column_name
    ):

        classification = CLASSIFICATION_PCI

        sensitivity = "RESTRICTED"

        contains_pci_data = True

        masking_required = True

        encryption_required = True

        confidence_score = 0.99

        reason = (
            "Payment card information detected."
        )


    return {
        "catalog_name": row["catalog_name"],
        "schema_name": row["schema_name"],
        "table_name": row["table_name"],
        "column_name": row["column_name"],

        "classification": classification,

        "sensitivity_level": sensitivity,

        "contains_pii": contains_pii,

        "contains_financial_data":
            contains_financial_data,

        "contains_pci_data":
            contains_pci_data,

        "masking_required":
            masking_required,

        "encryption_required":
            encryption_required,

        "classification_reason":
            reason,

        "confidence_score":
            confidence_score,

        "classification_source":
            "AUTOMATED"
    }

# ============================================================
# 7. PROCESS PROFILE RESULTS
# ============================================================

def classify_dataset(
    catalog_name: str,
    schema_name: str,
    table_name: str
):

    profile_df = (
        spark.table(PROFILE_TABLE)
        .filter(
            (F.col("catalog_name") == catalog_name)
            &
            (F.col("schema_name") == schema_name)
            &
            (F.col("table_name") == table_name)
        )
    )

    profile_rows = profile_df.collect()

    print(
        f"Found {len(profile_rows)} "
        f"columns for classification."
    )

    classifications = []

    for row in profile_rows:

        classification = classify_column(row)

        classifications.append(
            classification
        )

    return classifications

# ============================================================
# 8. SAVE CLASSIFICATION RESULTS
# ============================================================

def save_classifications(
    classifications: List[Dict]
):

    if not classifications:

        print(
            "No classifications generated."
        )

        return

    classification_df = (
        spark.createDataFrame(
            classifications
        )
    )

    classification_df = (
        classification_df
        .withColumn(
            "classified_at",
            F.current_timestamp()
        )
    )

    (
        classification_df
        .write
        .format("delta")
        .mode("append")
        .saveAsTable(
            CLASSIFICATION_TABLE
        )
    )

    print(
        f"Saved {classification_df.count()} "
        f"classification results."
    )

# ============================================================
# 9. MAIN
# ============================================================

if __name__ == "__main__":

    catalog = "dev_catalog"

    schema = "silver"

    table = "customers"

    classifications = classify_dataset(
        catalog_name=catalog,
        schema_name=schema,
        table_name=table
    )

    save_classifications(
        classifications
    )

