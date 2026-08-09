# ============================================================
# File        : governance_validator.py
# Project     : Enterprise Data Governance Platform
# Purpose     : Validate governance controls
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

COMPLIANCE_TABLE = (
    f"{GOVERNANCE_CATALOG}."
    f"{GOVERNANCE_SCHEMA}."
    f"governance_compliance_results"
)

# ============================================================
# 3. CREATE COMPLIANCE TABLE
# ============================================================

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {COMPLIANCE_TABLE}
(
    check_name STRING,
    check_category STRING,

    catalog_name STRING,
    schema_name STRING,
    table_name STRING,
    column_name STRING,

    severity STRING,
    status STRING,

    issue_description STRING,
    remediation_action STRING,

    executed_at TIMESTAMP

)
USING DELTA
""")

# ============================================================
# 4. RESULT BUILDER
# ============================================================

def build_result(
    check_name: str,
    check_category: str,
    row,
    severity: str,
    status: str,
    issue_description: str,
    remediation_action: str
) -> Dict:

    return {
        "check_name": check_name,
        "check_category": check_category,

        "catalog_name": row["catalog_name"],
        "schema_name": row["schema_name"],
        "table_name": row["table_name"],
        "column_name": row["column_name"],

        "severity": severity,
        "status": status,

        "issue_description":
            issue_description,

        "remediation_action":
            remediation_action
    }

# ============================================================
# 5. PII MASKING VALIDATION
# ============================================================

def validate_pii_masking(row) -> Dict:

    if not row["contains_pii"]:

        return build_result(
            check_name="PII_MASKING_REQUIRED",
            check_category="SECURITY",
            row=row,
            severity="HIGH",
            status="PASS",
            issue_description=(
                "Column does not contain PII."
            ),
            remediation_action=(
                "No action required."
            )
        )

    if row["masking_required"]:

        return build_result(
            check_name="PII_MASKING_REQUIRED",
            check_category="SECURITY",
            row=row,
            severity="HIGH",
            status="PASS",
            issue_description=(
                "PII column requires masking."
            ),
            remediation_action=(
                "Verify masking policy is applied."
            )
        )

    return build_result(
        check_name="PII_MASKING_REQUIRED",
        check_category="SECURITY",
        row=row,
        severity="CRITICAL",
        status="FAIL",
        issue_description=(
            "PII column does not have "
            "masking requirement enabled."
        ),
        remediation_action=(
            "Enable approved masking policy."
        )
    )

# ============================================================
# 6. ENCRYPTION VALIDATION
# ============================================================

def validate_encryption(row) -> Dict:

    sensitive_data = (
        row["contains_pii"]
        or row["contains_financial_data"]
        or row["contains_pci_data"]
    )

    if not sensitive_data:

        return build_result(
            check_name="ENCRYPTION_REQUIRED",
            check_category="SECURITY",
            row=row,
            severity="HIGH",
            status="PASS",
            issue_description=(
                "Column is not classified as "
                "sensitive data."
            ),
            remediation_action=(
                "No action required."
            )
        )

    if row["encryption_required"]:

        return build_result(
            check_name="ENCRYPTION_REQUIRED",
            check_category="SECURITY",
            row=row,
            severity="HIGH",
            status="PASS",
            issue_description=(
                "Encryption requirement is defined."
            ),
            remediation_action=(
                "Verify physical encryption controls."
            )
        )

    return build_result(
        check_name="ENCRYPTION_REQUIRED",
        check_category="SECURITY",
        row=row,
        severity="CRITICAL",
        status="FAIL",
        issue_description=(
            "Sensitive column does not have "
            "encryption requirement."
        ),
        remediation_action=(
            "Enable approved encryption controls."
        )
    )

# ============================================================
# 7. PCI SECURITY VALIDATION
# ============================================================

def validate_pci_security(row) -> Dict:

    if not row["contains_pci_data"]:

        return build_result(
            check_name="PCI_SECURITY_CONTROLS",
            check_category="SECURITY",
            row=row,
            severity="CRITICAL",
            status="PASS",
            issue_description=(
                "Column does not contain PCI data."
            ),
            remediation_action=(
                "No action required."
            )
        )

    controls_present = (
        row["sensitivity_level"] == "RESTRICTED"
        and row["masking_required"]
        and row["encryption_required"]
    )

    if controls_present:

        return build_result(
            check_name="PCI_SECURITY_CONTROLS",
            check_category="SECURITY",
            row=row,
            severity="CRITICAL",
            status="PASS",
            issue_description=(
                "PCI security controls are configured "
                "in governance metadata."
            ),
            remediation_action=(
                "Verify physical policy implementation."
            )
        )

    return build_result(
        check_name="PCI_SECURITY_CONTROLS",
        check_category="SECURITY",
        row=row,
        severity="CRITICAL",
        status="FAIL",
        issue_description=(
            "PCI column is missing one or more "
            "required security controls."
        ),
        remediation_action=(
            "Apply restricted access, masking, "
            "and encryption controls."
        )
    )

# ============================================================
# 8. CLASSIFICATION CONFIDENCE CHECK
# ============================================================

def validate_classification_confidence(row) -> Dict:

    confidence = row["confidence_score"]

    if confidence >= 0.95:

        status = "PASS"
        severity = "LOW"

        issue = (
            "Classification confidence is high."
        )

        remediation = (
            "No action required."
        )

    elif confidence >= 0.80:

        status = "REVIEW"
        severity = "MEDIUM"

        issue = (
            "Classification requires "
            "data steward review."
        )

        remediation = (
            "Review classification and approve "
            "or correct the assigned classification."
        )

    else:

        status = "FAIL"
        severity = "HIGH"

        issue = (
            "Classification confidence is low."
        )

        remediation = (
            "Manually classify the column."
        )

    return build_result(
        check_name="CLASSIFICATION_CONFIDENCE",
        check_category="GOVERNANCE",
        row=row,
        severity=severity,
        status=status,
        issue_description=issue,
        remediation_action=remediation
    )

# ============================================================
# 9. SENSITIVITY VALIDATION
# ============================================================

def validate_sensitivity(row) -> Dict:

    classification = row["classification"]

    sensitivity = row["sensitivity_level"]

    valid = True

    if classification == "PCI":

        valid = sensitivity == "RESTRICTED"

    elif classification == "FINANCIAL":

        valid = sensitivity == "RESTRICTED"

    elif classification == "PII":

        valid = sensitivity in (
            "CONFIDENTIAL",
            "RESTRICTED"
        )

    if valid:

        return build_result(
            check_name="SENSITIVITY_CLASSIFICATION",
            check_category="GOVERNANCE",
            row=row,
            severity="HIGH",
            status="PASS",
            issue_description=(
                "Sensitivity level is consistent "
                "with classification."
            ),
            remediation_action=(
                "No action required."
            )
        )

    return build_result(
        check_name="SENSITIVITY_CLASSIFICATION",
        check_category="GOVERNANCE",
        row=row,
        severity="CRITICAL",
        status="FAIL",
        issue_description=(
            "Sensitivity level does not match "
            "data classification."
        ),
        remediation_action=(
            "Correct the sensitivity classification."
        )
    )

# ============================================================
# 10. VALIDATE COLUMN
# ============================================================

def validate_column(row) -> List[Dict]:

    results = []

    results.append(
        validate_pii_masking(row)
    )

    results.append(
        validate_encryption(row)
    )

    results.append(
        validate_pci_security(row)
    )

    results.append(
        validate_classification_confidence(row)
    )

    results.append(
        validate_sensitivity(row)
    )

    return results

# ============================================================
# 11. VALIDATE DATASET
# ============================================================

def validate_dataset(
    catalog_name: str,
    schema_name: str,
    table_name: str
):

    classification_df = (
        spark.table(
            CLASSIFICATION_TABLE
        )
        .filter(
            (F.col("catalog_name") == catalog_name)
            &
            (F.col("schema_name") == schema_name)
            &
            (F.col("table_name") == table_name)
        )
    )

    rows = classification_df.collect()

    print(
        f"Validating {len(rows)} "
        f"classified columns."
    )

    all_results = []

    for row in rows:

        column_results = validate_column(row)

        all_results.extend(
            column_results
        )

    return all_results

# ============================================================
# 12. SAVE RESULTS
# ============================================================

def save_results(
    results: List[Dict]
):

    if not results:

        print(
            "No governance validation results."
        )

        return

    result_df = (
        spark.createDataFrame(
            results
        )
    )

    result_df = (
        result_df
        .withColumn(
            "executed_at",
            F.current_timestamp()
        )
    )

    (
        result_df
        .write
        .format("delta")
        .mode("append")
        .saveAsTable(
            COMPLIANCE_TABLE
        )
    )

    print(
        f"Saved {result_df.count()} "
        f"governance validation results."
    )

# ============================================================
# 13. COMPLIANCE SUMMARY
# ============================================================

def generate_summary():

    summary_df = spark.sql(f"""
        SELECT
            check_category,

            COUNT(*) AS total_checks,

            SUM(
                CASE
                    WHEN status = 'PASS'
                    THEN 1
                    ELSE 0
                END
            ) AS passed_checks,

            SUM(
                CASE
                    WHEN status = 'FAIL'
                    THEN 1
                    ELSE 0
                END
            ) AS failed_checks,

            SUM(
                CASE
                    WHEN status = 'REVIEW'
                    THEN 1
                    ELSE 0
                END
            ) AS review_required

        FROM {COMPLIANCE_TABLE}

        GROUP BY check_category

        ORDER BY check_category
    """)

    return summary_df

# ============================================================
# 14. MAIN
# ============================================================

if __name__ == "__main__":

    catalog = "dev_catalog"

    schema = "silver"

    table = "customers"

    results = validate_dataset(
        catalog_name=catalog,
        schema_name=schema,
        table_name=table
    )

    save_results(results)

    summary = generate_summary()

    summary.show(
        truncate=False
    )

