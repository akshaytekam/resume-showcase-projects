# ============================================================
# File        : governance_pipeline.py
# Project     : Enterprise Data Governance Platform
# Purpose     : Orchestrate governance and security checks
# ============================================================

from datetime import datetime
from typing import Dict, List

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ============================================================
# 1. SPARK SESSION
# ============================================================

spark = (
    SparkSession
    .builder
    .appName(
        "EnterpriseDataGovernancePipeline"
    )
    .getOrCreate()
)

# ============================================================
# 2. CONFIGURATION
# ============================================================

CATALOG = "dev_catalog"

BRONZE_SCHEMA = "bronze"

SILVER_SCHEMA = "silver"

GOLD_SCHEMA = "gold"

GOVERNANCE_SCHEMA = "governance"

# ============================================================
# 3. GOVERNED DATASETS
# ============================================================

GOVERNED_TABLES = [

    {
        "catalog": CATALOG,
        "schema": GOLD_SCHEMA,
        "table": "customer_360"
    },

    {
        "catalog": CATALOG,
        "schema": GOLD_SCHEMA,
        "table": "transactions"
    },

    {
        "catalog": CATALOG,
        "schema": GOLD_SCHEMA,
        "table": "payments"
    }
]

# ============================================================
# 4. EXECUTION CONTEXT
# ============================================================

RUN_ID = (
    datetime.utcnow()
    .strftime("%Y%m%d%H%M%S")
)

PIPELINE_NAME = (
    "enterprise_data_governance"
)

START_TIME = datetime.utcnow()

# ============================================================
# 5. PIPELINE STATUS
# ============================================================

pipeline_status = {

    "run_id": RUN_ID,

    "pipeline_name":
        PIPELINE_NAME,

    "status":
        "STARTED",

    "start_time":
        START_TIME,

    "end_time":
        None,

    "datasets_processed":
        0,

    "datasets_failed":
        0
}

# ============================================================
# 6. LOGGING
# ============================================================

def log(message: str):

    timestamp = (
        datetime.utcnow()
        .strftime(
            "%Y-%m-%d %H:%M:%S"
        )
    )

    print(
        f"[{timestamp}] "
        f"[{PIPELINE_NAME}] "
        f"[RUN_ID={RUN_ID}] "
        f"{message}"
    )

# ============================================================
# 7. AUDIT TABLE
# ============================================================

AUDIT_TABLE = (
    f"{CATALOG}."
    f"{GOVERNANCE_SCHEMA}."
    f"governance_pipeline_audit"
)

# ============================================================
# 8. WRITE AUDIT RESULT
# ============================================================

def write_audit_result(
    dataset_name: str,
    check_name: str,
    check_category: str,
    check_status: str,
    severity: str,
    message: str,
    started_at,
    completed_at
):

    record = [{

        "run_id":
            RUN_ID,

        "pipeline_name":
            PIPELINE_NAME,

        "dataset_name":
            dataset_name,

        "check_name":
            check_name,

        "check_category":
            check_category,

        "check_status":
            check_status,

        "severity":
            severity,

        "message":
            message,

        "started_at":
            started_at,

        "completed_at":
            completed_at
    }]

    df = spark.createDataFrame(
        record
    )

    (
        df.write
        .format("delta")
        .mode("append")
        .saveAsTable(
            AUDIT_TABLE
        )
    )

# ============================================================
# 9. CHECK EXECUTION WRAPPER
# ============================================================

def execute_check(
    dataset_name: str,
    check_name: str,
    check_category: str,
    check_function
):

    check_start = datetime.utcnow()

    log(
        f"Running {check_name} "
        f"for {dataset_name}"
    )

    try:

        result = check_function()

        check_end = datetime.utcnow()

        if result:

            write_audit_result(

                dataset_name,

                check_name,

                check_category,

                "PASS",

                "INFO",

                (
                    f"{check_name} "
                    f"passed successfully."
                ),

                check_start,

                check_end
            )

            log(
                f"{check_name} "
                f"PASSED"
            )

            return True

        else:

            write_audit_result(

                dataset_name,

                check_name,

                check_category,

                "FAIL",

                "HIGH",

                (
                    f"{check_name} "
                    f"failed."
                ),

                check_start,

                check_end
            )

            log(
                f"{check_name} "
                f"FAILED"
            )

            return False

    except Exception as exc:

        check_end = datetime.utcnow()

        write_audit_result(

            dataset_name,

            check_name,

            check_category,

            "ERROR",

            "CRITICAL",

            str(exc),

            check_start,

            check_end
        )

        log(
            f"{check_name} "
            f"ERROR: {exc}"
        )

        return False


# ============================================================
# 10. IMPORT GOVERNANCE MODULES
# ============================================================

from governance.data_profiler import (
    profile_table
)

from governance.classification_engine import (
    classify_table
)

from governance.lineage_tracker import (
    register_dataset_lineage
)

from security.masking_manager import (
    generate_masking_policies,
    save_masking_metadata
)

from security.access_manager import (
    generate_all_access_policies,
    save_access_policies
)

from security.row_level_security_manager import (
    register_rls_policy,
    validate_rls_registration
)

# ============================================================
# 10. IMPORT GOVERNANCE MODULES
# ============================================================

from governance.data_profiler import (
    profile_table
)

from governance.classification_engine import (
    classify_table
)

from governance.lineage_tracker import (
    register_dataset_lineage
)

from security.masking_manager import (
    generate_masking_policies,
    save_masking_metadata
)

from security.access_manager import (
    generate_all_access_policies,
    save_access_policies
)

from security.row_level_security_manager import (
    register_rls_policy,
    validate_rls_registration
)

# ============================================================
# 12. PROCESS DATASET
# ============================================================

def process_dataset(
    dataset: Dict
):

    catalog = dataset["catalog"]

    schema = dataset["schema"]

    table = dataset["table"]

    full_name = (
        f"{catalog}."
        f"{schema}."
        f"{table}"
    )

    log(
        f"Starting governance processing "
        f"for {full_name}"
    )

    if not table_exists(
        catalog,
        schema,
        table
    ):

        log(
            f"Table not found: "
            f"{full_name}"
        )

        pipeline_status[
            "datasets_failed"
        ] += 1

        return False

      # --------------------------------------------------------
    # DATA PROFILING
    # --------------------------------------------------------

    profile_start = datetime.utcnow()

    try:

        profile_table(
            catalog=catalog,
            schema=schema,
            table=table
        )

        profile_end = datetime.utcnow()

        write_audit_result(

            full_name,

            "DATA_PROFILING",

            "GOVERNANCE",

            "PASS",

            "INFO",

            "Data profiling completed.",

            profile_start,

            profile_end
        )

        log(
            f"Profiling completed: "
            f"{full_name}"
        )

    except Exception as exc:

        profile_end = datetime.utcnow()

        write_audit_result(

            full_name,

            "DATA_PROFILING",

            "GOVERNANCE",

            "ERROR",

            "HIGH",

            str(exc),

            profile_start,

            profile_end
        )

        log(
            f"Profiling failed: "
            f"{exc}"
        )

        return False


    # --------------------------------------------------------
    # DATA CLASSIFICATION
    # --------------------------------------------------------

    classification_start = datetime.utcnow()

    try:

        classification_result = (
            classify_table(
                catalog=catalog,
                schema=schema,
                table=table
            )
        )

        classification_end = (
            datetime.utcnow()
        )

        write_audit_result(

            full_name,

            "DATA_CLASSIFICATION",

            "GOVERNANCE",

            "PASS",

            "INFO",

            "Classification completed.",

            classification_start,

            classification_end
        )

        log(
            f"Classification completed: "
            f"{full_name}"
        )

    except Exception as exc:

        classification_end = (
            datetime.utcnow()
        )

        write_audit_result(

            full_name,

            "DATA_CLASSIFICATION",

            "GOVERNANCE",

            "ERROR",

            "HIGH",

            str(exc),

            classification_start,

            classification_end
        )

        log(
            f"Classification failed: "
            f"{exc}"
        )

        return False

      # --------------------------------------------------------
    # LINEAGE
    # --------------------------------------------------------

    lineage_start = datetime.utcnow()

    try:

        if schema == GOLD_SCHEMA:

            register_dataset_lineage(

                source_catalog=catalog,

                source_schema=SILVER_SCHEMA,

                source_table=table,

                target_catalog=catalog,

                target_schema=GOLD_SCHEMA,

                target_table=table,

                pipeline_name=PIPELINE_NAME,

                transformation_type=
                    "GOVERNED_TRANSFORMATION",

                transformation_logic=
                    "Silver dataset transformed "
                    "into governed Gold dataset."
            )

        lineage_end = datetime.utcnow()

        write_audit_result(

            full_name,

            "LINEAGE_REGISTRATION",

            "GOVERNANCE",

            "PASS",

            "INFO",

            "Lineage registration completed.",

            lineage_start,

            lineage_end
        )

    except Exception as exc:

        lineage_end = datetime.utcnow()

        write_audit_result(

            full_name,

            "LINEAGE_REGISTRATION",

            "GOVERNANCE",

            "ERROR",

            "MEDIUM",

            str(exc),

            lineage_start,

            lineage_end
        )

        log(
            f"Lineage registration failed: "
            f"{exc}"
        )


    # --------------------------------------------------------
    # MASKING
    # --------------------------------------------------------

    masking_start = datetime.utcnow()

    try:

        masking_specs = (
            generate_masking_policies(
                catalog_name=catalog,
                schema_name=schema,
                table_name=table
            )
        )

        if masking_specs:

            save_masking_metadata(
                masking_specs
            )

        masking_end = datetime.utcnow()

        write_audit_result(

            full_name,

            "MASKING_POLICY_CHECK",

            "SECURITY",

            "PASS",

            "INFO",

            (
                f"Generated "
                f"{len(masking_specs)} "
                f"masking policies."
            ),

            masking_start,

            masking_end
        )

    except Exception as exc:

        masking_end = datetime.utcnow()

        write_audit_result(

            full_name,

            "MASKING_POLICY_CHECK",

            "SECURITY",

            "ERROR",

            "HIGH",

            str(exc),

            masking_start,

            masking_end
        )

        log(
            f"Masking processing failed: "
            f"{exc}"
        )

        return False


    # --------------------------------------------------------
    # ROW LEVEL SECURITY
    # --------------------------------------------------------

    rls_start = datetime.utcnow()

    try:

        register_rls_policy(

            catalog_name=catalog,

            schema_name=schema,

            table_name=table,

            filter_column="region"
        )

        rls_end = datetime.utcnow()

        write_audit_result(

            full_name,

            "RLS_POLICY_REGISTRATION",

            "SECURITY",

            "PASS",

            "INFO",

            "RLS policy registered.",

            rls_start,

            rls_end
        )

    except Exception as exc:

        rls_end = datetime.utcnow()

        write_audit_result(

            full_name,

            "RLS_POLICY_REGISTRATION",

            "SECURITY",

            "ERROR",

            "HIGH",

            str(exc),

            rls_start,

            rls_end
        )

        log(
            f"RLS registration failed: "
            f"{exc}"
        )

        return False


    # --------------------------------------------------------
    # RLS VALIDATION
    # --------------------------------------------------------

    rls_validation = (
        validate_rls_registration(
            catalog_name=catalog,
            schema_name=schema,
            table_name=table
        )
    )

    if not rls_validation:

        log(
            f"RLS validation failed "
            f"for {full_name}"
        )

        pipeline_status[
            "datasets_failed"
        ] += 1

        return False


# ============================================================
# 13. GENERATE RBAC POLICIES
# ============================================================

def process_rbac():

    log(
        "Starting RBAC policy generation."
    )

    try:

        policies = (
            generate_all_access_policies()
        )

        save_access_policies(
            policies
        )

        log(
            f"Generated {len(policies)} "
            f"RBAC policies."
        )

        return True

    except Exception as exc:

        log(
            f"RBAC generation failed: "
            f"{exc}"
        )

        return False


# ============================================================
# 14. MAIN PIPELINE
# ============================================================

def run_pipeline():

    log(
        "================================================"
    )

    log(
        "ENTERPRISE DATA GOVERNANCE PIPELINE STARTED"
    )

    log(
        "================================================"
    )

    rbac_success = process_rbac()

    if not rbac_success:

        log(
            "RBAC processing failed."
        )

    for dataset in GOVERNED_TABLES:

        process_dataset(
            dataset
        )

    pipeline_status[
        "end_time"
    ] = datetime.utcnow()

    if pipeline_status[
        "datasets_failed"
    ] > 0:

        pipeline_status[
            "status"
        ] = "FAILED"

    else:

        pipeline_status[
            "status"
        ] = "SUCCESS"

    log(
        "================================================"
    )

    log(
        f"Pipeline status: "
        f"{pipeline_status['status']}"
    )

    log(
        f"Datasets processed: "
        f"{pipeline_status['datasets_processed']}"
    )

    log(
        f"Datasets failed: "
        f"{pipeline_status['datasets_failed']}"
    )

    log(
        "================================================"
    )

    return pipeline_status

# ============================================================
# 15. ENTRY POINT
# ============================================================

if __name__ == "__main__":

    result = run_pipeline()

    print(
        result
    )

