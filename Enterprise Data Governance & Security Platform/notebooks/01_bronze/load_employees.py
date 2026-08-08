# ============================================================
# File        : load_employees.py
# Project     : Enterprise Data Governance Platform
# Purpose     : Ingest employee master data into Bronze Delta
# ============================================================

from datetime import datetime
import uuid

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_date,
    current_timestamp,
    input_file_name,
    lit,
    trim,
    upper
)

from schemas.employee_schema import employee_schema


# ============================================================
# Configuration
# ============================================================

SOURCE_PATH = (
    "s3://enterprise-data-platform/raw/employees/"
)

TARGET_TABLE = (
    "dev_catalog.bronze.employees"
)

QUARANTINE_TABLE = (
    "dev_catalog.bronze.employees_quarantine"
)

AUDIT_TABLE = (
    "dev_catalog.governance.ingestion_audit"
)

PIPELINE_NAME = "employee_ingestion"

BATCH_ID = str(uuid.uuid4())


# ============================================================
# Spark Session
# ============================================================

spark = (
    SparkSession.builder
    .appName("EmployeeDataIngestion")
    .getOrCreate()
)


pipeline_start_time = datetime.now()


try:

    print(
        "Starting employee ingestion pipeline"
    )

    # ========================================================
    # STEP 1 — Read Employee Source
    # ========================================================

    employees_df = (
        spark.read
        .option("header", "true")
        .schema(employee_schema)
        .csv(SOURCE_PATH)
    )

    source_count = employees_df.count()

    print(
        f"Source employee records: {source_count}"
    )


    # ========================================================
    # STEP 2 — Add Technical Metadata
    # ========================================================

    employees_df = (
        employees_df
        .withColumn(
            "source_file",
            input_file_name()
        )
        .withColumn(
            "ingestion_timestamp",
            current_timestamp()
        )
        .withColumn(
            "batch_id",
            lit(BATCH_ID)
        )
    )


    # ========================================================
    # STEP 3 — Standardize String Values
    # ========================================================

    employees_df = (
        employees_df

        .withColumn(
            "employee_id",
            trim(col("employee_id"))
        )

        .withColumn(
            "first_name",
            trim(col("first_name"))
        )

        .withColumn(
            "last_name",
            trim(col("last_name"))
        )

        .withColumn(
            "email",
            lower(trim(col("email")))
        )

        .withColumn(
            "department",
            upper(trim(col("department")))
        )

        .withColumn(
            "job_title",
            trim(col("job_title"))
        )

        .withColumn(
            "employment_status",
            upper(trim(col("employment_status")))
        )

        .withColumn(
            "employment_type",
            upper(trim(col("employment_type")))
        )

        .withColumn(
            "manager_id",
            trim(col("manager_id"))
        )
    )


    # ========================================================
    # STEP 4 — Required Field Validation
    # ========================================================

    invalid_required_condition = (
        col("employee_id").isNull()
        | (trim(col("employee_id")) == "")

        | col("first_name").isNull()
        | (trim(col("first_name")) == "")

        | col("last_name").isNull()
        | (trim(col("last_name")) == "")

        | col("email").isNull()
        | (trim(col("email")) == "")

        | col("department").isNull()

        | col("job_title").isNull()

        | col("employment_status").isNull()

        | col("employment_type").isNull()

        | col("joining_date").isNull()
    )


    invalid_required_df = (
        employees_df
        .filter(invalid_required_condition)
        .withColumn(
            "quarantine_reason",
            lit("REQUIRED_FIELD_MISSING")
        )
    )


    valid_required_df = (
        employees_df
        .filter(~invalid_required_condition)
    )


    # ========================================================
    # STEP 5 — Email Format Validation
    # ========================================================

    email_pattern = (
        "^[A-Za-z0-9._%+-]+"
        "@[A-Za-z0-9.-]+"
        "\\.[A-Za-z]{2,}$"
    )


    invalid_email_df = (
        valid_required_df
        .filter(
            ~col("email").rlike(
                email_pattern
            )
        )
        .withColumn(
            "quarantine_reason",
            lit("INVALID_EMAIL_FORMAT")
        )
    )


    valid_email_df = (
        valid_required_df
        .filter(
            col("email").rlike(
                email_pattern
            )
        )
    )


    # ========================================================
    # STEP 6 — Employee ID Validation
    # ========================================================

    invalid_employee_id_df = (
        valid_email_df
        .filter(
            ~col("employee_id").rlike(
                "^EMP[0-9]{6}$"
            )
        )
        .withColumn(
            "quarantine_reason",
            lit("INVALID_EMPLOYEE_ID")
        )
    )


    valid_employee_id_df = (
        valid_email_df
        .filter(
            col("employee_id").rlike(
                "^EMP[0-9]{6}$"
            )
        )
    )


    # ========================================================
    # STEP 7 — Department Validation
    # ========================================================

    valid_departments = [
        "ENGINEERING",
        "DATA",
        "FINANCE",
        "HR",
        "SALES",
        "MARKETING",
        "OPERATIONS",
        "SECURITY",
        "LEGAL",
        "PROCUREMENT"
    ]


    invalid_department_df = (
        valid_employee_id_df
        .filter(
            ~col("department").isin(
                valid_departments
            )
        )
        .withColumn(
            "quarantine_reason",
            lit("INVALID_DEPARTMENT")
        )
    )


    valid_department_df = (
        valid_employee_id_df
        .filter(
            col("department").isin(
                valid_departments
            )
        )
    )


    # ========================================================
    # STEP 8 — Employment Status Validation
    # ========================================================

    valid_statuses = [
        "ACTIVE",
        "INACTIVE",
        "ON_LEAVE",
        "TERMINATED"
    ]


    invalid_status_df = (
        valid_department_df
        .filter(
            ~col("employment_status").isin(
                valid_statuses
            )
        )
        .withColumn(
            "quarantine_reason",
            lit("INVALID_EMPLOYMENT_STATUS")
        )
    )


    valid_status_df = (
        valid_department_df
        .filter(
            col("employment_status").isin(
                valid_statuses
            )
        )
    )


    # ========================================================
    # STEP 9 — Employment Type Validation
    # ========================================================

    valid_employment_types = [
        "FULL_TIME",
        "PART_TIME",
        "CONTRACT",
        "TEMPORARY",
        "INTERN"
    ]


    invalid_employment_type_df = (
        valid_status_df
        .filter(
            ~col("employment_type").isin(
                valid_employment_types
            )
        )
        .withColumn(
            "quarantine_reason",
            lit("INVALID_EMPLOYMENT_TYPE")
        )
    )


    valid_employment_type_df = (
        valid_status_df
        .filter(
            col("employment_type").isin(
                valid_employment_types
            )
        )
    )


    # ========================================================
    # STEP 10 — Salary Validation
    # ========================================================

    invalid_salary_df = (
        valid_employment_type_df
        .filter(
            col("annual_salary") < 0
        )
        .withColumn(
            "quarantine_reason",
            lit("INVALID_ANNUAL_SALARY")
        )
    )


    valid_salary_df = (
        valid_employment_type_df
        .filter(
            col("annual_salary") >= 0
        )
    )


    # ========================================================
    # STEP 11 — Joining Date Validation
    # ========================================================

    invalid_joining_date_df = (
        valid_salary_df
        .filter(
            col("joining_date")
            > current_date()
        )
        .withColumn(
            "quarantine_reason",
            lit("FUTURE_JOINING_DATE")
        )
    )


    valid_joining_date_df = (
        valid_salary_df
        .filter(
            col("joining_date").isNull()
            |
            (
                col("joining_date")
                <= current_date()
            )
        )
    )


    # ========================================================
    # STEP 12 — Duplicate Employee Validation
    # ========================================================

    duplicate_employee_ids = (
        valid_joining_date_df
        .groupBy("employee_id")
        .count()
        .filter(
            col("count") > 1
        )
        .select("employee_id")
    )


    duplicate_employees_df = (
        valid_joining_date_df
        .join(
            duplicate_employee_ids,
            on="employee_id",
            how="inner"
        )
        .withColumn(
            "quarantine_reason",
            lit("DUPLICATE_EMPLOYEE_ID")
        )
    )


    valid_employees_df = (
        valid_joining_date_df
        .dropDuplicates(
            ["employee_id"]
        )
    )


    # ========================================================
    # STEP 13 — Manager Referential Integrity
    # ========================================================

    employee_reference_df = (
        valid_employees_df
        .select(
            col("employee_id")
        )
        .dropDuplicates()
    )


    invalid_manager_df = (
        valid_employees_df
        .filter(
            col("manager_id").isNotNull()
        )
        .join(
            employee_reference_df,
            valid_employees_df.manager_id
            ==
            employee_reference_df.employee_id,
            "left_anti"
        )
        .withColumn(
            "quarantine_reason",
            lit("MANAGER_NOT_FOUND")
        )
    )


    valid_manager_df = (
        valid_employees_df
        .filter(
            col("manager_id").isNull()
        )
        .unionByName(
            valid_employees_df
            .filter(
                col("manager_id").isNotNull()
            )
            .join(
                employee_reference_df,
                valid_employees_df.manager_id
                ==
                employee_reference_df.employee_id,
                "inner"
            )
            .select(
                valid_employees_df["*"]
            )
        )
    )


    # ========================================================
    # STEP 14 — Quarantine Counts
    # ========================================================

    invalid_required_count = (
        invalid_required_df.count()
    )

    invalid_email_count = (
        invalid_email_df.count()
    )

    invalid_employee_id_count = (
        invalid_employee_id_df.count()
    )

    invalid_department_count = (
        invalid_department_df.count()
    )

    invalid_status_count = (
        invalid_status_df.count()
    )

    invalid_employment_type_count = (
        invalid_employment_type_df.count()
    )

    invalid_salary_count = (
        invalid_salary_df.count()
    )

    invalid_joining_date_count = (
        invalid_joining_date_df.count()
    )

    duplicate_count = (
        duplicate_employees_df.count()
    )

    invalid_manager_count = (
        invalid_manager_df.count()
    )


    total_invalid_count = (
        invalid_required_count
        + invalid_email_count
        + invalid_employee_id_count
        + invalid_department_count
        + invalid_status_count
        + invalid_employment_type_count
        + invalid_salary_count
        + invalid_joining_date_count
        + duplicate_count
        + invalid_manager_count
    )


    # ========================================================
    # STEP 15 — Quarantine Invalid Records
    # ========================================================

    quarantine_dfs = [

        invalid_required_df,

        invalid_email_df,

        invalid_employee_id_df,

        invalid_department_df,

        invalid_status_df,

        invalid_employment_type_df,

        invalid_salary_df,

        invalid_joining_date_df,

        duplicate_employees_df,

        invalid_manager_df
    ]


    for quarantine_df in quarantine_dfs:

        if quarantine_df.limit(1).count() > 0:

            (
                quarantine_df
                .write
                .format("delta")
                .mode("append")
                .saveAsTable(
                    QUARANTINE_TABLE
                )
            )


    # ========================================================
    # STEP 16 — Write Valid Employees
    # ========================================================

    target_count = (
        valid_manager_df.count()
    )


    (
        valid_manager_df
        .write
        .format("delta")
        .mode("append")
        .saveAsTable(
            TARGET_TABLE
        )
    )


    # ========================================================
    # STEP 17 — Audit Record
    # ========================================================

    pipeline_end_time = datetime.now()

    execution_time_seconds = (
        pipeline_end_time
        - pipeline_start_time
    ).total_seconds()


    audit_data = [

        (
            PIPELINE_NAME,
            BATCH_ID,
            "SUCCESS",
            source_count,
            target_count,
            total_invalid_count,
            duplicate_count,
            execution_time_seconds,
            pipeline_start_time,
            pipeline_end_time
        )
    ]


    audit_df = spark.createDataFrame(
        audit_data,
        [
            "pipeline_name",
            "batch_id",
            "status",
            "source_count",
            "target_count",
            "invalid_count",
            "duplicate_count",
            "execution_time_seconds",
            "start_time",
            "end_time"
        ]
    )


    (
        audit_df
        .write
        .format("delta")
        .mode("append")
        .saveAsTable(
            AUDIT_TABLE
        )
    )


    print(
        "Employee ingestion completed successfully"
    )


except Exception as e:

    # ========================================================
    # FAILURE HANDLING
    # ========================================================

    pipeline_end_time = datetime.now()

    execution_time_seconds = (
        pipeline_end_time
        - pipeline_start_time
    ).total_seconds()


    print(
        f"Employee ingestion failed: {str(e)}"
    )


    audit_data = [

        (
            PIPELINE_NAME,
            BATCH_ID,
            "FAILED",
            0,
            0,
            0,
            0,
            execution_time_seconds,
            pipeline_start_time,
            pipeline_end_time
        )
    ]


    audit_df = spark.createDataFrame(
        audit_data,
        [
            "pipeline_name",
            "batch_id",
            "status",
            "source_count",
            "target_count",
            "invalid_count",
            "duplicate_count",
            "execution_time_seconds",
            "start_time",
            "end_time"
        ]
    )


    (
        audit_df
        .write
        .format("delta")
        .mode("append")
        .saveAsTable(
            AUDIT_TABLE
        )
    )


    raise


finally:

    spark.stop()
