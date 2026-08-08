# ============================================================
# File        : employee_schema.py
# Project     : Enterprise Data Governance Platform
# Purpose     : Define the source schema for employee data
# ============================================================

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DateType,
    DecimalType
)


employee_schema = StructType([

    # --------------------------------------------------------
    # Employee Identifier
    # --------------------------------------------------------

    StructField(
        "employee_id",
        StringType(),
        nullable=False
    ),

    # --------------------------------------------------------
    # Personal Information
    # --------------------------------------------------------

    StructField(
        "employee_name",
        StringType(),
        nullable=False
    ),

    StructField(
        "email_address",
        StringType(),
        nullable=True
    ),

    StructField(
        "phone_number",
        StringType(),
        nullable=True
    ),

    StructField(
        "date_of_birth",
        DateType(),
        nullable=True
    ),

    # --------------------------------------------------------
    # Organization Information
    # --------------------------------------------------------

    StructField(
        "department",
        StringType(),
        nullable=False
    ),

    StructField(
        "job_title",
        StringType(),
        nullable=False
    ),

    StructField(
        "manager_id",
        StringType(),
        nullable=True
    ),

    # --------------------------------------------------------
    # Compensation
    # --------------------------------------------------------

    StructField(
        "salary",
        DecimalType(18, 2),
        nullable=True
    ),

    # --------------------------------------------------------
    # Employment Information
    # --------------------------------------------------------

    StructField(
        "joining_date",
        DateType(),
        nullable=False
    ),

    StructField(
        "employment_status",
        StringType(),
        nullable=False
    )
])
