# ============================================================
# File        : customer_schema.py
# Project     : Enterprise Data Governance Platform
# Purpose     : Define the source schema for customer data
# ============================================================

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DateType,
    TimestampType
)


customer_schema = StructType([

    # --------------------------------------------------------
    # Customer Business Key
    # --------------------------------------------------------

    StructField(
        "customer_id",
        StringType(),
        nullable=False
    ),

    # --------------------------------------------------------
    # Customer Personal Information
    # --------------------------------------------------------

    StructField(
        "customer_name",
        StringType(),
        nullable=True
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
    # Customer Address
    # --------------------------------------------------------

    StructField(
        "address_line1",
        StringType(),
        nullable=True
    ),

    StructField(
        "city",
        StringType(),
        nullable=True
    ),

    StructField(
        "state",
        StringType(),
        nullable=True
    ),

    StructField(
        "postal_code",
        StringType(),
        nullable=True
    ),

    StructField(
        "country_code",
        StringType(),
        nullable=True
    ),

    # --------------------------------------------------------
    # Highly Sensitive Financial / Identity Information
    # --------------------------------------------------------

    StructField(
        "pan_number",
        StringType(),
        nullable=True
    ),

    # --------------------------------------------------------
    # Business Attributes
    # --------------------------------------------------------

    StructField(
        "loyalty_tier",
        StringType(),
        nullable=True
    ),

    StructField(
        "customer_status",
        StringType(),
        nullable=True
    ),

    # --------------------------------------------------------
    # Source System Timestamp
    # --------------------------------------------------------

    StructField(
        "created_at",
        TimestampType(),
        nullable=True
    )
])
