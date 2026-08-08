# ============================================================
# File        : store_schema.py
# Project     : Enterprise Data Governance Platform
# Purpose     : Define the source schema for store data
# ============================================================

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DateType
)


store_schema = StructType([

    # --------------------------------------------------------
    # Store Identifier
    # --------------------------------------------------------

    StructField(
        "store_id",
        StringType(),
        nullable=False
    ),

    # --------------------------------------------------------
    # Store Information
    # --------------------------------------------------------

    StructField(
        "store_name",
        StringType(),
        nullable=False
    ),

    StructField(
        "store_type",
        StringType(),
        nullable=False
    ),

    # --------------------------------------------------------
    # Location Information
    # --------------------------------------------------------

    StructField(
        "city",
        StringType(),
        nullable=False
    ),

    StructField(
        "state",
        StringType(),
        nullable=False
    ),

    StructField(
        "postal_code",
        StringType(),
        nullable=True
    ),

    StructField(
        "region",
        StringType(),
        nullable=False
    ),

    # --------------------------------------------------------
    # Store Management
    # --------------------------------------------------------

    StructField(
        "manager_id",
        StringType(),
        nullable=True
    ),

    # --------------------------------------------------------
    # Store Lifecycle
    # --------------------------------------------------------

    StructField(
        "opening_date",
        DateType(),
        nullable=True
    ),

    StructField(
        "store_status",
        StringType(),
        nullable=False
    )
])
