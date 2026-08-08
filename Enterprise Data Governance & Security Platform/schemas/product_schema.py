# ============================================================
# File        : product_schema.py
# Project     : Enterprise Data Governance Platform
# Purpose     : Define the source schema for product data
# ============================================================

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DecimalType,
    DateType
)


product_schema = StructType([

    # --------------------------------------------------------
    # Product Identifier
    # --------------------------------------------------------

    StructField(
        "product_id",
        StringType(),
        nullable=False
    ),

    # --------------------------------------------------------
    # Product Description
    # --------------------------------------------------------

    StructField(
        "product_name",
        StringType(),
        nullable=False
    ),

    StructField(
        "category",
        StringType(),
        nullable=False
    ),

    StructField(
        "subcategory",
        StringType(),
        nullable=True
    ),

    StructField(
        "brand",
        StringType(),
        nullable=True
    ),

    # --------------------------------------------------------
    # Supplier Reference
    # --------------------------------------------------------

    StructField(
        "supplier_id",
        StringType(),
        nullable=True
    ),

    # --------------------------------------------------------
    # Financial Attributes
    # --------------------------------------------------------

    StructField(
        "unit_cost",
        DecimalType(18, 2),
        nullable=False
    ),

    StructField(
        "selling_price",
        DecimalType(18, 2),
        nullable=False
    ),

    StructField(
        "tax_rate",
        DecimalType(5, 2),
        nullable=True
    ),

    # --------------------------------------------------------
    # Product Lifecycle
    # --------------------------------------------------------

    StructField(
        "product_status",
        StringType(),
        nullable=False
    ),

    StructField(
        "launch_date",
        DateType(),
        nullable=True
    )
])
