# ============================================================
# File        : order_schema.py
# Project     : Enterprise Data Governance Platform
# Purpose     : Define the source schema for order data
# ============================================================

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DateType,
    IntegerType,
    DecimalType
)


order_schema = StructType([

    # --------------------------------------------------------
    # Order Identifier
    # --------------------------------------------------------

    StructField(
        "order_id",
        StringType(),
        nullable=False
    ),

    # --------------------------------------------------------
    # Customer Reference
    # --------------------------------------------------------

    StructField(
        "customer_id",
        StringType(),
        nullable=False
    ),

    # --------------------------------------------------------
    # Order Date
    # --------------------------------------------------------

    StructField(
        "order_date",
        DateType(),
        nullable=False
    ),

    # --------------------------------------------------------
    # Store / Product References
    # --------------------------------------------------------

    StructField(
        "store_id",
        StringType(),
        nullable=False
    ),

    StructField(
        "product_id",
        StringType(),
        nullable=False
    ),

    # --------------------------------------------------------
    # Quantity
    # --------------------------------------------------------

    StructField(
        "quantity",
        IntegerType(),
        nullable=False
    ),

    # --------------------------------------------------------
    # Pricing
    # --------------------------------------------------------

    StructField(
        "unit_price",
        DecimalType(18, 2),
        nullable=False
    ),

    StructField(
        "discount_amount",
        DecimalType(18, 2),
        nullable=True
    ),

    StructField(
        "tax_amount",
        DecimalType(18, 2),
        nullable=True
    ),

    StructField(
        "total_amount",
        DecimalType(18, 2),
        nullable=False
    ),

    # --------------------------------------------------------
    # Payment / Order Status
    # --------------------------------------------------------

    StructField(
        "payment_method",
        StringType(),
        nullable=True
    ),

    StructField(
        "order_status",
        StringType(),
        nullable=False
    )
])
