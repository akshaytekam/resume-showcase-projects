# ============================================================
# File        : payment_schema.py
# Project     : Enterprise Data Governance Platform
# Purpose     : Define the source schema for payment data
# ============================================================

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DateType,
    DecimalType
)


payment_schema = StructType([

    # --------------------------------------------------------
    # Payment Identifier
    # --------------------------------------------------------

    StructField(
        "payment_id",
        StringType(),
        nullable=False
    ),

    # --------------------------------------------------------
    # Related Order
    # --------------------------------------------------------

    StructField(
        "order_id",
        StringType(),
        nullable=False
    ),

    # --------------------------------------------------------
    # Related Customer
    # --------------------------------------------------------

    StructField(
        "customer_id",
        StringType(),
        nullable=False
    ),

    # --------------------------------------------------------
    # Payment Date
    # --------------------------------------------------------

    StructField(
        "payment_date",
        DateType(),
        nullable=False
    ),

    # --------------------------------------------------------
    # Payment Method
    # --------------------------------------------------------

    StructField(
        "payment_method",
        StringType(),
        nullable=False
    ),

    # --------------------------------------------------------
    # External Transaction Reference
    # --------------------------------------------------------

    StructField(
        "transaction_reference",
        StringType(),
        nullable=False
    ),

    # --------------------------------------------------------
    # Payment Amount
    # --------------------------------------------------------

    StructField(
        "amount",
        DecimalType(18, 2),
        nullable=False
    ),

    # --------------------------------------------------------
    # Currency
    # --------------------------------------------------------

    StructField(
        "currency",
        StringType(),
        nullable=False
    ),

    # --------------------------------------------------------
    # Payment Status
    # --------------------------------------------------------

    StructField(
        "payment_status",
        StringType(),
        nullable=False
    )
])
