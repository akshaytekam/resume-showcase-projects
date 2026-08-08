# ============================================================
# File        : load_payments.py
# Project     : Enterprise Data Governance Platform
# Purpose     : Ingest payment data into Bronze Delta
# ============================================================

from datetime import datetime
import uuid

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    input_file_name,
    lit,
    trim,
    upper
)

from schemas.payment_schema import payment_schema


# ============================================================
# Configuration
# ============================================================

SOURCE_PATH = (
    "s3://enterprise-data-platform/raw/payments/"
)

TARGET_TABLE = (
    "dev_catalog.bronze.payments"
)

QUARANTINE_TABLE = (
    "dev_catalog.bronze.payments_quarantine"
)

AUDIT_TABLE = (
    "dev_catalog.governance.ingestion_audit"
)

ORDER_TABLE = (
    "dev_catalog.bronze.orders"
)

CUSTOMER_TABLE = (
    "dev_catalog.bronze.customers"
)

PIPELINE_NAME = "payment_ingestion"

BATCH_ID = str(uuid.uuid4())


# ============================================================
# Spark Session
# ============================================================

spark = (
    SparkSession.builder
    .appName("PaymentDataIngestion")
    .getOrCreate()
)


pipeline_start_time = datetime.now()


try:

    print(
        "Starting payment ingestion pipeline"
    )

    # ========================================================
    # STEP 1 — Read Payment Source
    # ========================================================

    payments_df = (
        spark.read
        .option("header", "true")
        .schema(payment_schema)
        .csv(SOURCE_PATH)
    )

    source_count = payments_df.count()

    print(
        f"Source payment records: {source_count}"
    )


    # ========================================================
    # STEP 2 — Add Technical Metadata
    # ========================================================

    payments_df = (
        payments_df

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
    # STEP 3 — Standardize Values
    # ========================================================

    payments_df = (
        payments_df

        .withColumn(
            "payment_id",
            trim(col("payment_id"))
        )

        .withColumn(
            "order_id",
            trim(col("order_id"))
        )

        .withColumn(
            "customer_id",
            trim(col("customer_id"))
        )

        .withColumn(
            "payment_method",
            upper(
                trim(
                    col("payment_method")
                )
            )
        )

        .withColumn(
            "payment_status",
            upper(
                trim(
                    col("payment_status")
                )
            )
        )

        .withColumn(
            "currency",
            upper(
                trim(
                    col("currency")
                )
            )
        )

        .withColumn(
            "payment_provider",
            upper(
                trim(
                    col("payment_provider")
                )
            )
        )

        .withColumn(
            "card_last4",
            trim(
                col("card_last4")
            )
        )
    )


    # ========================================================
    # STEP 4 — Required Field Validation
    # ========================================================

    invalid_required_condition = (
        col("payment_id").isNull()
        | (trim(col("payment_id")) == "")

        | col("order_id").isNull()
        | (trim(col("order_id")) == "")

        | col("customer_id").isNull()
        | (trim(col("customer_id")) == "")

        | col("payment_method").isNull()

        | col("payment_status").isNull()

        | col("transaction_amount").isNull()

        | col("currency").isNull()

        | col("transaction_timestamp").isNull()
    )


    invalid_required_df = (
        payments_df
        .filter(
            invalid_required_condition
        )
        .withColumn(
            "quarantine_reason",
            lit("REQUIRED_FIELD_MISSING")
        )
    )


    valid_required_df = (
        payments_df
        .filter(
            ~invalid_required_condition
        )
    )


    # ========================================================
    # STEP 5 — Payment ID Format
    # ========================================================

    invalid_payment_id_df = (
        valid_required_df
        .filter(
            ~col("payment_id").rlike(
                "^PAY[0-9]{8}$"
            )
        )
        .withColumn(
            "quarantine_reason",
            lit("INVALID_PAYMENT_ID")
        )
    )


    valid_payment_id_df = (
        valid_required_df
        .filter(
            col("payment_id").rlike(
                "^PAY[0-9]{8}$"
            )
        )
    )


    # ========================================================
    # STEP 6 — Validate Payment Method
    # ========================================================

    valid_payment_methods = [
        "CREDIT_CARD",
        "DEBIT_CARD",
        "UPI",
        "NET_BANKING",
        "WALLET",
        "CASH"
    ]


    invalid_method_df = (
        valid_payment_id_df
        .filter(
            ~col("payment_method").isin(
                valid_payment_methods
            )
        )
        .withColumn(
            "quarantine_reason",
            lit("INVALID_PAYMENT_METHOD")
        )
    )


    valid_method_df = (
        valid_payment_id_df
        .filter(
            col("payment_method").isin(
                valid_payment_methods
            )
        )
    )


    # ========================================================
    # STEP 7 — Validate Payment Status
    # ========================================================

    valid_payment_statuses = [
        "INITIATED",
        "AUTHORIZED",
        "CAPTURED",
        "FAILED",
        "REFUNDED",
        "CANCELLED"
    ]


    invalid_status_df = (
        valid_method_df
        .filter(
            ~col("payment_status").isin(
                valid_payment_statuses
            )
        )
        .withColumn(
            "quarantine_reason",
            lit("INVALID_PAYMENT_STATUS")
        )
    )


    valid_status_df = (
        valid_method_df
        .filter(
            col("payment_status").isin(
                valid_payment_statuses
            )
        )
    )


    # ========================================================
    # STEP 8 — Validate Payment Amount
    # ========================================================

    invalid_amount_df = (
        valid_status_df
        .filter(
            col("transaction_amount") < 0
        )
        .withColumn(
            "quarantine_reason",
            lit("NEGATIVE_PAYMENT_AMOUNT")
        )
    )


    valid_amount_df = (
        valid_status_df
        .filter(
            col("transaction_amount") >= 0
        )
    )


    # ========================================================
    # STEP 9 — Validate Currency
    # ========================================================

    valid_currencies = [
        "INR",
        "USD",
        "EUR",
        "GBP"
    ]


    invalid_currency_df = (
        valid_amount_df
        .filter(
            ~col("currency").isin(
                valid_currencies
            )
        )
        .withColumn(
            "quarantine_reason",
            lit("INVALID_CURRENCY")
        )
    )


    valid_currency_df = (
        valid_amount_df
        .filter(
            col("currency").isin(
                valid_currencies
            )
        )
    )


    # ========================================================
    # STEP 10 — Validate Card Last Four
    # ========================================================

    # card_last4 is optional because UPI, cash, wallet,
    # and net banking may not have a card number.

    invalid_card_last4_df = (
        valid_currency_df
        .filter(
            col("card_last4").isNotNull()
            &
            (
                ~col("card_last4").rlike(
                    "^[0-9]{4}$"
                )
            )
        )
        .withColumn(
            "quarantine_reason",
            lit("INVALID_CARD_LAST4")
        )
    )


    valid_card_last4_df = (
        valid_currency_df
        .filter(
            col("card_last4").isNull()
            |
            col("card_last4").rlike(
                "^[0-9]{4}$"
            )
        )
    )


    # ========================================================
    # STEP 11 — Duplicate Payment Validation
    # ========================================================

    duplicate_payment_ids = (
        valid_card_last4_df
        .groupBy("payment_id")
        .count()
        .filter(
            col("count") > 1
        )
        .select("payment_id")
    )


    duplicate_payments_df = (
        valid_card_last4_df
        .join(
            duplicate_payment_ids,
            on="payment_id",
            how="inner"
        )
        .withColumn(
            "quarantine_reason",
            lit("DUPLICATE_PAYMENT_ID")
        )
    )


    valid_payments_df = (
        valid_card_last4_df
        .dropDuplicates(
            ["payment_id"]
        )
    )


    # ========================================================
    # STEP 12 — Order Referential Integrity
    # ========================================================

    order_reference_df = (
        spark.table(ORDER_TABLE)
        .select(
            "order_id"
        )
        .dropDuplicates()
    )


    invalid_order_payment_df = (
        valid_payments_df
        .join(
            order_reference_df,
            on="order_id",
            how="left_anti"
        )
        .withColumn(
            "quarantine_reason",
            lit("ORDER_NOT_FOUND")
        )
    )


    valid_order_payment_df = (
        valid_payments_df
        .join(
            order_reference_df,
            on="order_id",
            how="inner"
        )
    )


    # ========================================================
    # STEP 13 — Customer Referential Integrity
    # ========================================================

    customer_reference_df = (
        spark.table(CUSTOMER_TABLE)
        .select(
            "customer_id"
        )
        .dropDuplicates()
    )


    invalid_customer_payment_df = (
        valid_order_payment_df
        .join(
            customer_reference_df,
            on="customer_id",
            how="left_anti"
        )
        .withColumn(
            "quarantine_reason",
            lit("CUSTOMER_NOT_FOUND")
        )
    )


    valid_customer_payment_df = (
        valid_order_payment_df
        .join(
            customer_reference_df,
            on="customer_id",
            how="inner"
        )
    )


    # ========================================================
    # STEP 14 — Payment/Order Amount Reconciliation
    # ========================================================

    order_amount_df = (
        spark.table(ORDER_TABLE)
        .select(
            "order_id",
            col("total_amount")
            .alias("order_amount")
        )
    )


    payment_reconciliation_df = (
        valid_customer_payment_df
        .join(
            order_amount_df,
            on="order_id",
            how="inner"
        )
    )


    # We allow a tiny difference because of decimal rounding.

    invalid_reconciliation_df = (
        payment_reconciliation_df
        .filter(
            (
                col("transaction_amount")
                -
                col("order_amount")
            ).cast("decimal(18,2)")
            != 0
        )
        .withColumn(
            "quarantine_reason",
            lit("PAYMENT_ORDER_AMOUNT_MISMATCH")
        )
    )


    valid_reconciliation_df = (
        payment_reconciliation_df
        .filter(
            (
                col("transaction_amount")
                -
                col("order_amount")
            ).cast("decimal(18,2)")
            == 0
        )
    )


    # ========================================================
    # STEP 15 — Calculate Validation Counts
    # ========================================================

    invalid_required_count = (
        invalid_required_df.count()
    )

    invalid_payment_id_count = (
        invalid_payment_id_df.count()
    )

    invalid_method_count = (
        invalid_method_df.count()
    )

    invalid_status_count = (
        invalid_status_df.count()
    )

    invalid_amount_count = (
        invalid_amount_df.count()
    )

    invalid_currency_count = (
        invalid_currency_df.count()
    )

    invalid_card_count = (
        invalid_card_last4_df.count()
    )

    duplicate_count = (
        duplicate_payments_df.count()
    )

    invalid_order_count = (
        invalid_order_payment_df.count()
    )

    invalid_customer_count = (
        invalid_customer_payment_df.count()
    )

    invalid_reconciliation_count = (
        invalid_reconciliation_df.count()
    )


    total_invalid_count = (
        invalid_required_count
        + invalid_payment_id_count
        + invalid_method_count
        + invalid_status_count
        + invalid_amount_count
        + invalid_currency_count
        + invalid_card_count
        + duplicate_count
        + invalid_order_count
        + invalid_customer_count
        + invalid_reconciliation_count
    )


    # ========================================================
    # STEP 16 — Quarantine Invalid Records
    # ========================================================

    quarantine_dfs = [

        invalid_required_df,

        invalid_payment_id_df,

        invalid_method_df,

        invalid_status_df,

        invalid_amount_df,

        invalid_currency_df,

        invalid_card_last4_df,

        duplicate_payments_df,

        invalid_order_payment_df,

        invalid_customer_payment_df,

        invalid_reconciliation_df
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
    # STEP 17 — Select Final Columns
    # ========================================================

    final_payments_df = (
        valid_reconciliation_df
        .select(
            "payment_id",
            "order_id",
            "customer_id",
            "payment_method",
            "payment_status",
            "transaction_amount",
            "currency",
            "card_last4",
            "payment_provider",
            "transaction_timestamp",
            "source_file",
            "ingestion_timestamp",
            "batch_id"
        )
    )


    # ========================================================
    # STEP 18 — Write Valid Payments
    # ========================================================

    target_count = (
        final_payments_df.count()
    )


    (
        final_payments_df
        .write
        .format("delta")
        .mode("append")
        .saveAsTable(
            TARGET_TABLE
        )
    )


    # ========================================================
    # STEP 19 — Audit Record
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
        "Payment ingestion completed successfully"
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
        f"Payment ingestion failed: {str(e)}"
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
