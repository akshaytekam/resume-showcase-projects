
# 13_gold_treasury.py
# Gold Treasury reporting for Power BI

from pyspark.sql import functions as F

RECON_TABLE = "quince_dev.silver.settlement_bank_reconciliation"
SETTLEMENT_TABLE = "quince_dev.silver.gateway_settlements"
BANK_TABLE = "quince_dev.silver.bank_statement_transactions"

FACT_RECON = "quince_dev.gold.fact_treasury_reconciliation"
AGG_DAILY = "quince_dev.gold.agg_treasury_daily"
FACT_BANK_UNMATCHED = "quince_dev.gold.fact_bank_unmatched"

# --------------------------------------------------
# 1. Validate required inputs
# --------------------------------------------------
for table_name in [RECON_TABLE, SETTLEMENT_TABLE, BANK_TABLE]:
    if not spark.catalog.tableExists(table_name):
        raise ValueError(
            f"Required input table does not exist: {table_name}"
        )

recon = spark.table(RECON_TABLE)
settlements = spark.table(SETTLEMENT_TABLE)
bank = spark.table(BANK_TABLE)

required_recon_columns = [
    "settlement_id",
    "bank_transaction_id",
    "currency",
    "expected_payout",
    "bank_credit",
    "amount_variance",
    "reconciliation_status",
]

missing = [
    c for c in required_recon_columns
    if c not in recon.columns
]

if missing:
    raise ValueError(
        f"Missing reconciliation columns: {missing}"
    )

# --------------------------------------------------
# 2. Gold settlement-level fact table
# --------------------------------------------------
# Bring in the settlement date for reporting.
if "settlement_timestamp_clean" in settlements.columns:
    settlement_dates = settlements.select(
        F.col("settlement_id_clean").cast("string").alias(
            "_settlement_id"
        ),
        F.to_date("settlement_timestamp_clean").alias(
            "settlement_date"
        ),
    ).dropDuplicates(["_settlement_id"])
else:
    settlement_dates = (
        recon.select(
            F.col("settlement_id").cast("string").alias(
                "_settlement_id"
            )
        )
        .distinct()
        .withColumn(
            "settlement_date",
            F.lit(None).cast("date")
        )
    )

fact_recon = (
    recon
    .withColumn(
        "settlement_id",
        F.col("settlement_id").cast("string")
    )
    .join(
        settlement_dates,
        F.col("settlement_id") == F.col("_settlement_id"),
        "left"
    )
    .drop("_settlement_id")
    .withColumn(
        "is_matched",
        F.col("reconciliation_status") == "MATCHED"
    )
    .withColumn(
        "is_variance",
        F.col("reconciliation_status") == "VARIANCE"
    )
    .withColumn(
        "is_unmatched",
        F.col("reconciliation_status") == "UNMATCHED"
    )
    .withColumn(
        "absolute_variance",
        F.abs(F.col("amount_variance"))
    )
    .withColumn(
        "gold_processed_at_utc",
        F.current_timestamp()
    )
)

(
    fact_recon.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(FACT_RECON)
)

# --------------------------------------------------
# 3. Find bank credits not assigned to a settlement
# --------------------------------------------------
# A bank transaction used by either a MATCHED or VARIANCE
# record is considered assigned. Unassigned credits remain
# visible for investigation.
assigned_bank_ids = (
    recon
    .filter(F.col("bank_transaction_id").isNotNull())
    .select(
        F.col("bank_transaction_id").cast("string").alias(
            "_assigned_bank_id"
        )
    )
    .distinct()
)

bank_credits = bank.filter(
    F.col("signed_amount_clean") > 0
)

bank_unmatched = (
    bank_credits
    .join(
        assigned_bank_ids,
        F.col("bank_transaction_id_clean").cast("string")
        == F.col("_assigned_bank_id"),
        "left_anti"
    )
    .withColumn(
        "bank_transaction_date",
        F.to_date("bank_transaction_timestamp_clean")
    )
    .withColumn(
        "exception_type",
        F.lit("BANK_CREDIT_WITHOUT_ASSIGNED_SETTLEMENT")
    )
    .withColumn(
        "gold_processed_at_utc",
        F.current_timestamp()
    )
)

(
    bank_unmatched.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(FACT_BANK_UNMATCHED)
)

# --------------------------------------------------
# 4. Build daily KPI aggregation
# --------------------------------------------------
# Settlement-oriented metrics:
# - eligible settlements
# - matched and exception counts
# - expected payout
# - matched bank credit
# - unmatched expected payout
# - absolute variance
#
# Group by currency to avoid adding unlike currencies.
daily = (
    fact_recon
    .filter(F.col("settlement_date").isNotNull())
    .groupBy("settlement_date", "currency")
    .agg(
        F.countDistinct("settlement_id").alias(
            "eligible_settlement_count"
        ),
        F.countDistinct(
            F.when(F.col("is_matched"), F.col("settlement_id"))
        ).alias("matched_settlement_count"),
        F.countDistinct(
            F.when(F.col("is_variance"), F.col("settlement_id"))
        ).alias("variance_settlement_count"),
        F.countDistinct(
            F.when(F.col("is_unmatched"), F.col("settlement_id"))
        ).alias("unmatched_settlement_count"),
        F.sum("expected_payout").alias(
            "expected_payout_amount"
        ),
        F.sum(
            F.when(
                F.col("is_matched") | F.col("is_variance"),
                F.col("bank_credit")
            ).otherwise(F.lit(0))
        ).alias("assigned_bank_credit_amount"),
        F.sum(
            F.when(
                F.col("is_unmatched"),
                F.col("expected_payout")
            ).otherwise(F.lit(0))
        ).alias("unmatched_expected_payout_amount"),
        F.sum(
            F.when(
                F.col("is_variance"),
                F.col("absolute_variance")
            ).otherwise(F.lit(0))
        ).alias("absolute_variance_amount"),
    )
    .withColumn(
        "settlement_match_rate_pct",
        F.when(
            F.col("eligible_settlement_count") > 0,
            F.round(
                F.col("matched_settlement_count") * 100.0
                / F.col("eligible_settlement_count"),
                2
            )
        )
    )
    .withColumn(
        "gold_processed_at_utc",
        F.current_timestamp()
    )
)

(
    daily.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(AGG_DAILY)
)

print("Created:", FACT_RECON)
print("Created:", FACT_BANK_UNMATCHED)
print("Created:", AGG_DAILY)
