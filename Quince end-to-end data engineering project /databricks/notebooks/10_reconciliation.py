
# 10_reconciliation.py
# Reconcile gateway settlements to bank statement transactions.
#
# Inputs:
#   quince_dev.silver.gateway_settlements
#   quince_dev.silver.bank_statement_transactions
#
# Outputs:
#   quince_dev.silver.settlement_bank_reconciliation
#   quince_dev.audit.settlement_reconciliation_exceptions

from pyspark.sql import functions as F
from pyspark.sql.window import Window

SETTLEMENT_TABLE = "quince_dev.silver.gateway_settlements"
BANK_TABLE = "quince_dev.silver.bank_statement_transactions"

RESULT_TABLE = "quince_dev.silver.settlement_bank_reconciliation"
EXCEPTION_TABLE = "quince_dev.audit.settlement_reconciliation_exceptions"

# Configure this for your business and currency precision.
# For two-decimal currencies, 0.01 is a one-cent tolerance.
AMOUNT_TOLERANCE = 0.01

# --------------------------------------------------
# 1. Read inputs and validate required columns
# --------------------------------------------------
settlements = spark.table(SETTLEMENT_TABLE)
bank = spark.table(BANK_TABLE)

required_settlement = [
    "settlement_id_clean",
    "currency_clean",
]

required_bank = [
    "bank_transaction_id_clean",
    "currency_clean",
    "signed_amount_clean",
]

for name in required_settlement:
    if name not in settlements.columns:
        raise ValueError(
            f"Missing settlement column: {name}"
        )

for name in required_bank:
    if name not in bank.columns:
        raise ValueError(
            f"Missing bank column: {name}"
        )

# Expected payout priority:
# 1. Net amount explicitly supplied by gateway.
# 2. Settlement amount only when its semantics are confirmed.
#
# Do not automatically use gross amount as the bank payout.
if "net_amount_clean" in settlements.columns:
    settlements = settlements.withColumn(
        "_expected_payout",
        F.coalesce(
            F.col("net_amount_clean"),
            F.col("settlement_amount_clean")
            if "settlement_amount_clean" in settlements.columns
            else F.lit(None).cast("decimal(18,2)")
        )
    )
elif "settlement_amount_clean" in settlements.columns:
    settlements = settlements.withColumn(
        "_expected_payout",
        F.col("settlement_amount_clean")
    )
else:
    raise ValueError(
        "No expected payout amount is available. "
        "Confirm the gateway amount semantics first."
    )

# Only settlements that represent completed/paid payouts
# should be compared with bank credits.
if "settlement_status_clean" in settlements.columns:
    completed_statuses = [
        "PAID",
        "SETTLED",
        "COMPLETED",
        "SUCCESS",
        "SUCCESSFUL",
    ]

    settlements = settlements.withColumn(
        "_is_completed",
        F.upper(F.col("settlement_status_clean")).isin(
            completed_statuses
        )
    )
else:
    # Without a status field, we cannot reliably distinguish
    # pending payouts from completed payouts.
    raise ValueError(
        "Settlement status column is missing. "
        "Add an explicit business rule for eligible payouts "
        "before reconciling."
    )

settlements = settlements.filter(
    F.col("_is_completed")
    & F.col("_expected_payout").isNotNull()
    & F.col("currency_clean").isNotNull()
)

# Keep bank credits only. Negative values are debits.
bank = bank.filter(
    F.col("signed_amount_clean") > 0
)

# --------------------------------------------------
# 2. Find explicit reference columns
# --------------------------------------------------
def find_reference_column(df, candidates):
    for candidate in candidates:
        if candidate in df.columns:
            return candidate
    return None

settlement_ref_col = find_reference_column(
    settlements,
    [
        "payout_reference_clean",
        "settlement_reference_clean",
        "bank_reference_clean",
        "transfer_reference_clean",
        "reference_clean",
        "bank_reference",
        "payout_reference",
    ],
)

bank_ref_col = find_reference_column(
    bank,
    [
        "settlement_reference_clean",
        "payout_reference_clean",
        "gateway_reference_clean",
        "bank_reference_clean",
        "reference_clean",
        "reference",
        "transaction_reference",
    ],
)

# If the gateway payout reference is not available,
# the settlement ID may be used only if the bank statement
# actually carries that ID as a reference.
if settlement_ref_col is None:
    settlement_ref_col = "settlement_id_clean"

# Prepare the bank reference:
# Prefer a structured reference field. If none exists,
# use the bank description as a limited exact-token search.
if bank_ref_col:
    bank = bank.withColumn(
        "_bank_reference",
        F.upper(F.trim(F.col(bank_ref_col).cast("string")))
    )
elif "bank_description_clean" in bank.columns:
    bank = bank.withColumn(
        "_bank_reference",
        F.upper(F.trim(F.col("bank_description_clean")))
    )
else:
    bank = bank.withColumn(
        "_bank_reference",
        F.lit(None).cast("string")
    )

settlements = settlements.withColumn(
    "_match_reference",
    F.upper(
        F.trim(F.col(settlement_ref_col).cast("string"))
    )
)

# Stable input identifiers for this run.
settlements = settlements.withColumn(
    "_settlement_key",
    F.col("settlement_id_clean").cast("string")
)

bank = bank.withColumn(
    "_bank_key",
    F.col("bank_transaction_id_clean").cast("string")
)

# --------------------------------------------------
# 3. Build explicit-reference match candidates
# --------------------------------------------------
# This is not unrestricted fuzzy matching. We require a
# nonblank settlement reference to appear as a distinct
# token in the bank reference/description.
#
# If the bank has a structured reference field, exact equality
# is preferred. If it has only a description, token matching
# is used and results should be reviewed for false positives.

s = settlements.alias("s")
b = bank.alias("b")

reference_match = (
    (F.col("s.currency_clean") == F.col("b.currency_clean"))
    & F.col("s._match_reference").isNotNull()
    & (F.col("s._match_reference") != "")
    & F.col("b._bank_reference").isNotNull()
    & (
        (F.col("b._bank_reference") == F.col("s._match_reference"))
        | (
            F.col("b._bank_reference").rlike(
                r"(^|[^A-Z0-9])"
            )
            # For arbitrary reference values, literal token
            # matching is safer than constructing a regex.
        )
    )
)

# Use literal token containment for description matching.
# Structured reference columns use exact equality only.
if bank_ref_col:
    reference_match = (
        (F.col("s.currency_clean") == F.col("b.currency_clean"))
        & (
            F.col("s._match_reference")
            == F.col("b._bank_reference")
        )
        & F.col("s._match_reference").isNotNull()
        & (F.col("s._match_reference") != "")
    )
else:
    reference_match = (
        (F.col("s.currency_clean") == F.col("b.currency_clean"))
        & F.col("s._match_reference").isNotNull()
        & (F.col("s._match_reference") != "")
        & F.col("b._bank_reference").isNotNull()
        & F.expr(
            "instr(b._bank_reference, s._match_reference) > 0"
        )
    )

candidates = (
    s.join(b, reference_match, "inner")
    .select(
        F.col("s._settlement_key").alias("settlement_id"),
        F.col("b._bank_key").alias("bank_transaction_id"),
        F.col("s.currency_clean").alias("currency"),
        F.col("s._expected_payout").alias("expected_payout"),
        F.col("b.signed_amount_clean").alias("bank_credit"),
        F.abs(
            F.col("s._expected_payout")
            - F.col("b.signed_amount_clean")
        ).alias("amount_variance"),
        F.lit("EXPLICIT_REFERENCE").alias("match_method"),
    )
)

# --------------------------------------------------
# 4. Rank candidates to avoid duplicate matches
# --------------------------------------------------
# Choose the closest amount first, then stable identifiers.
# This is a greedy one-to-one selection, not a global
# optimization across every possible combination.
settlement_rank = Window.partitionBy(
    "settlement_id"
).orderBy(
    F.col("amount_variance").asc(),
    F.col("bank_transaction_id").asc(),
)

ranked = candidates.withColumn(
    "_settlement_rank",
    F.row_number().over(settlement_rank)
)

best_per_settlement = ranked.filter(
    F.col("_settlement_rank") == 1
).drop("_settlement_rank")

bank_rank = Window.partitionBy(
    "bank_transaction_id"
).orderBy(
    F.col("amount_variance").asc(),
    F.col("settlement_id").asc(),
)

best_per_bank = (
    best_per_settlement
    .withColumn(
        "_bank_rank",
        F.row_number().over(bank_rank)
    )
    .filter(F.col("_bank_rank") == 1)
    .drop("_bank_rank")
)

# --------------------------------------------------
# 5. Classify reconciled and exception records
# --------------------------------------------------
matched = best_per_bank.withColumn(
    "reconciliation_status",
    F.when(
        F.col("amount_variance") <= F.lit(AMOUNT_TOLERANCE),
        F.lit("MATCHED")
    ).otherwise(F.lit("VARIANCE"))
)

matched_settlement_ids = matched.select(
    "settlement_id"
).distinct()

unmatched = (
    settlements
    .join(
        matched_settlement_ids,
        settlements["_settlement_key"] == matched_settlement_ids["settlement_id"],
        "left_anti",
    )
    .select(
        F.col("_settlement_key").alias("settlement_id"),
        F.lit(None).cast("string").alias("bank_transaction_id"),
        F.col("currency_clean").alias("currency"),
        F.col("_expected_payout").alias("expected_payout"),
        F.lit(None).cast("decimal(18,2)").alias("bank_credit"),
        F.lit(None).cast("decimal(18,2)").alias("amount_variance"),
        F.lit(None).cast("string").alias("match_method"),
        F.lit("UNMATCHED").alias("reconciliation_status"),
    )
)

result = (
    matched.select(
        "settlement_id",
        "bank_transaction_id",
        "currency",
        "expected_payout",
        "bank_credit",
        "amount_variance",
        "match_method",
        "reconciliation_status",
    )
    .unionByName(unmatched)
    .withColumn(
        "reconciled_at_utc",
        F.current_timestamp()
    )
)

# --------------------------------------------------
# 6. Persist reconciliation output
# --------------------------------------------------
(
    result.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(RESULT_TABLE)
)

exceptions = result.filter(
    F.col("reconciliation_status") != "MATCHED"
)

(
    exceptions.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(EXCEPTION_TABLE)
)

print("Reconciliation output:", RESULT_TABLE)
print("Exception output:", EXCEPTION_TABLE)
print("Total settlement rows:", result.count())
