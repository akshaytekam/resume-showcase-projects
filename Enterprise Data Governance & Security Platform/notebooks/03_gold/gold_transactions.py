from pyspark.sql import functions as F


CATALOG = "dev_catalog"

SILVER = f"{CATALOG}.silver"
GOLD = f"{CATALOG}.gold"


def build_transactions():

    transactions = spark.table(
        f"{SILVER}.transactions"
    )

    result = (
        transactions

        .withColumn(
            "transaction_year",
            F.year("transaction_date")
        )

        .withColumn(
            "transaction_month",
            F.month("transaction_date")
        )

        .withColumn(
            "transaction_quarter",
            F.quarter("transaction_date")
        )

        .withColumn(
            "transaction_status",

            F.when(
                F.col("amount") > 0,
                "COMPLETED"
            )
            .otherwise(
                "INVALID"
            )
        )

        .select(
            "transaction_id",
            "customer_id",
            "transaction_date",
            "amount",
            "country",
            "transaction_year",
            "transaction_month",
            "transaction_quarter",
            "transaction_status"
        )
    )

    (
        result
        .write
        .format("delta")
        .mode("overwrite")
        .option(
            "overwriteSchema",
            "true"
        )
        .saveAsTable(
            f"{GOLD}.transactions"
        )
    )


if __name__ == "__main__":

    build_transactions()
