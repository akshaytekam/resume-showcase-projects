from pyspark.sql import functions as F


CATALOG = "dev_catalog"

SILVER = f"{CATALOG}.silver"
GOLD = f"{CATALOG}.gold"


def build_customer_360():

    customers = spark.table(
        f"{SILVER}.customers"
    )

    transactions = spark.table(
        f"{SILVER}.transactions"
    )

    transaction_summary = (
        transactions
        .groupBy("customer_id")
        .agg(

            F.count(
                "transaction_id"
            ).alias(
                "total_transactions"
            ),

            F.sum(
                "amount"
            ).alias(
                "total_spend"
            ),

            F.avg(
                "amount"
            ).alias(
                "average_transaction_value"
            ),

            F.max(
                "transaction_date"
            ).alias(
                "last_transaction_date"
            )
        )
    )

    customer_360 = (
        customers.alias("c")

        .join(
            transaction_summary.alias("t"),
            F.col("c.customer_id")
            == F.col("t.customer_id"),
            "left"
        )

        .select(

            F.col(
                "c.customer_id"
            ),

            F.col(
                "c.first_name"
            ),

            F.col(
                "c.last_name"
            ),

            F.col(
                "c.email"
            ),

            F.col(
                "c.phone"
            ),

            F.col(
                "c.country"
            ),

            F.coalesce(
                F.col("t.total_transactions"),
                F.lit(0)
            ).alias(
                "total_transactions"
            ),

            F.coalesce(
                F.col("t.total_spend"),
                F.lit(0)
            ).alias(
                "total_spend"
            ),

            F.col(
                "t.average_transaction_value"
            ),

            F.col(
                "t.last_transaction_date"
            )
        )

        .withColumn(
            "customer_segment",

            F.when(
                F.col("total_spend") >= 100000,
                "PLATINUM"
            )
            .when(
                F.col("total_spend") >= 50000,
                "GOLD"
            )
            .when(
                F.col("total_spend") >= 10000,
                "SILVER"
            )
            .otherwise(
                "STANDARD"
            )
        )

        .withColumn(
            "created_at",
            F.current_timestamp()
        )
    )

    (
        customer_360
        .write
        .format("delta")
        .mode("overwrite")
        .option(
            "overwriteSchema",
            "true"
        )
        .saveAsTable(
            f"{GOLD}.customer_360"
        )
    )


if __name__ == "__main__":

    build_customer_360()
