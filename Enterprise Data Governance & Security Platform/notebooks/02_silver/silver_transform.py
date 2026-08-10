from pyspark.sql import functions as F


CATALOG = "dev_catalog"

BRONZE = f"{CATALOG}.bronze"
SILVER = f"{CATALOG}.silver"


def transform_customers():

    df = spark.table(
        f"{BRONZE}.customers"
    )

    silver_df = (
        df
        # Remove duplicate customers
        .dropDuplicates(["customer_id"])

        # Standardize names
        .withColumn(
            "first_name",
            F.initcap(F.trim("first_name"))
        )
        .withColumn(
            "last_name",
            F.initcap(F.trim("last_name"))
        )

        # Standardize email
        .withColumn(
            "email",
            F.lower(F.trim("email"))
        )

        # Standardize phone
        .withColumn(
            "phone",
            F.regexp_replace(
                "phone",
                "[^0-9]",
                ""
            )
        )

        # Standardize country
        .withColumn(
            "country",
            F.upper(F.trim("country"))
        )

        # Handle missing values
        .withColumn(
            "email",
            F.when(
                F.col("email") == "",
                None
            ).otherwise(F.col("email"))
        )

        .withColumn(
            "updated_at",
            F.current_timestamp()
        )
    )

    (
        silver_df
        .write
        .format("delta")
        .mode("overwrite")
        .option(
            "overwriteSchema",
            "true"
        )
        .saveAsTable(
            f"{SILVER}.customers"
        )
    )


def transform_transactions():

    df = spark.table(
        f"{BRONZE}.transactions"
    )

    silver_df = (
        df
        .dropDuplicates(["transaction_id"])

        .withColumn(
            "transaction_date",
            F.to_date("transaction_date")
        )

        .withColumn(
            "amount",
            F.col("amount").cast("decimal(18,2)")
        )

        .filter(
            F.col("amount") >= 0
        )

        .withColumn(
            "country",
            F.upper(F.trim("country"))
        )

        .withColumn(
            "updated_at",
            F.current_timestamp()
        )
    )

    (
        silver_df
        .write
        .format("delta")
        .mode("overwrite")
        .option(
            "overwriteSchema",
            "true"
        )
        .saveAsTable(
            f"{SILVER}.transactions"
        )
    )


if __name__ == "__main__":

    transform_customers()
    transform_transactions()
