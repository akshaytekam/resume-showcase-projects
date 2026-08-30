# Databricks notebook source
# MAGIC %md
# MAGIC **Now we build the Delta Live Tables / Lakeflow Declarative Pipelines part of the project.**
# MAGIC
# MAGIC **The goal is to put batch + streaming + quality rules + Bronze → Silver → Gold into one declarative pipeline.**

# COMMAND ----------

# MAGIC %md
# MAGIC **_DLT understands the dependencies:_**
# MAGIC
# MAGIC ```text
# MAGIC              BATCH CSV
# MAGIC                  │
# MAGIC                  ↓
# MAGIC           DLT BRONZE BATCH
# MAGIC                  │
# MAGIC                  ↓
# MAGIC           DLT SILVER
# MAGIC                  │
# MAGIC                  ↓
# MAGIC            DLT GOLD
# MAGIC                  │
# MAGIC                  ↓
# MAGIC              DASHBOARD
# MAGIC
# MAGIC
# MAGIC           STREAMING JSON
# MAGIC                  │
# MAGIC                  ↓
# MAGIC         DLT BRONZE STREAM
# MAGIC                  │
# MAGIC                  ↓
# MAGIC           DLT SILVER
# MAGIC ```
# MAGIC
# MAGIC **For the DLT pipeline, let the pipeline configuration control the target catalog/schema rather than hard-coding the target schema everywhere.**
# MAGIC
# MAGIC That makes the pipeline easier to deploy between environments such as:
# MAGIC
# MAGIC ```text
# MAGIC DEV -> TEST -> PROD
# MAGIC ```

# COMMAND ----------

import dlt

from pyspark.sql.functions import (
    col,
    upper,
    trim,
    sum,
    countDistinct
)


# ============================================================
# BRONZE - BATCH

@dlt.table(
    name="bronze_orders",
    comment="Raw batch order data"
)
def bronze_orders():

    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(
            "/Volumes/retail_lakehouse/raw/"
            "retail_files/orders/"
        )
    )


# ============================================================
# BRONZE - STREAMING

@dlt.table(
    name="bronze_order_events",
    comment="Streaming order events"
)
def bronze_order_events():

    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option(
            "cloudFiles.schemaLocation",
            "/Volumes/retail_lakehouse/raw/"
            "retail_files/schema/dlt_events/"
        )
        .load(
            "/Volumes/retail_lakehouse/raw/"
            "retail_files/events/"
        )
    )


# ============================================================
# SILVER

@dlt.table(
    name="silver_orders",
    comment="Cleaned and validated orders"
)
@dlt.expect(
    "valid_order_id",
    "order_id IS NOT NULL"
)
@dlt.expect(
    "valid_customer_id",
    "customer_id IS NOT NULL"
)
@dlt.expect(
    "valid_quantity",
    "quantity > 0"
)
@dlt.expect(
    "valid_price",
    "unit_price >= 0"
)
def silver_orders():

    df = dlt.read("bronze_orders")

    return (
        df
        .filter(col("order_id").isNotNull())
        .filter(col("customer_id").isNotNull())
        .filter(col("quantity") > 0)
        .filter(col("unit_price") >= 0)
        .withColumn(
            "status",
            upper(trim(col("status")))
        )
        .dropDuplicates(["order_id"])
    )


# ============================================================
# GOLD - REVENUE

@dlt.table(
    name="gold_revenue",
    comment="Daily sales KPIs"
)
def gold_revenue():

    df = dlt.read("silver_orders")

    return (
        df.groupBy("order_date")
          .agg(
              sum(
                  col("quantity") * col("unit_price")
              ).alias("total_revenue"),

              sum("quantity")
              .alias("total_units"),

              countDistinct("order_id")
              .alias("total_orders")
          )
    )


# ============================================================
# GOLD - CUSTOMERS

@dlt.table(
    name="gold_customers",
    comment="Customer sales analytics"
)
def gold_customers():

    df = dlt.read("silver_orders")

    return (
        df.groupBy("customer_id")
          .agg(
              countDistinct("order_id")
              .alias("total_orders"),

              sum(
                  col("quantity") * col("unit_price")
              ).alias("total_spend")
          )
    )