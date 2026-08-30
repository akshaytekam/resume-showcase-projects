# Databricks notebook source
# MAGIC %md
# MAGIC **Now we move from Silver → Gold.**
# MAGIC
# MAGIC **The Silver table contains cleaned, deduplicated orders. The Gold layer converts that data into business-ready sales KPIs.**
# MAGIC
# MAGIC ```text
# MAGIC SILVER
# MAGIC    │
# MAGIC    │ business aggregation
# MAGIC    ↓
# MAGIC GOLD_REVENUE
# MAGIC    │
# MAGIC    ├── Revenue
# MAGIC    ├── Orders
# MAGIC    └── Units
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Gold schema

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS retail_lakehouse.gold;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold revenue table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE retail_lakehouse.gold.gold_revenue
# MAGIC USING DELTA
# MAGIC AS
# MAGIC
# MAGIC SELECT
# MAGIC     order_date,
# MAGIC
# MAGIC     SUM(quantity * unit_price) AS total_revenue,
# MAGIC
# MAGIC     SUM(quantity) AS total_units,
# MAGIC
# MAGIC     COUNT(DISTINCT order_id) AS total_orders,
# MAGIC
# MAGIC     ROUND(
# MAGIC         SUM(quantity * unit_price)
# MAGIC         / COUNT(DISTINCT order_id),
# MAGIC         2
# MAGIC     ) AS average_order_value
# MAGIC
# MAGIC FROM retail_lakehouse.silver.orders
# MAGIC
# MAGIC GROUP BY order_date
# MAGIC
# MAGIC ORDER BY order_date;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validate the Gold table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM retail_lakehouse.gold.gold_revenue
# MAGIC ORDER BY order_date;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Some business validation
# MAGIC We should make sure we don't have negative revenue.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS invalid_rows
# MAGIC FROM retail_lakehouse.gold.gold_revenue
# MAGIC WHERE total_revenue < 0;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calculate overall KPIs
# MAGIC
# MAGIC These queries will eventually be useful for the dashboard.

# COMMAND ----------

# MAGIC %md
# MAGIC **Total revenue**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     ROUND(SUM(total_revenue), 2) AS total_revenue
# MAGIC FROM retail_lakehouse.gold.gold_revenue;

# COMMAND ----------

# MAGIC %md
# MAGIC **Total orders**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     SUM(total_orders) AS total_orders
# MAGIC FROM retail_lakehouse.gold.gold_revenue;

# COMMAND ----------

# MAGIC %md
# MAGIC **Total units**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     SUM(total_units) AS total_units
# MAGIC FROM retail_lakehouse.gold.gold_revenue;

# COMMAND ----------

