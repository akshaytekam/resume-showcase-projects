# Databricks notebook source
# MAGIC %md
# MAGIC **The dashboard will use:**
# MAGIC
# MAGIC - retail_lakehouse.gold.gold_revenue
# MAGIC - retail_lakehouse.gold.gold_customers

# COMMAND ----------

# MAGIC %md
# MAGIC ### KPI — Total Revenue

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     ROUND(SUM(total_revenue), 2) AS total_revenue
# MAGIC FROM retail_lakehouse.gold.gold_revenue;

# COMMAND ----------

# MAGIC %md
# MAGIC ### KPI — Total Orders

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     SUM(total_orders) AS total_orders
# MAGIC FROM retail_lakehouse.gold.gold_revenue;

# COMMAND ----------

# MAGIC %md
# MAGIC ### KPI — Total Units

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     SUM(total_units) AS total_units
# MAGIC FROM retail_lakehouse.gold.gold_revenue;

# COMMAND ----------

# MAGIC %md
# MAGIC ### KPI — Average Order Value

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     ROUND(
# MAGIC         SUM(total_revenue) / SUM(total_orders),
# MAGIC         2
# MAGIC     ) AS average_order_value
# MAGIC FROM retail_lakehouse.gold.gold_revenue;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Revenue trend

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     order_date,
# MAGIC     ROUND(total_revenue, 2) AS revenue
# MAGIC FROM retail_lakehouse.gold.gold_revenue
# MAGIC ORDER BY order_date;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Orders trend

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     order_date,
# MAGIC     total_orders
# MAGIC FROM retail_lakehouse.gold.gold_revenue
# MAGIC ORDER BY order_date;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Top 10 customers

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     customer_id,
# MAGIC     total_orders,
# MAGIC     ROUND(total_spend, 2) AS total_spend,
# MAGIC     last_order_date
# MAGIC FROM retail_lakehouse.gold.gold_customers
# MAGIC ORDER BY total_spend DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Customer revenue distribution

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     CASE
# MAGIC         WHEN total_spend >= 20000 THEN 'VIP'
# MAGIC         WHEN total_spend >= 10000 THEN 'HIGH_VALUE'
# MAGIC         WHEN total_spend >= 5000 THEN 'MEDIUM_VALUE'
# MAGIC         ELSE 'REGULAR'
# MAGIC     END AS customer_segment,
# MAGIC
# MAGIC     COUNT(*) AS customers,
# MAGIC     ROUND(SUM(total_spend), 2) AS revenue
# MAGIC
# MAGIC FROM retail_lakehouse.gold.gold_customers
# MAGIC
# MAGIC GROUP BY
# MAGIC     CASE
# MAGIC         WHEN total_spend >= 20000 THEN 'VIP'
# MAGIC         WHEN total_spend >= 10000 THEN 'HIGH_VALUE'
# MAGIC         WHEN total_spend >= 5000 THEN 'MEDIUM_VALUE'
# MAGIC         ELSE 'REGULAR'
# MAGIC     END
# MAGIC
# MAGIC ORDER BY revenue DESC;

# COMMAND ----------

