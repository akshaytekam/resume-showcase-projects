# Databricks notebook source
# MAGIC %md
# MAGIC **Now we'll create the second Gold table: customer-level sales analytics.**
# MAGIC
# MAGIC ```text
# MAGIC SILVER
# MAGIC    │
# MAGIC    │ customer aggregation
# MAGIC    ↓
# MAGIC GOLD_CUSTOMERS
# MAGIC    │
# MAGIC    ├── Total Orders
# MAGIC    ├── Total Spend
# MAGIC    └── Last Order Date
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create the Gold table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE retail_lakehouse.gold.gold_customers
# MAGIC USING DELTA
# MAGIC AS
# MAGIC
# MAGIC SELECT
# MAGIC     customer_id,
# MAGIC
# MAGIC     COUNT(DISTINCT order_id) AS total_orders,
# MAGIC
# MAGIC     ROUND(
# MAGIC         SUM(quantity * unit_price),
# MAGIC         2
# MAGIC     ) AS total_spend,
# MAGIC
# MAGIC     MAX(order_date) AS last_order_date
# MAGIC
# MAGIC FROM retail_lakehouse.silver.orders
# MAGIC
# MAGIC GROUP BY customer_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validate the table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM retail_lakehouse.gold.gold_customers
# MAGIC ORDER BY total_spend DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Find high-value customers

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     customer_id,
# MAGIC     total_orders,
# MAGIC     total_spend,
# MAGIC     last_order_date
# MAGIC FROM retail_lakehouse.gold.gold_customers
# MAGIC WHERE total_spend >= 10000
# MAGIC ORDER BY total_spend DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Customer segmentation
# MAGIC
# MAGIC simple business segment:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     customer_id,
# MAGIC     total_orders,
# MAGIC     total_spend,
# MAGIC     last_order_date,
# MAGIC
# MAGIC     CASE
# MAGIC         WHEN total_spend >= 20000 THEN 'VIP'
# MAGIC         WHEN total_spend >= 10000 THEN 'HIGH_VALUE'
# MAGIC         WHEN total_spend >= 5000 THEN 'MEDIUM_VALUE'
# MAGIC         ELSE 'REGULAR'
# MAGIC     END AS customer_segment
# MAGIC
# MAGIC FROM retail_lakehouse.gold.gold_customers;
# MAGIC
# MAGIC -- For this project i would keep segmentation out of the physical table and add it later if required. 
# MAGIC -- The core Gold table should remain simple.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Validation
# MAGIC SELECT *
# MAGIC FROM retail_lakehouse.gold.gold_customers
# MAGIC ORDER BY total_spend DESC
# MAGIC LIMIT 10;

# COMMAND ----------

