# Databricks notebook source
# MAGIC %md
# MAGIC **Now we move from Bronze → Silver.**
# MAGIC
# MAGIC **This is where we perform the actual data-quality cleanup and deduplication.**
# MAGIC
# MAGIC ```text
# MAGIC BRONZE
# MAGIC   │
# MAGIC   ├── NULL validation
# MAGIC   ├── Invalid quantity
# MAGIC   ├── Invalid price
# MAGIC   ├── Status standardization
# MAGIC   └── Duplicate removal
# MAGIC           ↓
# MAGIC        SILVER
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC **So our actions should be:**
# MAGIC
# MAGIC ```text
# MAGIC Problem                    Action
# MAGIC ------------------------------------------------
# MAGIC NULL customer_id           Remove
# MAGIC quantity <= 0              Remove
# MAGIC unit_price < 0             Remove
# MAGIC " completed "              Standardize
# MAGIC Duplicate order_id         Keep latest record
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Silver schema

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS retail_lakehouse.silver;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Silver table
# MAGIC **We'll use a temporary view first so the transformation is easy to understand.**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW cleaned_orders AS
# MAGIC
# MAGIC SELECT
# MAGIC     order_id,
# MAGIC     customer_id,
# MAGIC     order_date,
# MAGIC     product_id,
# MAGIC     quantity,
# MAGIC     unit_price,
# MAGIC
# MAGIC     UPPER(TRIM(status)) AS status,
# MAGIC
# MAGIC     ROW_NUMBER() OVER (
# MAGIC         PARTITION BY order_id
# MAGIC         ORDER BY order_date DESC
# MAGIC     ) AS rn
# MAGIC
# MAGIC FROM retail_lakehouse.bronze.orders
# MAGIC
# MAGIC WHERE order_id IS NOT NULL
# MAGIC   AND customer_id IS NOT NULL
# MAGIC   AND order_date IS NOT NULL
# MAGIC   AND product_id IS NOT NULL
# MAGIC   AND quantity > 0
# MAGIC   AND unit_price >= 0;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE retail_lakehouse.silver.orders
# MAGIC USING DELTA
# MAGIC AS
# MAGIC
# MAGIC SELECT
# MAGIC     order_id,
# MAGIC     customer_id,
# MAGIC     order_date,
# MAGIC     product_id,
# MAGIC     quantity,
# MAGIC     unit_price,
# MAGIC     status
# MAGIC
# MAGIC FROM cleaned_orders
# MAGIC
# MAGIC WHERE rn = 1;

# COMMAND ----------

# MAGIC %md
# MAGIC **That's the core Silver logic.**

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validate Silver Table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS silver_records
# MAGIC FROM retail_lakehouse.silver.orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Remaining Invalid Records
# MAGIC SELECT COUNT(*) AS invalid_records
# MAGIC FROM retail_lakehouse.silver.orders
# MAGIC WHERE customer_id IS NULL
# MAGIC    OR quantity <= 0
# MAGIC    OR unit_price < 0;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Show Duplicates Removed

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     order_id,
# MAGIC     COUNT(*) AS cnt
# MAGIC FROM retail_lakehouse.silver.orders
# MAGIC GROUP BY order_id
# MAGIC HAVING COUNT(*) > 1;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check standardized status

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT status
# MAGIC FROM retail_lakehouse.silver.orders;

# COMMAND ----------

