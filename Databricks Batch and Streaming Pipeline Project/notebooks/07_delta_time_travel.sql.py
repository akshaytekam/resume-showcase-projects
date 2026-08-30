# Databricks notebook source
# MAGIC %md
# MAGIC **Now we'll implement the audit/history requirement of the project.**
# MAGIC
# MAGIC **Delta Lake keeps transaction history for Delta tables, which lets us see operations performed on the table and query previous table versions using Time Travel.**

# COMMAND ----------

# MAGIC %md
# MAGIC ```text
# MAGIC Silver / Gold Delta Table
# MAGIC           │
# MAGIC           ↓
# MAGIC     Delta Transaction Log
# MAGIC           │
# MAGIC      ┌────┴─────┐
# MAGIC      ↓          ↓
# MAGIC  HISTORY    TIME TRAVEL
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### View Delta transaction history

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY retail_lakehouse.gold.gold_revenue;
# MAGIC
# MAGIC -- The exact history depends on how many times you've executed the previous notebooks.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get only the important audit information

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     version,
# MAGIC     timestamp,
# MAGIC     userName,
# MAGIC     operation
# MAGIC FROM (
# MAGIC     DESCRIBE HISTORY retail_lakehouse.gold.gold_revenue
# MAGIC )
# MAGIC ORDER BY version DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check the current version

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     MAX(version) AS current_version
# MAGIC FROM (
# MAGIC     DESCRIBE HISTORY retail_lakehouse.gold.gold_revenue
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ### Time Travel, means read an older version

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM retail_lakehouse.gold.gold_revenue
# MAGIC VERSION AS OF 2;
# MAGIC
# MAGIC -- This doesn't modify your current table.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Compare current vs previous version

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS current_rows
# MAGIC FROM retail_lakehouse.gold.gold_revenue;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS old_rows
# MAGIC FROM retail_lakehouse.gold.gold_revenue
# MAGIC VERSION AS OF 2;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Time Travel using timestamp

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM retail_lakehouse.gold.gold_revenue
# MAGIC TIMESTAMP AS OF '2026-08-30 09:00:00';

# COMMAND ----------

# MAGIC %md
# MAGIC **Time Travel is not the same as backup**
# MAGIC
# MAGIC Time Travel lets you access previous versions while the required Delta data files and transaction-log history are retained.

# COMMAND ----------

# MAGIC %md
# MAGIC ### See the underlying table details

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL retail_lakehouse.gold.gold_revenue;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Audit a specific operation
# MAGIC
# MAGIC Filter history

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     version,
# MAGIC     timestamp,
# MAGIC     operation,
# MAGIC     operationParameters
# MAGIC FROM (
# MAGIC     DESCRIBE HISTORY retail_lakehouse.gold.gold_revenue
# MAGIC )
# MAGIC WHERE operation = 'CREATE OR REPLACE TABLE'
# MAGIC ORDER BY version DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC **This is particularly useful for:**
# MAGIC
# MAGIC - auditing
# MAGIC - debugging
# MAGIC - accidental data changes
# MAGIC - comparing table versions
# MAGIC - investigating pipeline failures
# MAGIC - recovering/reading previous states

# COMMAND ----------

# MAGIC %md
# MAGIC