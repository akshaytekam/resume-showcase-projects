-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Here we'll prove that our batch ingestion is idempotent.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ```text
-- MAGIC First run:
-- MAGIC CSV files → COPY INTO → Bronze
-- MAGIC                          ↓
-- MAGIC                     13717 records
-- MAGIC
-- MAGIC Second run:
-- MAGIC Same CSV files → COPY INTO → Bronze
-- MAGIC                          ↓
-- MAGIC                     Still 13717 records
-- MAGIC ```
-- MAGIC It should not become 27434

-- COMMAND ----------

-- 1. Check current record count
SELECT
    COUNT(*) AS before_count
FROM retail_lakehouse.bronze.orders;

-- COMMAND ----------

-- 2. Run COPY INTO again
COPY INTO retail_lakehouse.bronze.orders
FROM (
  SELECT
    CAST(order_id AS BIGINT) AS order_id,
    CAST(customer_id AS BIGINT) AS customer_id,
    CAST(order_date AS DATE) AS order_date,
    product_id,
    CAST(quantity AS INT) AS quantity,
    CAST(unit_price AS DECIMAL(10,2)) AS unit_price,
    status
  FROM '/Volumes/retail_lakehouse/raw/retail_files/orders/'
)
FILEFORMAT = CSV
FORMAT_OPTIONS (
    'header' = 'true'
);

-- COMMAND ----------

-- 3. Check record count after second execution
SELECT
    COUNT(*) AS after_count
FROM retail_lakehouse.bronze.orders;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **And That was our idempotency proof.**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Now the below code will show you the duplicate records in the dataset not idempotency failuer

-- COMMAND ----------

-- 4. Check duplicate order IDs
SELECT
    order_id,
    COUNT(*) AS record_count
FROM retail_lakehouse.bronze.orders
GROUP BY order_id
HAVING COUNT(*) > 1
ORDER BY record_count DESC;

-- COMMAND ----------

-- Idempotency validation
SELECT
    CASE
        WHEN COUNT(*) = 13717
        THEN 'PASS - Idempotent ingestion verified'
        ELSE 'FAIL - Unexpected record count'
    END AS validation_result
FROM retail_lakehouse.bronze.orders;

-- COMMAND ----------

DESCRIBE HISTORY retail_lakehouse.bronze.orders;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC _**COPY INTO tracks the source files that have already been processed. If the same job is rerun with the same files, those files are skipped rather than inserted again. In our project we loaded 13,717 orders, reran the same COPY INTO command, and verified that the target still contained 13,717 records.**_

-- COMMAND ----------

