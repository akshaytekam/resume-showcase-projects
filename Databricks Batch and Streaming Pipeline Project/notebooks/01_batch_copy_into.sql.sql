-- Databricks notebook source
-- MAGIC %md
-- MAGIC **Project Setup and Raw Data Ingetion Notebook**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## **In Databricks, we'll create:**
-- MAGIC ```text
-- MAGIC Catalog
-- MAGIC    ↓
-- MAGIC retail_lakehouse (Catalog)
-- MAGIC    ↓
-- MAGIC raw (Schema)
-- MAGIC    ↓
-- MAGIC retail_files (Volume)
-- MAGIC ```

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create Catalog

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS retail_lakehouse;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### CREATE SCHEMA

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS retail_lakehouse.raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### CREATE VOLUME

-- COMMAND ----------

CREATE VOLUME IF NOT EXISTS retail_lakehouse.raw.retail_files;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Bronze schema

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS retail_lakehouse.bronze;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create the Bronze Delta table Orders

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS retail_lakehouse.bronze.orders
(
    order_id BIGINT,
    customer_id BIGINT,
    order_date DATE,
    product_id STRING,
    quantity INT,
    unit_price DECIMAL(10,2),
    status STRING
)
USING DELTA;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **COPY INTO loads files from a file location into a Delta table** 

-- COMMAND ----------

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

-- MAGIC %md
-- MAGIC **Validate record count**

-- COMMAND ----------

SELECT COUNT(*) AS total_records
FROM retail_lakehouse.bronze.orders;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Preview data**

-- COMMAND ----------

SELECT *
FROM retail_lakehouse.bronze.orders
LIMIT 20;

-- COMMAND ----------

