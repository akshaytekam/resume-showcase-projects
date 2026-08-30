# Databricks notebook source
# MAGIC %md
# MAGIC ### we'll implement the Streaming side of the architecture:

# COMMAND ----------

# MAGIC %md
# MAGIC ```text
# MAGIC JSON Events
# MAGIC      │
# MAGIC      │ Auto Loader
# MAGIC      ↓
# MAGIC Bronze Delta
# MAGIC ```
# MAGIC Instead of repeatedly scanning the entire directory, Auto Loader tracks newly arriving files and process using [.format("cloudFiles")]

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# 1. Source location

source_path = (
    "/Volumes/retail_lakehouse/raw/retail_files/events/"
)

# 2. Schema location

schema_path = (
    "/Volumes/retail_lakehouse/raw/retail_files/"
    "schema/events/"
)

# 3. Checkpoint location

checkpoint_path = (
    "/Volumes/retail_lakehouse/raw/retail_files/"
    "checkpoints/events/"
)

# 4. Read JSON using Auto Loader

events_df = (
    spark.readStream
         .format("cloudFiles")  # this tells databricks to use Auto Loader to detect and ingest files arriving in this location.
         .option("cloudFiles.format", "json")
         .option("cloudFiles.schemaLocation", schema_path) # Auto Loader stores schema information here.
         .load(source_path)
)

# COMMAND ----------

# 5. Add ingestion timestamp

events_df = events_df.withColumn(
    "ingestion_time",
    current_timestamp()
)

# COMMAND ----------

# 6. Write to Bronze Delta

streaming_query = (
    events_df.writeStream
             .format("delta")
             .option(
                 "checkpointLocation",
                 checkpoint_path
             )                        # The checkpoint stores streaming progress information
             .outputMode("append")
             .trigger(availableNow=True)
             .toTable(
                 "retail_lakehouse.bronze.order_events"
             )
)

# Wait for the streaming query to finish processing all available files
streaming_query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC **Verify Bronze Streaming Table**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS total_events
# MAGIC FROM retail_lakehouse.bronze.order_events;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM retail_lakehouse.bronze.order_events
# MAGIC LIMIT 10;

# COMMAND ----------

