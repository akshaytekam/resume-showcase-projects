from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType
)

from datetime import datetime

# source path
SOURCE_PATH = (
    "s3://abc-retail-governance/"
    "dev/source/customers/"
)

#target
TARGET_TABLE = "dev_catalog.bronze.customers"

#quarantine table
QUARANTINE_TABLE = (
    "dev_catalog.bronze.customers_quarantine"
)

#read CSV
df_raw = (
    spark.read
         .format("csv")
         .option("header", "true")
         .option("mode", "PERMISSIVE")
         .schema(customer_schema)
         .load(SOURCE_PATH)
)

