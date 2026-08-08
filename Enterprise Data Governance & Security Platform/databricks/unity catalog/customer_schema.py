from pyspark.sql.types import (
    StructType,
    StructField,
    StringType
)

customer_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("customer_name", StringType(), True),
    StructField("email_address", StringType(), True),
    StructField("phone_number", StringType(), True),
    StructField("date_of_birth", StringType(), True),
    StructField("address_line1", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("postal_code", StringType(), True),
    StructField("country_code", StringType(), True),
    StructField("pan_number", StringType(), True),
    StructField("loyalty_tier", StringType(), True),
    StructField("customer_status", StringType(), True),
    StructField("created_at", StringType(), True)
])
