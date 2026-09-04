from pyspark import pipelines as dp


CUSTOMER_PATH = "/Volumes/demo/lakeflow/raw/customers"
PRODUCT_PATH = "/Volumes/demo/lakeflow/raw/products"
SALES_PATH = "/Volumes/demo/lakeflow/raw/sales"


@dp.table(
    name="bronze_customers"
)
@dp.expect("valid_customer_id", "customer_id IS NOT NULL")
def bronze_customers():

    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load(CUSTOMER_PATH)
    )


@dp.table(
    name="bronze_products"
)
@dp.expect("valid_product_id", "product_id IS NOT NULL")
def bronze_products():

    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load(PRODUCT_PATH)
    )


@dp.table(
    name="bronze_sales"
)
@dp.expect("valid_sale_id", "sale_id IS NOT NULL")
@dp.expect("positive_quantity", "quantity > 0")
def bronze_sales():

    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load(SALES_PATH)
    )
