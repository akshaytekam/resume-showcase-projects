from pyspark import pipelines as dp


# =============================
# Source locations
# =====================================

CATALOG = spark.conf.get("pipeline.catalog", "de_demo")
SCHEMA = spark.conf.get("pipeline.schema", "lakeflow_sales")
VOLUME = spark.conf.get("pipeline.volume", "raw")

BASE_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

CUSTOMER_PATH = f"{BASE_PATH}/customers"
PRODUCT_PATH = f"{BASE_PATH}/products"
SALES_PATH = f"{BASE_PATH}/sales"


# ========================================================
# BRONZE - CUSTOMERS
# ============================================================

@dp.table(
    name="bronze_customers"
)
@dp.expect(
    "valid_customer_id",
    "customer_id IS NOT NULL"
)
def bronze_customers():
    return (
        spark.readStream
        .format("cloudFiles")    # activates Auto Loader.
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", "/Volumes/de_demo/lakeflow_sales/schemas/customers")
        .option("multiLine", "true")  # Handle multi-line JSON arrays
        .load(CUSTOMER_PATH)
    )


# ===========================================================
# BRONZE - PRODUCTS
# ======================================================

@dp.table(
    name="bronze_products"
)
@dp.expect(
    "valid_product_id",
    "product_id IS NOT NULL"
)
def bronze_products():

    return (
        spark.readStream
        .format("cloudFiles")     # activates Auto Loader.
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", "/Volumes/de_demo/lakeflow_sales/schemas/products")
        .option("multiLine", "true")  # Handle multi-line JSON arrays
        .load(PRODUCT_PATH)
    )


# ==========================================================
# BRONZE - SALES
# ============================================================

@dp.table(
    name="bronze_sales"
)
@dp.expect(
    "valid_sale_id",
    "sale_id IS NOT NULL"
)
@dp.expect(
    "positive_quantity",
    "quantity > 0"
)
def bronze_sales():

    return (
        spark.readStream
        .format("cloudFiles")     # activates Auto Loader.
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", "/Volumes/de_demo/lakeflow_sales/schemas/sales")
        .option("multiLine", "true")  # Handle multi-line JSON arrays
        .load(SALES_PATH)
    )
