from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr


# ============================================================
# SILVER CUSTOMERS - SCD TYPE 1
# ============================================================

dp.create_streaming_table(
    name="silver_customers_scd1",
    comment="Customer dimension using SCD Type 1"
)

dp.create_auto_cdc_flow(
    target="silver_customers_scd1",
    source="bronze_customers",
    keys=["customer_id"],
    sequence_by=col("sequence_num"),
    apply_as_deletes=expr("operation = 'DELETE'"),
    except_column_list=[
        "operation",
        "sequence_num"
    ],
    stored_as_scd_type=1
)


# ============================================================
# SILVER PRODUCTS - SCD TYPE 1
# ============================================================

dp.create_streaming_table(
    name="silver_products_scd1",
    comment="Product dimension using SCD Type 1"
)

dp.create_auto_cdc_flow(
    target="silver_products_scd1",
    source="bronze_products",
    keys=["product_id"],
    sequence_by=col("sequence_num"),
    apply_as_deletes=expr("operation = 'DELETE'"),
    except_column_list=[
        "operation",
        "sequence_num"
    ],
    stored_as_scd_type=1
)

# ============================================================
# SILVER CUSTOMERS - SCD TYPE 2
# ============================================================

dp.create_streaming_table(
    name="silver_customers_scd2",
    comment="Customer dimension using SCD Type 2"
)

dp.create_auto_cdc_flow(
    target="silver_customers_scd2",
    source="bronze_customers",
    keys=["customer_id"],
    sequence_by=col("sequence_num"),

    # Handle DELETE events
    apply_as_deletes=expr("operation = 'DELETE'"),

    # Remove CDC control columns
    except_column_list=[
        "operation",
        "sequence_num"
    ],

    # Store complete history
    stored_as_scd_type=2
)

# ============================================================
# SILVER SALES FACT
# ============================================================

@dp.table(
    name="silver_sales",
    comment="Cleaned and enriched sales fact table"
)
@dp.expect(
    "valid_sale_id",
    "sale_id IS NOT NULL"
)
@dp.expect(
    "valid_customer_id",
    "customer_id IS NOT NULL"
)
@dp.expect(
    "valid_product_id",
    "product_id IS NOT NULL"
)
@dp.expect(
    "positive_quantity",
    "quantity > 0"
)
@dp.expect(
    "non_negative_amount",
    "sale_amount >= 0"
)
def silver_sales():

    sales = (
        spark.readStream
        .table("bronze_sales")
    )

    customers = (
        spark.read
        .table("silver_customers_scd1")
        .select(
            "customer_id",
            "customer_name",
            "city"
        )
    )

    products = (
        spark.read
        .table("silver_products_scd1")
        .select(
            "product_id",
            "product_name",
            "category",
            "price"
        )
    )

    result = (
        sales
        .join(
            customers,
            on="customer_id",
            how="left"
        )
        .join(
            products,
            on="product_id",
            how="left"
        )
        .select(
            sales.sale_id,
            sales.customer_id,
            customers.customer_name,
            customers.city,
            sales.product_id,
            products.product_name,
            products.category,
            products.price.alias("current_product_price"),
            sales.quantity,
            sales.sale_amount,
            sales.sale_timestamp
        )
    )

    return (
        result
        .withColumn(
            "sale_timestamp",
            col("sale_timestamp").cast("timestamp")
        )
        .dropDuplicates(["sale_id"])
    )
