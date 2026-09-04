# The modern API is create_auto_cdc_flow(), which replaces the older apply_changes() function.

from pyspark import pipelines as dp

# ------Customers-----------

@dp.create_streaming_table(
    name="silver_customers_scd1"
)

dp.create_auto_cdc_flow(
    target="silver_customers_scd1",
    source="bronze_customers",
    keys=["customer_id"],
    sequence_by="sequence_num",
    apply_as_delete="operation = 'DELETE'",
    except_column_list=["operation", "sequence_num"],
    stored_as_scd_type=1
)

@dp.create_streaming_table(
    name="silver_customers_scd2"
)

dp.create_auto_cdc_flow(
    target="silver_customers_scd2",
    source="bronze_customers",
    keys=["customer_id"],
    sequence_by="sequence_num",
    apply_as_delete="operation = 'DELETE'",
    except_column_list=["operation", "sequence_num"],
    stored_as_scd_type=2
)

# ----------Products------------

@dp.create_streaming_table(
    name="silver_products_scd2"
)

dp.create_auto_cdc_flow(
    target="silver_products_scd2",
    source="bronze_products",
    keys=["product_id"],
    sequence_by="sequence_num",
    apply_as_delete="operation = 'DELETE'",
    except_column_list=["operation", "sequence_num"],
    stored_as_scd_type=2
)

# Sales don't require SCD because they're transactional events.

@dp.table(
    name="silver_sales"
)
@dp.expect("valid_customer", "customer_id IS NOT NULL")
@dp.expect("valid_product", "product_id IS NOT NULL")
@dp.expect("positive_amount", "sale_amount > 0")
def silver_sales():

    return (
        spark.readStream
        .table("bronze_sales")
        .select(
            "sale_id",
            "customer_id",
            "product_id",
            "quantity",
            "sale_amount",
            "sale_timestamp"
        )
    )
