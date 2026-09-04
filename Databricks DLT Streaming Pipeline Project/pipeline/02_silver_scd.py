# The modern API is create_auto_cdc_flow(), which replaces the older apply_changes() function.

from pyspark import pipelines as dp


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
