# This module will let Bronze, Silver, Gold, batch, and streaming processes write execution information into the same audit table.

from datetime import datetime
from pyspark.sql import SparkSession


def write_audit_record(
    spark: SparkSession,
    pipeline_name: str,
    environment: str,
    layer: str,
    table_name: str,
    batch_id: str,
    start_time: datetime,
    end_time: datetime,
    records_read: int,
    records_written: int,
    records_rejected: int,
    status: str,
    error_message: str = None
):
    """
    Write one pipeline execution record
    into the generic audit table.
    """

    audit_table = (
        f"adas_catalog.{environment}.pipeline_audit"
    )

    audit_data = [
        (
            pipeline_name,
            environment,
            layer,
            table_name,
            batch_id,
            start_time,
            end_time,
            records_read,
            records_written,
            records_rejected,
            status,
            error_message
        )
    ]

    audit_columns = [
        "pipeline_name",
        "environment",
        "layer",
        "table_name",
        "batch_id",
        "start_time",
        "end_time",
        "records_read",
        "records_written",
        "records_rejected",
        "status",
        "error_message"
    ]

    audit_df = spark.createDataFrame(
        audit_data,
        audit_columns
    )

    audit_df.write.mode("append").saveAsTable(
        audit_table
    )
