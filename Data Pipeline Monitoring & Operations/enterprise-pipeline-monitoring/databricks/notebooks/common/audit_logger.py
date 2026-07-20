"""
===============================================================================
File Name : audit_logger.py

Description:
    Enterprise Audit Logger for Databricks ETL Pipelines.

Business Purpose
----------------
Captures execution details for every ETL notebook.

Audit information is stored in Delta Lake so Operations,
Support and Monitoring teams can track execution history.

Features
--------
✓ Start Time
✓ End Time
✓ Duration
✓ Pipeline Name
✓ Layer
✓ Notebook Name
✓ Status
✓ Records Read
✓ Records Written
✓ Error Message
✓ Run ID
✓ Timestamp

Author:
Enterprise Data Engineering Team

Version:
1.0
===============================================================================
"""

import uuid
from datetime import datetime

from pyspark.sql import Row


class AuditLogger:

    def __init__(
        self,
        spark,
        config,
        pipeline_name,
        layer,
        notebook_name
    ):

        self.spark = spark
        self.config = config

        self.pipeline_name = pipeline_name
        self.layer = layer
        self.notebook_name = notebook_name

        self.audit_path = config["audit_path"]

        self.run_id = str(uuid.uuid4())

        self.start_time = None

    # ------------------------------------------------------------------

    def start(self):

        self.start_time = datetime.now()

        print("=" * 80)
        print("Pipeline Started")
        print("=" * 80)

        print("Pipeline :", self.pipeline_name)
        print("Layer :", self.layer)
        print("Notebook :", self.notebook_name)
        print("Run ID :", self.run_id)
        print("Start :", self.start_time)

    # ------------------------------------------------------------------

    def success(

        self,

        records_read,

        records_written

    ):

        end_time = datetime.now()

        duration = (

            end_time -

            self.start_time

        ).total_seconds()

        row = Row(

            run_id=self.run_id,

            pipeline_name=self.pipeline_name,

            notebook=self.notebook_name,

            layer=self.layer,

            status="SUCCESS",

            start_time=str(self.start_time),

            end_time=str(end_time),

            duration_seconds=duration,

            records_read=records_read,

            records_written=records_written,

            error_message=""

        )

        self._write(row)

        print("=" * 80)
        print("Pipeline Completed Successfully")
        print("=" * 80)

    # ------------------------------------------------------------------

    def failure(

        self,

        exception

    ):

        end_time = datetime.now()

        duration = (

            end_time -

            self.start_time

        ).total_seconds()

        row = Row(

            run_id=self.run_id,

            pipeline_name=self.pipeline_name,

            notebook=self.notebook_name,

            layer=self.layer,

            status="FAILED",

            start_time=str(self.start_time),

            end_time=str(end_time),

            duration_seconds=duration,

            records_read=0,

            records_written=0,

            error_message=str(exception)

        )

        self._write(row)

        raise exception

    # ------------------------------------------------------------------

    def _write(

        self,

        row

    ):

        df = self.spark.createDataFrame([row])

        (

            df.write

            .format("delta")

            .mode("append")

            .save(

                self.audit_path

            )

        )
