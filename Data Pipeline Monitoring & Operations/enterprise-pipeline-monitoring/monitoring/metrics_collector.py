"""
===============================================================================

File Name
    metrics_collector.py

Description
    Enterprise Pipeline Metrics Collector

Features

    • Pipeline Metrics
    • Task Metrics
    • Record Counts
    • Validation Metrics
    • Execution Duration

Author
    Enterprise Data Engineering Team

===============================================================================
"""

from datetime import datetime

from monitoring.monitoring_logger import (
    log_info,
    log_error
)

###############################################################################
# Metrics Collector
###############################################################################

class MetricsCollector:
    """
    Collects execution metrics for a pipeline run.
    """

    def __init__(self):

        self.metrics = {

            "pipeline_name": None,

            "execution_date": None,

            "start_time": None,

            "end_time": None,

            "duration_seconds": 0,

            "records_read": 0,

            "records_written": 0,

            "records_failed": 0,

            "duplicate_records": 0,

            "validation_failures": 0,

            "tasks": {}

        }

    ###########################################################################
    # Start Pipeline
    ###########################################################################

    def start_pipeline(
        self,
        pipeline_name
    ):

        self.metrics["pipeline_name"] = pipeline_name

        self.metrics["execution_date"] = datetime.utcnow().strftime(
            "%Y-%m-%d"
        )

        self.metrics["start_time"] = datetime.utcnow()

        log_info(
            f"Metrics collection started for {pipeline_name}"
        )

    ###########################################################################
    # End Pipeline
    ###########################################################################

    def end_pipeline(self):

        self.metrics["end_time"] = datetime.utcnow()

        duration = (

            self.metrics["end_time"]
            -
            self.metrics["start_time"]

        ).total_seconds()

        self.metrics["duration_seconds"] = round(
            duration,
            2
        )

        log_info(
            f"Pipeline completed in {duration:.2f} seconds."
        )

    ###########################################################################
    # Get Metrics
    ###########################################################################

    def get_metrics(self):

        return self.metrics

  ###############################################################################
# Task Metrics
###############################################################################

    def add_task_metric(
        self,
        task_name,
        duration_seconds,
        status
    ):
        """
        Store execution metrics for a task.
        """

        self.metrics["tasks"][task_name] = {

            "duration_seconds": duration_seconds,

            "status": status

        }

        log_info(

            f"Task Metric Added : {task_name}"

        )

###############################################################################
# Record Counts
###############################################################################

    def set_record_counts(
        self,
        records_read,
        records_written,
        records_failed
    ):
        """
        Store pipeline record counts.
        """

        self.metrics["records_read"] = records_read

        self.metrics["records_written"] = records_written

        self.metrics["records_failed"] = records_failed

###############################################################################
# Duplicate Records
###############################################################################

    def set_duplicate_records(
        self,
        duplicate_records
    ):
        """
        Store duplicate record count.
        """

        self.metrics["duplicate_records"] = duplicate_records

###############################################################################
# Validation Metrics
###############################################################################

    def set_validation_failures(
        self,
        validation_failures
    ):
        """
        Store validation failure count.
        """

        self.metrics["validation_failures"] = validation_failures

###############################################################################
# Success Rate
###############################################################################

    def calculate_success_rate(self):
        """
        Calculate successful processing percentage.
        """

        read = self.metrics["records_read"]

        failed = self.metrics["records_failed"]

        if read == 0:

            return 0.0

        success_rate = (

            (read - failed)

            / read

        ) * 100

        return round(

            success_rate,

            2

        )

###############################################################################
# Data Quality Score
###############################################################################

    def calculate_quality_score(self):
        """
        Calculate overall quality score.
        """

        read = self.metrics["records_read"]

        duplicates = self.metrics["duplicate_records"]

        validation = self.metrics["validation_failures"]

        if read == 0:

            return 0.0

        penalty = (

            duplicates

            +

            validation

        ) / read

        quality = (

            1 - penalty

        ) * 100

        return round(

            quality,

            2

        )

import json

###############################################################################
# Pipeline Summary
###############################################################################

    def build_summary(self):
        """
        Build execution summary.
        """

        summary = {

            "pipeline_name": self.metrics["pipeline_name"],

            "execution_date": self.metrics["execution_date"],

            "duration_seconds": self.metrics["duration_seconds"],

            "records_read": self.metrics["records_read"],

            "records_written": self.metrics["records_written"],

            "records_failed": self.metrics["records_failed"],

            "duplicate_records": self.metrics["duplicate_records"],

            "validation_failures": self.metrics["validation_failures"],

            "success_rate": self.calculate_success_rate(),

            "quality_score": self.calculate_quality_score()

        }

        return summary

###############################################################################
# JSON Export
###############################################################################

    def export_json(
        self,
        output_file
    ):
        """
        Export metrics to JSON.
        """

        with open(
            output_file,
            "w"
        ) as file:

            json.dump(

                self.metrics,

                file,

                indent=4,

                default=str

            )

        log_info(

            f"Metrics exported to {output_file}"

        )

###############################################################################
# CloudWatch Payload
###############################################################################

    def build_cloudwatch_payload(self):
        """
        Build CloudWatch metric payload.
        """

        payload = [

            {

                "MetricName": "PipelineDuration",

                "Value": self.metrics["duration_seconds"],

                "Unit": "Seconds"

            },

            {

                "MetricName": "RecordsRead",

                "Value": self.metrics["records_read"],

                "Unit": "Count"

            },

            {

                "MetricName": "RecordsWritten",

                "Value": self.metrics["records_written"],

                "Unit": "Count"

            },

            {

                "MetricName": "RecordsFailed",

                "Value": self.metrics["records_failed"],

                "Unit": "Count"

            },

            {

                "MetricName": "SuccessRate",

                "Value": self.calculate_success_rate(),

                "Unit": "Percent"

            },

            {

                "MetricName": "QualityScore",

                "Value": self.calculate_quality_score(),

                "Unit": "Percent"

            }

        ]

        return payload

###############################################################################
# Reset Metrics
###############################################################################

    def reset(self):
        """
        Reset collector for next pipeline run.
        """

        self.metrics = {

            "pipeline_name": None,

            "execution_date": None,

            "start_time": None,

            "end_time": None,

            "duration_seconds": 0,

            "records_read": 0,

            "records_written": 0,

            "records_failed": 0,

            "duplicate_records": 0,

            "validation_failures": 0,

            "tasks": {}

        }

        log_info(

            "Metrics collector reset."

        )

###############################################################################
# Print Summary
###############################################################################

    def print_summary(self):
        """
        Print execution summary.
        """

        summary = self.build_summary()

        logger_separator = "=" * 80

        log_info(logger_separator)

        for key, value in summary.items():

            log_info(

                f"{key:<25}: {value}"

            )

        log_info(logger_separator)
