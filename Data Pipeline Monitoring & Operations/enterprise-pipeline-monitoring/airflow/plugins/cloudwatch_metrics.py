"""
===============================================================================

File Name
    cloudwatch_metrics.py

Description
    Enterprise CloudWatch Metrics Publisher

Features

    • Pipeline Success Metrics
    • Pipeline Failure Metrics
    • Task Duration Metrics
    • Record Count Metrics
    • SLA Metrics

Author
    Enterprise Data Engineering Team

===============================================================================
"""

import boto3

import logging

from datetime import datetime

from include.airflow_config import (

    AWS_REGION,

    METRIC_NAMESPACE,

    ENABLE_CLOUDWATCH

)

logger = logging.getLogger(__name__)

###############################################################################
# CloudWatch Client
###############################################################################

cloudwatch = boto3.client(

    "cloudwatch",

    region_name=AWS_REGION

)

###############################################################################
# Generic Metric Publisher
###############################################################################

def publish_metric(

    metric_name,

    metric_value,

    dimensions=None,

    unit="Count"

):
    """
    Publish custom CloudWatch metric.
    """

    if not ENABLE_CLOUDWATCH:

        return

    if dimensions is None:

        dimensions = []

    cloudwatch.put_metric_data(

        Namespace=METRIC_NAMESPACE,

        MetricData=[

            {

                "MetricName": metric_name,

                "Value": metric_value,

                "Unit": unit,

                "Timestamp": datetime.utcnow(),

                "Dimensions": dimensions

            }

        ]

    )

    logger.info(

        f"Published Metric : {metric_name}"

    )

###############################################################################
# Pipeline Success
###############################################################################

def publish_pipeline_success(
    pipeline_name
):
    """
    Publish successful pipeline execution.
    """

    publish_metric(

        metric_name="PipelineSuccess",

        metric_value=1,

        dimensions=[

            {

                "Name": "Pipeline",

                "Value": pipeline_name

            }

        ]

    )

###############################################################################
# Pipeline Failure
###############################################################################

def publish_pipeline_failure(
    pipeline_name
):
    """
    Publish failed pipeline execution.
    """

    publish_metric(

        metric_name="PipelineFailure",

        metric_value=1,

        dimensions=[

            {

                "Name": "Pipeline",

                "Value": pipeline_name

            }

        ]

    )

###############################################################################
# Task Duration
###############################################################################

def publish_task_duration(

    task_name,

    duration

):
    """
    Publish task execution duration.
    """

    publish_metric(

        metric_name="TaskDuration",

        metric_value=duration,

        unit="Seconds",

        dimensions=[

            {

                "Name": "Task",

                "Value": task_name

            }

        ]

    )

###############################################################################
# Records Processed
###############################################################################

def publish_records_processed(

    task_name,

    records

):
    """
    Publish processed record count.
    """

    publish_metric(

        metric_name="RecordsProcessed",

        metric_value=records,

        unit="Count",

        dimensions=[

            {

                "Name": "Task",

                "Value": task_name

            }

        ]

    )

###############################################################################
# Validation Failures
###############################################################################

def publish_validation_failure(

    validation_name

):
    """
    Publish validation failure metric.
    """

    publish_metric(

        metric_name="ValidationFailure",

        metric_value=1,

        dimensions=[

            {

                "Name": "Validation",

                "Value": validation_name

            }

        ]

    )

###############################################################################
# SLA Breach
###############################################################################

def publish_sla_breach(

    task_name

):
    """
    Publish SLA breach metric.
    """

    publish_metric(

        metric_name="SLABreach",

        metric_value=1,

        dimensions=[

            {

                "Name": "Task",

                "Value": task_name

            }

        ]

    )
