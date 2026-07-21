"""
===============================================================================

File Name
    custom_callbacks.py

Description
    Enterprise Airflow Task Callbacks

Implements

    • Success Callback
    • Failure Callback
    • Execution Metadata Collection
    • Logging
    • CloudWatch Integration (Hook)
    • Email Integration (Hook)
    • Audit Logging (Hook)

Author
    Enterprise Data Engineering Team

===============================================================================
"""

from datetime import datetime

import logging

from include.airflow_config import (
    PIPELINE_NAME,
    ENABLE_CLOUDWATCH,
    ENABLE_EMAIL_ALERT
)

logger = logging.getLogger(__name__)

###############################################################################
# Helper Function
###############################################################################

def get_task_metadata(context):
    """
    Extract useful Airflow execution metadata.
    """

    task_instance = context["task_instance"]

    dag = context["dag"]

    metadata = {

        "pipeline_name": PIPELINE_NAME,

        "dag_id": dag.dag_id,

        "task_id": task_instance.task_id,

        "run_id": context.get("run_id"),

        "execution_date": str(context.get("logical_date")),

        "try_number": task_instance.try_number,

        "hostname": task_instance.hostname,

        "operator": task_instance.operator,

        "log_url": task_instance.log_url,

        "start_date": str(task_instance.start_date),

        "end_date": str(task_instance.end_date),

        "duration": task_instance.duration

    }

    return metadata

###############################################################################
# Logging Helper
###############################################################################

def log_metadata(metadata):
    """
    Write execution metadata to Airflow logs.
    """

    logger.info("=" * 80)

    logger.info("Pipeline Execution Metadata")

    logger.info("=" * 80)

    for key, value in metadata.items():

        logger.info(f"{key:<20}: {value}")

    logger.info("=" * 80)

###############################################################################
# CloudWatch Hook
###############################################################################

def publish_cloudwatch_metric(
    metadata,
    status
):
    """
    Publish execution metrics.

    Future implementation:
    AWS CloudWatch
    """

    if not ENABLE_CLOUDWATCH:

        return

    logger.info(

        f"Publishing CloudWatch Metric : {status}"

    )

###############################################################################
# Email Hook
###############################################################################

def send_email_notification(
    metadata,
    status,
    error_message=None
):
    """
    Send pipeline notification.

    Future implementation:
    SMTP / SES / Outlook
    """

    if not ENABLE_EMAIL_ALERT:

        return

    logger.info(

        f"Sending Email Notification : {status}"

    )

###############################################################################
# Audit Hook
###############################################################################

def write_audit_log(
    metadata,
    status,
    error_message=None
):
    """
    Future Audit Table.

    Example

    execution_id
    dag
    task
    duration
    status

    """

    logger.info(

        f"Writing Audit Record : {status}"

    )

###############################################################################
# Success Callback
###############################################################################

def on_success_callback(context):
    """
    Called whenever an Airflow task
    finishes successfully.
    """

    metadata = get_task_metadata(

        context

    )

    metadata["status"] = "SUCCESS"

    metadata["completion_time"] = str(

        datetime.utcnow()

    )

    log_metadata(

        metadata

    )

    publish_cloudwatch_metric(

        metadata,

        "SUCCESS"

    )

    send_email_notification(

        metadata,

        "SUCCESS"

    )

    write_audit_log(

        metadata,

        "SUCCESS"

    )

    logger.info(

        "Task completed successfully."

    )

###############################################################################
# Failure Callback
###############################################################################

def on_failure_callback(context):
    """
    Called whenever an Airflow task fails.
    """

    metadata = get_task_metadata(

        context

    )

    metadata["status"] = "FAILED"

    metadata["completion_time"] = str(

        datetime.utcnow()

    )

    exception = context.get(

        "exception"

    )

    error_message = (

        str(exception)

        if exception

        else "Unknown Error"

    )

    metadata["error_message"] = error_message

    log_metadata(

        metadata

    )

    logger.error(

        "=" * 80

    )

    logger.error(

        "PIPELINE FAILURE"

    )

    logger.error(

        f"Task : {metadata['task_id']}"

    )

    logger.error(

        f"Error : {error_message}"

    )

    logger.error(

        "=" * 80

    )

    publish_cloudwatch_metric(

        metadata,

        "FAILED"

    )

    send_email_notification(

        metadata,

        "FAILED",

        error_message

    )

    write_audit_log(

        metadata,

        "FAILED",

        error_message

    )

