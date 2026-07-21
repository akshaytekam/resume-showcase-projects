"""
===============================================================================

File Name
    dag_utils.py

Description
    Enterprise Airflow Utility Functions

Features

    • Execution Context Helpers
    • Processing Date Utilities
    • XCom Utilities
    • Logging Helpers
    • Execution Timer
    • Runtime Parameters

Author
    Enterprise Data Engineering Team

===============================================================================
"""

from datetime import datetime
import logging
import time

logger = logging.getLogger(__name__)

###############################################################################
# Processing Date
###############################################################################

def get_processing_date(context):
    """
    Return Airflow logical processing date.
    """

    return context["logical_date"].strftime("%Y-%m-%d")

###############################################################################
# Run ID
###############################################################################

def get_run_id(context):
    """
    Return DAG Run ID.
    """

    return context["run_id"]

###############################################################################
# Execution Timestamp
###############################################################################

def get_execution_timestamp():
    """
    Current UTC timestamp.
    """

    return datetime.utcnow().strftime(

        "%Y-%m-%d %H:%M:%S"

    )

###############################################################################
# Logger
###############################################################################

def log_pipeline_event(message):
    """
    Standard logging helper.
    """

    logger.info("=" * 80)

    logger.info(message)

    logger.info("=" * 80)

###############################################################################
# Timer Start
###############################################################################

def start_timer():
    """
    Start execution timer.
    """

    return time.time()

###############################################################################
# Timer End
###############################################################################

def stop_timer(start_time):
    """
    Return execution duration.
    """

    return round(

        time.time() - start_time,

        2

    )

###############################################################################
# XCom Push
###############################################################################

def push_xcom(
    context,
    key,
    value
):
    """
    Push value to XCom.
    """

    task_instance = context["ti"]

    task_instance.xcom_push(

        key=key,

        value=value

    )

    logger.info(

        f"XCom Push : {key} = {value}"

    )

###############################################################################
# XCom Pull
###############################################################################

def pull_xcom(
    context,
    task_ids,
    key
):
    """
    Read value from XCom.
    """

    task_instance = context["ti"]

    value = task_instance.xcom_pull(

        task_ids=task_ids,

        key=key

    )

    logger.info(

        f"XCom Pull : {key} = {value}"

    )

    return value

###############################################################################
# Runtime Parameters
###############################################################################

def get_runtime_parameter(
    context,
    parameter_name,
    default_value=None
):
    """
    Read DAG Run configuration parameter.
    """

    dag_run = context.get("dag_run")

    if dag_run and dag_run.conf:

        return dag_run.conf.get(

            parameter_name,

            default_value

        )

    return default_value

###############################################################################
# Environment
###############################################################################

def get_environment():
    """
    Return execution environment.
    """

    import os

    return os.getenv(

        "ENVIRONMENT",

        "DEV"

    )

###############################################################################
# Task State
###############################################################################

def get_task_state(context):
    """
    Current Airflow task state.
    """

    return context["task_instance"].state

###############################################################################
# Exception Formatter
###############################################################################

def format_exception(exception):
    """
    Format exception for logs/emails.
    """

    if exception is None:

        return "Unknown Error"

    return f"{type(exception).__name__}: {exception}"

###############################################################################
# Execution Summary
###############################################################################

def build_execution_summary(
    metadata
):
    """
    Build pipeline execution summary.
    """

    summary = {

        "Pipeline": metadata.get(

            "pipeline_name"

        ),

        "DAG": metadata.get(

            "dag_id"

        ),

        "Task": metadata.get(

            "task_id"

        ),

        "Run ID": metadata.get(

            "run_id"

        ),

        "Status": metadata.get(

            "status"

        ),

        "Duration": metadata.get(

            "duration"

        ),

        "Execution Date": metadata.get(

            "execution_date"

        )

    }

    return summary
