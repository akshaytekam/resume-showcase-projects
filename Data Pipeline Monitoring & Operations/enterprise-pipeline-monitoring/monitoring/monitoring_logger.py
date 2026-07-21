"""
===============================================================================

File Name
    monitoring_logger.py

Description
    Enterprise Monitoring Logger

Features

    • Centralized Logging
    • Console Logging
    • File Logging
    • Log Rotation
    • Standard Log Format

Author
    Enterprise Data Engineering Team

===============================================================================
"""

import logging

import os

from logging.handlers import RotatingFileHandler

from monitoring.config import (

    LOG_LEVEL,

    LOG_DIRECTORY,

    LOG_FILE_NAME,

    ENABLE_CONSOLE_LOGGING

)

###############################################################################
# Create Log Directory
###############################################################################

if not os.path.exists(

    LOG_DIRECTORY

):

    os.makedirs(

        LOG_DIRECTORY

    )

###############################################################################
# Logger
###############################################################################

logger = logging.getLogger(

    "pipeline_monitor"

)

logger.setLevel(

    LOG_LEVEL

)

###############################################################################
# Formatter
###############################################################################

LOG_FORMAT = (

    "%(asctime)s | "

    "%(levelname)-8s | "

    "%(name)s | "

    "%(message)s"

)

formatter = logging.Formatter(

    LOG_FORMAT

)

###############################################################################
# File Handler
###############################################################################

file_handler = RotatingFileHandler(

    filename=os.path.join(

        LOG_DIRECTORY,

        LOG_FILE_NAME

    ),

    maxBytes=10 * 1024 * 1024,

    backupCount=10

)

file_handler.setFormatter(

    formatter

)

logger.addHandler(

    file_handler

)

###############################################################################
# Console Handler
###############################################################################

if ENABLE_CONSOLE_LOGGING:

    console_handler = logging.StreamHandler()

    console_handler.setFormatter(

        formatter

    )

    logger.addHandler(

        console_handler

    )

###############################################################################
# Generic Log APIs
###############################################################################

def log_info(message):
    """
    Log INFO message.
    """

    logger.info(message)


def log_warning(message):
    """
    Log WARNING message.
    """

    logger.warning(message)


def log_error(message):
    """
    Log ERROR message.
    """

    logger.error(message)


###############################################################################
# Pipeline Logs
###############################################################################

def log_pipeline_start(
    pipeline_name,
    execution_date
):
    """
    Log pipeline start.
    """

    logger.info("=" * 80)

    logger.info(
        f"Pipeline Started : {pipeline_name}"
    )

    logger.info(
        f"Execution Date : {execution_date}"
    )

    logger.info("=" * 80)


def log_pipeline_end(
    pipeline_name,
    duration
):
    """
    Log successful pipeline completion.
    """

    logger.info("=" * 80)

    logger.info(
        f"Pipeline Completed : {pipeline_name}"
    )

    logger.info(
        f"Duration : {duration} Seconds"
    )

    logger.info("=" * 80)


###############################################################################
# Validation Logs
###############################################################################

def log_validation_result(
    validation_name,
    passed,
    failed
):
    """
    Log validation summary.
    """

    logger.info(
        f"Validation : {validation_name}"
    )

    logger.info(
        f"Passed Records : {passed}"
    )

    logger.info(
        f"Failed Records : {failed}"
    )


###############################################################################
# SLA Logs
###############################################################################

def log_sla_breach(
    task_name,
    expected_minutes,
    actual_minutes
):
    """
    Log SLA breach.
    """

    logger.warning("=" * 80)

    logger.warning(
        "SLA BREACH DETECTED"
    )

    logger.warning(
        f"Task : {task_name}"
    )

    logger.warning(
        f"SLA : {expected_minutes} Minutes"
    )

    logger.warning(
        f"Actual : {actual_minutes} Minutes"
    )

    logger.warning("=" * 80)


###############################################################################
# Exception Logs
###############################################################################

def log_exception(
    task_name,
    exception
):
    """
    Log task exception.
    """

    logger.exception(
        f"{task_name} Failed : {exception}"
    )


###############################################################################
# Health Score Logs
###############################################################################

def log_health_score(
    score,
    status
):
    """
    Log pipeline health score.
    """

    logger.info("=" * 80)

    logger.info(
        f"Pipeline Health Score : {score}"
    )

    logger.info(
        f"Health Status : {status}"
    )

    logger.info("=" * 80)
