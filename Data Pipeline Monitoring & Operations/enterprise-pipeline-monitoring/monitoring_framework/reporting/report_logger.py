"""
===============================================================================

File Name
    report_logger.py

Description
    Enterprise Reporting Logger

Features

    • Report Generation Logs
    • Export Logs
    • Scheduler Logs
    • Error Logging
    • Performance Logging

Author
    Enterprise Data Engineering Team

===============================================================================
"""

import logging

from pathlib import Path

from monitoring_framework.config.config import (

    LOG_DIRECTORY,

    LOG_LEVEL,

    ENABLE_CONSOLE_LOGGING,

    ENABLE_FILE_LOGGING

)

###############################################################################
# Log Directory
###############################################################################

Path(

    LOG_DIRECTORY

).mkdir(

    parents=True,

    exist_ok=True

)

###############################################################################
# Logger Configuration
###############################################################################

LOGGER_NAME = "report_logger"

LOG_FILE = (

    Path(LOG_DIRECTORY)

    /

    "reporting.log"

)

logger = logging.getLogger(

    LOGGER_NAME

)

logger.setLevel(

    getattr(

        logging,

        LOG_LEVEL

    )

)

###############################################################################
# Prevent Duplicate Handlers
###############################################################################

if not logger.handlers:

    formatter = logging.Formatter(

        "%(asctime)s | "

        "%(levelname)s | "

        "%(message)s"

    )

    ###########################################################################
    # File Handler
    ###########################################################################

    if ENABLE_FILE_LOGGING:

        file_handler = logging.FileHandler(

            LOG_FILE

        )

        file_handler.setFormatter(

            formatter

        )

        logger.addHandler(

            file_handler

        )

    ###########################################################################
    # Console Handler
    ###########################################################################

    if ENABLE_CONSOLE_LOGGING:

        console_handler = logging.StreamHandler()

        console_handler.setFormatter(

            formatter

        )

        logger.addHandler(

            console_handler
        )


from datetime import datetime
import traceback

###############################################################################
# Report Generation Started
###############################################################################

def log_report_started(
    report_name,
    report_type="DAILY"
):
    """
    Log report generation start.
    """

    logger.info(

        f"[REPORT STARTED] "

        f"Name={report_name} | "

        f"Type={report_type} | "

        f"StartTime={datetime.utcnow()}"

    )


###############################################################################
# Report Generation Completed
###############################################################################

def log_report_completed(
    report_name,
    duration_seconds
):
    """
    Log successful report generation.
    """

    logger.info(

        f"[REPORT COMPLETED] "

        f"Name={report_name} | "

        f"Duration={duration_seconds:.2f}s"

    )


###############################################################################
# Report Export
###############################################################################

def log_report_exported(
    report_name,
    export_format,
    output_path
):
    """
    Log report export details.
    """

    logger.info(

        f"[REPORT EXPORTED] "

        f"Name={report_name} | "

        f"Format={export_format} | "

        f"Output={output_path}"

    )


###############################################################################
# Scheduler Trigger
###############################################################################

def log_scheduler_trigger(
    scheduler_name,
    report_name
):
    """
    Log scheduled report execution.
    """

    logger.info(

        f"[SCHEDULER] "

        f"{scheduler_name} triggered "

        f"{report_name}"

    )


###############################################################################
# Dashboard Refresh
###############################################################################

def log_dashboard_refresh(
    dashboard_name
):
    """
    Log dashboard refresh event.
    """

    logger.info(

        f"[DASHBOARD REFRESH] "

        f"{dashboard_name}"

    )


###############################################################################
# Report Failure
###############################################################################

def log_report_failed(
    report_name,
    error
):
    """
    Log report generation failure.
    """

    logger.error(

        f"[REPORT FAILED] "

        f"Name={report_name} | "

        f"Error={error}"

    )


###############################################################################
# Exception Logger
###############################################################################

def log_exception(
    report_name,
    exception
):
    """
    Log exception with stack trace.
    """

    logger.error(

        f"[EXCEPTION] "

        f"Report={report_name} | "

        f"Exception={exception}"

    )

    logger.error(

        traceback.format_exc()

    )


###############################################################################
# Performance Logger
###############################################################################

def log_performance(
    operation,
    duration_seconds
):
    """
    Log execution performance.
    """

    logger.info(

        f"[PERFORMANCE] "

        f"Operation={operation} | "

        f"Duration={duration_seconds:.2f}s"

    )


###############################################################################
# Audit Logger
###############################################################################

def log_audit(
    report_name,
    generated_by,
    report_format
):
    """
    Log report audit information.
    """

    logger.info(

        f"[AUDIT] "

        f"Report={report_name} | "

        f"GeneratedBy={generated_by} | "

        f"Format={report_format} | "

        f"Timestamp={datetime.utcnow()}"

    )


###############################################################################
# Execution Summary
###############################################################################

def log_execution_summary(
    report_name,
    status,
    duration_seconds
):
    """
    Log final execution summary.
    """

    logger.info(

        f"[SUMMARY] "

        f"Report={report_name} | "

        f"Status={status} | "

        f"Duration={duration_seconds:.2f}s"

    )


###############################################################################
# Logger Shutdown
###############################################################################

def shutdown_logger():
    """
    Flush and close all logging handlers.
    """

    for handler in logger.handlers[:]:

        handler.flush()

        handler.close()

        logger.removeHandler(handler)

    logging.shutdown()
