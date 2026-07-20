"""
===============================================================================

File Name   : validation_logger.py

Description :
Centralized logger for Enterprise Data Validation Framework.

Supports:
    • Console Logging
    • File Logging
    • Delta Audit Logging
    • Future CloudWatch Integration

Author      : Enterprise Data Engineering Team

Version     : 2.0

===============================================================================
"""

import logging
import uuid
from datetime import datetime

from pyspark.sql import Row

from validation.validation_config import (
    logging_config,
    storage,
    pipeline
)

# =============================================================================
# Configure Python Logger
# =============================================================================

logging.basicConfig(

    level=getattr(logging, logging_config.LOG_LEVEL),

    format=logging_config.LOG_FORMAT

)

logger = logging.getLogger("ValidationLogger")


# =============================================================================
# Validation Logger Class
# =============================================================================

class ValidationLogger:

    """
    Enterprise Validation Logger.
    """

    def __init__(

            self,

            spark

    ):

        self.spark = spark

        self.execution_id = str(uuid.uuid4())

        self.pipeline_name = pipeline.PIPELINE_NAME

    # =========================================================================
    # Console Logging
    # =========================================================================

    def info(self, message):

        logger.info(message)

    def warning(self, message):

        logger.warning(message)

    def error(self, message):

        logger.error(message)

    # =========================================================================
    # Validation Result
    # =========================================================================

    def log_validation(

            self,

            validation_name,

            status,

            records_checked,

            failed_records,

            execution_time,

            message

    ):

        result = {

            "execution_id": self.execution_id,

            "pipeline_name": self.pipeline_name,

            "validation_name": validation_name,

            "status": status,

            "records_checked": records_checked,

            "failed_records": failed_records,

            "execution_time_seconds": execution_time,

            "message": message,

            "log_timestamp": str(datetime.now())

        }

        logger.info(result)

        return result

    # =========================================================================
    # Delta Audit Table
    # =========================================================================

    def write_audit_table(

            self,

            validation_result

    ):

        """
        Writes validation result to Delta Audit Table.
        """

        audit_df = self.spark.createDataFrame([

            Row(**validation_result)

        ])

        (

            audit_df.write

            .format("delta")

            .mode("append")

            .save(storage.AUDIT_PATH)

        )

        logger.info(

            "Validation result written to audit table."

        )

    # =========================================================================
    # Log Success
    # =========================================================================

    def success(

            self,

            validation_name,

            records_checked,

            execution_time

    ):

        result = self.log_validation(

            validation_name=validation_name,

            status="SUCCESS",

            records_checked=records_checked,

            failed_records=0,

            execution_time=execution_time,

            message="Validation completed successfully."

        )

        self.write_audit_table(result)

    # =========================================================================
    # Log Failure
    # =========================================================================

    def failure(

            self,

            validation_name,

            records_checked,

            failed_records,

            execution_time,

            exception

    ):

        result = self.log_validation(

            validation_name=validation_name,

            status="FAILED",

            records_checked=records_checked,

            failed_records=failed_records,

            execution_time=execution_time,

            message=str(exception)

        )

        self.write_audit_table(result)

    # =========================================================================
    # Future Integration Hooks
    # =========================================================================

    def publish_cloudwatch(self, validation_result):
        """
        Placeholder for AWS CloudWatch metrics.
        """
        pass

    def publish_grafana(self, validation_result):
        """
        Placeholder for Grafana/Prometheus metrics.
        """
        pass

    def send_email_alert(self, validation_result):
        """
        Placeholder for Email Alert integration.
        """
        pass

    def send_slack_alert(self, validation_result):
        """
        Placeholder for Slack integration.
        """
        pass
