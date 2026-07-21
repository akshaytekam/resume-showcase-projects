"""
===============================================================================

File Name
    custom_operators.py

Description
    Enterprise Custom Airflow Operators

Features

    • Base Operator
    • Standard Logging
    • Execution Timer
    • CloudWatch Metrics
    • Exception Handling
    • Audit Logging

Author
    Enterprise Data Engineering Team

===============================================================================
"""

import logging
import time

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from plugins.cloudwatch_metrics import (
    publish_task_duration,
    publish_pipeline_failure
)

logger = logging.getLogger(__name__)

###############################################################################
# Enterprise Base Operator
###############################################################################

class EnterpriseBaseOperator(BaseOperator):
    """
    Base operator inherited by all
    enterprise ETL operators.
    """

    @apply_defaults
    def __init__(
        self,
        pipeline_name,
        *args,
        **kwargs
    ):

        super().__init__(

            *args,

            **kwargs

        )

        self.pipeline_name = pipeline_name

    ###########################################################################
    # Common Logger
    ###########################################################################

    def log(self, message):

        logger.info("=" * 80)

        logger.info(message)

        logger.info("=" * 80)

    ###########################################################################
    # Execution Timer
    ###########################################################################

    def start_timer(self):

        return time.time()

    def stop_timer(self, start):

        return round(

            time.time() - start,

            2

        )

    ###########################################################################
    # Execute Wrapper
    ###########################################################################

    def execute(self, context):

        """
        Every child operator overrides this.
        """

        raise NotImplementedError(

            "Child Operator must implement execute()."

        )

    ###########################################################################
    # Success Handler
    ###########################################################################

    def publish_success(

        self,

        task_name,

        duration

    ):

        publish_task_duration(

            task_name,

            duration

        )

    ###########################################################################
    # Failure Handler
    ###########################################################################

    def publish_failure(

        self,

        exception

    ):

        publish_pipeline_failure(

            self.pipeline_name

        )

        logger.error(

            str(exception)

        )


###############################################################################
# Bronze Load Operator
###############################################################################

class BronzeLoadOperator(
    EnterpriseBaseOperator
):
    """
    Enterprise Bronze Load Operator.
    """

    @apply_defaults
    def __init__(
        self,
        bronze_callable,
        *args,
        **kwargs
    ):

        super().__init__(
            *args,
            **kwargs
        )

        self.bronze_callable = bronze_callable

    ###########################################################################
    # Execute
    ###########################################################################

    def execute(
        self,
        context
    ):

        self.log(
            "Starting Bronze Load..."
        )

        start = self.start_timer()

        try:

            result = self.bronze_callable()

            duration = self.stop_timer(
                start
            )

            self.publish_success(

                task_name="Bronze Load",

                duration=duration

            )

            self.log(

                f"Bronze Load completed in {duration} seconds."

            )

            return result

        except Exception as ex:

            self.publish_failure(ex)

            raise


###############################################################################
# Silver Transform Operator
###############################################################################

class SilverTransformOperator(
    EnterpriseBaseOperator
):
    """
    Enterprise Silver Transformation Operator.
    """

    @apply_defaults
    def __init__(
        self,
        silver_callable,
        *args,
        **kwargs
    ):

        super().__init__(
            *args,
            **kwargs
        )

        self.silver_callable = silver_callable

    ###########################################################################
    # Execute
    ###########################################################################

    def execute(
        self,
        context
    ):

        self.log(
            "Starting Silver Transformation..."
        )

        start = self.start_timer()

        try:

            result = self.silver_callable()

            duration = self.stop_timer(
                start
            )

            self.publish_success(

                task_name="Silver Transform",

                duration=duration

            )

            self.log(

                f"Silver Transformation completed in {duration} seconds."

            )

            return result

        except Exception as ex:

            self.publish_failure(ex)

            raise


###############################################################################
# Gold Load Operator
###############################################################################

class GoldLoadOperator(
    EnterpriseBaseOperator
):
    """
    Enterprise Gold Load Operator.
    """

    @apply_defaults
    def __init__(
        self,
        gold_callable,
        *args,
        **kwargs
    ):

        super().__init__(
            *args,
            **kwargs
        )

        self.gold_callable = gold_callable

    ###########################################################################
    # Execute
    ###########################################################################

    def execute(
        self,
        context
    ):

        self.log(
            "Starting Gold Load..."
        )

        start = self.start_timer()

        try:

            result = self.gold_callable()

            duration = self.stop_timer(
                start
            )

            self.publish_success(

                task_name="Gold Load",

                duration=duration

            )

            self.log(

                f"Gold Load completed in {duration} seconds."

            )

            return result

        except Exception as ex:

            self.publish_failure(ex)

            raise


###############################################################################
# Validation Operator
###############################################################################

class ValidationOperator(
    EnterpriseBaseOperator
):
    """
    Enterprise Validation Operator.
    """

    @apply_defaults
    def __init__(
        self,
        validation_callable,
        *args,
        **kwargs
    ):

        super().__init__(
            *args,
            **kwargs
        )

        self.validation_callable = validation_callable

    ###########################################################################
    # Execute
    ###########################################################################

    def execute(
        self,
        context
    ):

        self.log(
            "Starting Data Validation..."
        )

        start = self.start_timer()

        try:

            result = self.validation_callable()

            duration = self.stop_timer(
                start
            )

            self.publish_success(

                task_name="Validation",

                duration=duration

            )

            self.log(

                f"Validation completed in {duration} seconds."

            )

            return result

        except Exception as ex:

            self.publish_failure(ex)

            raise


###############################################################################
# Archive Operator
###############################################################################

class ArchiveOperator(
    EnterpriseBaseOperator
):
    """
    Enterprise Archive Operator.
    """

    @apply_defaults
    def __init__(
        self,
        archive_callable,
        *args,
        **kwargs
    ):

        super().__init__(
            *args,
            **kwargs
        )

        self.archive_callable = archive_callable

    ###########################################################################
    # Execute
    ###########################################################################

    def execute(
        self,
        context
    ):

        self.log(
            "Starting File Archival..."
        )

        start = self.start_timer()

        try:

            result = self.archive_callable()

            duration = self.stop_timer(
                start
            )

            self.publish_success(

                task_name="Archive",

                duration=duration

            )

            self.log(

                f"Archive completed in {duration} seconds."

            )

            return result

        except Exception as ex:

            self.publish_failure(ex)

            raise
