"""
===============================================================================

File Name   : validation_runner.py

Description :
Main orchestration engine for the Enterprise Validation Framework.

Responsibilities
----------------
• Executes all validation modules
• Captures execution time
• Logs validation results
• Determines pipeline status
• Raises exception on critical failures

Author      : Enterprise Data Engineering Team

Version     : 2.0

===============================================================================
"""

from validation.validation_utils import ExecutionTimer

from validation.validation_logger import ValidationLogger

from validation.validation_config import validation_names

# Individual Validation Modules
from validation.file_validation import FileValidation
from validation.schema_validation import SchemaValidation
from validation.null_validation import NullValidation
from validation.duplicate_validation import DuplicateValidation
from validation.business_validation import BusinessValidation
from validation.reconciliation_validation import ReconciliationValidation


# =============================================================================
# Validation Runner
# =============================================================================

class ValidationRunner:

    """
    Executes all validations in the correct order.
    """

    def __init__(

            self,

            spark,

            dataframe

    ):

        self.spark = spark

        self.df = dataframe

        self.logger = ValidationLogger(spark)

        self.results = []

    # =========================================================================
    # Execute One Validation
    # =========================================================================

    def execute_validation(

            self,

            validator,

            validation_name,

            stop_on_failure=True

    ):

        timer = ExecutionTimer()

        timer.start()

        try:

            result = validator.validate()

            execution_time = timer.stop()

            self.logger.success(

                validation_name=validation_name,

                records_checked=result["records_checked"],

                execution_time=execution_time

            )

            result["status"] = "SUCCESS"

            result["execution_time"] = execution_time

            self.results.append(result)

            return True

        except Exception as ex:

            execution_time = timer.stop()

            self.logger.failure(

                validation_name=validation_name,

                records_checked=0,

                failed_records=0,

                execution_time=execution_time,

                exception=ex

            )

            self.results.append({

                "validation_name": validation_name,

                "status": "FAILED",

                "message": str(ex),

                "execution_time": execution_time

            })

            if stop_on_failure:

                raise

            return False

    # =========================================================================
    # Execute All Validations
    # =========================================================================

    def run(self):

        print("=" * 80)
        print("Starting Enterprise Validation Framework")
        print("=" * 80)

        # ---------------------------------------------------------
        # Critical Validations
        # ---------------------------------------------------------

        self.execute_validation(

            FileValidation(self.spark),

            validation_names.FILE_VALIDATION

        )

        self.execute_validation(

            SchemaValidation(self.df),

            validation_names.SCHEMA_VALIDATION

        )

        self.execute_validation(

            DuplicateValidation(self.df),

            validation_names.DUPLICATE_VALIDATION

        )

        self.execute_validation(

            BusinessValidation(self.df),

            validation_names.BUSINESS_VALIDATION

        )

        self.execute_validation(

            ReconciliationValidation(self.spark),

            validation_names.RECONCILIATION_VALIDATION

        )

        # ---------------------------------------------------------
        # Warning Validations
        # ---------------------------------------------------------

        self.execute_validation(

            NullValidation(self.df),

            validation_names.NULL_VALIDATION,

            stop_on_failure=False

        )

        print("=" * 80)
        print("Validation Framework Completed")
        print("=" * 80)

        return self.results

    # =========================================================================
    # Overall Pipeline Status
    # =========================================================================

    def pipeline_passed(self):

        failed = [

            x

            for x in self.results

            if x["status"] == "FAILED"

        ]

        return len(failed) == 0

    # =========================================================================
    # Print Summary
    # =========================================================================

    def print_summary(self):

        print("\n")

        print("=" * 80)

        print("Validation Summary")

        print("=" * 80)

        for result in self.results:

            print(

                f"{result['validation_name']}"

                f" ---> "

                f"{result['status']}"

            )

        print("=" * 80)

        if self.pipeline_passed():

            print("PIPELINE STATUS : PASSED")

        else:

            print("PIPELINE STATUS : FAILED")

        print("=" * 80)
