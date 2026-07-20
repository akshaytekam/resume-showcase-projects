"""
===============================================================================
File Name : run_all_validations.py

Description:
    Executes all validation modules in sequence.

Business Purpose
----------------
This module acts as the Validation Orchestrator.

Responsibilities

✓ Execute every validation

✓ Continue execution even if one validation fails

✓ Collect validation results

✓ Generate consolidated Data Quality Report

✓ Return overall pipeline status

Author:
Enterprise Data Engineering Team

Version:
1.0
===============================================================================
"""

from utils.logger import logger

from validation.data_quality_report import DataQualityReport

from validation.file_arrival_validation import file_validation
from validation.schema_validation import schema_validation
from validation.duplicate_validation import duplicate_validation
from validation.null_validation import null_validation
from validation.primary_key_validation import primary_key_validation
from validation.foreign_key_validation import foreign_key_validation
from validation.business_rule_validation import business_validation


# ==============================================================================
# VALIDATION RUNNER
# ==============================================================================

def run_all_validations():

    logger.info("=" * 80)
    logger.info("STARTING ENTERPRISE DATA VALIDATION FRAMEWORK")
    logger.info("=" * 80)

    report = DataQualityReport()

    validation_functions = [

        file_validation,

        schema_validation,

        duplicate_validation,

        null_validation,

        primary_key_validation,

        foreign_key_validation,

        business_validation

    ]

    failed_validations = []

    successful_validations = []

    # --------------------------------------------------------------------------
    # Execute Validations
    # --------------------------------------------------------------------------

    for validation in validation_functions:

        validation_name = validation.__name__

        logger.info(f"Executing : {validation_name}")

        try:

            result = validation()

            report.add_result(result)

            successful_validations.append(validation_name)

            logger.info(f"{validation_name} PASSED")

        except Exception as ex:

            logger.exception(ex)

            failed_validations.append(validation_name)

            logger.error(f"{validation_name} FAILED")

    # --------------------------------------------------------------------------
    # Generate Final Report
    # --------------------------------------------------------------------------

    report.generate_csv()

    logger.info("=" * 80)
    logger.info("VALIDATION SUMMARY")
    logger.info("=" * 80)

    logger.info(
        f"Successful Validations : {len(successful_validations)}"
    )

    logger.info(
        f"Failed Validations : {len(failed_validations)}"
    )

    logger.info(
        f"Passed : {successful_validations}"
    )

    logger.info(
        f"Failed : {failed_validations}"
    )

    # --------------------------------------------------------------------------
    # Final Decision
    # --------------------------------------------------------------------------

    if len(failed_validations) > 0:

        raise Exception(

            "Pipeline Validation Failed."

        )

    logger.info(

        "All validations completed successfully."

    )

    return True


# ==============================================================================
# LOCAL EXECUTION
# ==============================================================================

if __name__ == "__main__":

    run_all_validations()
