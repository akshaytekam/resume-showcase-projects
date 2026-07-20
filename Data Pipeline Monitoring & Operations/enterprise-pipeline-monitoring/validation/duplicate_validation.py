"""
===============================================================================

File Name   : duplicate_validation.py

Description :
Enterprise Duplicate Validation Module.

Performs:

• Duplicate Record Detection
• Business Key Validation
• Duplicate Percentage Validation
• Threshold Validation
• Duplicate Report Generation

Author      : Enterprise Data Engineering Team

Version     : 2.0

===============================================================================
"""

from pyspark.sql.functions import col, count

from validation.validation_utils import (
    build_validation_result,
    calculate_percentage
)

from validation.validation_config import (
    schema,
    threshold
)


class DuplicateValidation:

    """
    Enterprise Duplicate Validation.
    """

    def __init__(self, dataframe):

        self.df = dataframe

        self.total_records = dataframe.count()

        self.business_keys = schema.PRIMARY_KEY

    # =========================================================================
    # Find Duplicate Records
    # =========================================================================

    def get_duplicate_dataframe(self):

        duplicate_df = (

            self.df

            .groupBy(*self.business_keys)

            .agg(

                count("*").alias("duplicate_count")

            )

            .filter(

                col("duplicate_count") > 1

            )

        )

        return duplicate_df

    # =========================================================================
    # Duplicate Count
    # =========================================================================

    def get_duplicate_count(self):

        duplicate_df = self.get_duplicate_dataframe()

        return duplicate_df.count()

    # =========================================================================
    # Duplicate Percentage
    # =========================================================================

    def get_duplicate_percentage(self):

        duplicate_count = self.get_duplicate_count()

        return calculate_percentage(

            duplicate_count,

            self.total_records

        )

    # =========================================================================
    # Print Duplicate Report
    # =========================================================================

    def print_report(self):

        duplicate_df = self.get_duplicate_dataframe()

        print("=" * 100)

        print("Duplicate Validation Report")

        print("=" * 100)

        duplicate_df.show(

            truncate=False

        )

        print("=" * 100)

    # =========================================================================
    # Threshold Validation
    # =========================================================================

    def validate_threshold(self):

        duplicate_percentage = self.get_duplicate_percentage()

        if duplicate_percentage > threshold.MAX_DUPLICATE_PERCENTAGE:

            raise Exception(

                f"Duplicate percentage "

                f"{duplicate_percentage}% "

                f"exceeds allowed "

                f"{threshold.MAX_DUPLICATE_PERCENTAGE}%"

            )

    # =========================================================================
    # Main Validation
    # =========================================================================

    def validate(self):

        duplicate_count = self.get_duplicate_count()

        self.print_report()

        self.validate_threshold()

        return build_validation_result(

            validation_name="Duplicate Validation",

            status="SUCCESS",

            records_checked=self.total_records,

            failed_records=duplicate_count,

            execution_time=0,

            message="Duplicate validation successful."

        )
