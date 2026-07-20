"""
===============================================================================

File Name   : null_validation.py

Description :
Enterprise NULL Validation Module.

Performs:

• NULL Count Validation
• NULL Percentage Validation
• Mandatory Column Validation
• Threshold Validation
• Detailed NULL Report

Author      : Enterprise Data Engineering Team

Version     : 2.0

===============================================================================
"""

from pyspark.sql.functions import col

from validation.validation_utils import (
    build_validation_result,
    calculate_percentage
)

# =============================================================================
# Column-Level NULL Thresholds
# =============================================================================

NULL_THRESHOLDS = {

    "sale_id": 0,

    "store_id": 0,

    "customer_id": 0,

    "product_id": 0,

    "sale_date": 0,

    "quantity": 0,

    "price": 0,

    "sales_amount": 0

}


class NullValidation:

    """
    Enterprise NULL validation.
    """

    def __init__(self, dataframe):

        self.df = dataframe

        self.total_records = dataframe.count()

        self.validation_summary = []

    # =========================================================================
    # NULL Count
    # =========================================================================

    def get_null_count(self, column_name):

        return (

            self.df

            .filter(

                col(column_name).isNull()

            )

            .count()

        )

    # =========================================================================
    # Validate One Column
    # =========================================================================

    def validate_column(self, column_name):

        null_count = self.get_null_count(column_name)

        null_percentage = calculate_percentage(

            null_count,

            self.total_records

        )

        threshold = NULL_THRESHOLDS.get(

            column_name,

            100

        )

        status = "PASS"

        if null_percentage > threshold:

            status = "FAIL"

        self.validation_summary.append({

            "column": column_name,

            "null_count": null_count,

            "null_percentage": null_percentage,

            "allowed_percentage": threshold,

            "status": status

        })

    # =========================================================================
    # Validate All Columns
    # =========================================================================

    def validate_all_columns(self):

        for column_name in self.df.columns:

            self.validate_column(column_name)

    # =========================================================================
    # Print Validation Report
    # =========================================================================

    def print_report(self):

        print("=" * 100)

        print("NULL Validation Report")

        print("=" * 100)

        print(

            f"{'Column':20}"

            f"{'NULLs':>10}"

            f"{'NULL %':>12}"

            f"{'Allowed %':>12}"

            f"{'Status':>12}"

        )

        print("-" * 100)

        for row in self.validation_summary:

            print(

                f"{row['column']:20}"

                f"{row['null_count']:>10}"

                f"{row['null_percentage']:>12.2f}"

                f"{row['allowed_percentage']:>12}"

                f"{row['status']:>12}"

            )

        print("=" * 100)

    # =========================================================================
    # Validation Result
    # =========================================================================

    def has_failures(self):

        return any(

            row["status"] == "FAIL"

            for row in self.validation_summary

        )

    # =========================================================================
    # Main Validation
    # =========================================================================

    def validate(self):

        self.validate_all_columns()

        self.print_report()

        failed_columns = [

            row

            for row in self.validation_summary

            if row["status"] == "FAIL"

        ]

        if failed_columns:

            raise Exception(

                f"NULL validation failed for "

                f"{len(failed_columns)} column(s)."

            )

        return build_validation_result(

            validation_name="NULL Validation",

            status="SUCCESS",

            records_checked=self.total_records,

            failed_records=0,

            execution_time=0,

            message="NULL validation successful."

        )
