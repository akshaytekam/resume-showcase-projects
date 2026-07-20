"""
===============================================================================

File Name   : business_validation.py

Description :
Enterprise Business Rule Validation Module.

Performs:

• Quantity Validation
• Price Validation
• Revenue Validation
• Future Date Validation
• Business Rule Reporting

Author      : Enterprise Data Engineering Team

Version     : 2.0

===============================================================================
"""

from pyspark.sql.functions import (
    col,
    current_date,
    round
)

from validation.validation_utils import (
    build_validation_result
)

from validation.validation_config import (
    business
)


class BusinessValidation:

    """
    Enterprise Business Validation.
    """

    def __init__(self, dataframe):

        self.df = dataframe

        self.total_records = dataframe.count()

        self.failed_records = 0

    # =====================================================================
    # Quantity Validation
    # =====================================================================

    def validate_quantity(self):

        invalid = (

            self.df

            .filter(

                col("quantity") < business.MIN_QUANTITY

            )

        )

        count = invalid.count()

        if count > 0:

            print(f"Invalid Quantity Records : {count}")

        self.failed_records += count

    # =====================================================================
    # Price Validation
    # =====================================================================

    def validate_price(self):

        invalid = (

            self.df

            .filter(

                (col("price") < business.MIN_PRICE)

                |

                (col("price") > business.MAX_PRICE)

            )

        )

        count = invalid.count()

        if count > 0:

            print(f"Invalid Price Records : {count}")

        self.failed_records += count

    # =====================================================================
    # Sales Amount Validation
    # =====================================================================

    def validate_sales_amount(self):

        invalid = (

            self.df

            .filter(

                round(

                    col("quantity") * col("price"),

                    2

                )

                !=

                round(

                    col("sales_amount"),

                    2

                )

            )

        )

        count = invalid.count()

        if count > 0:

            print(f"Invalid Revenue Records : {count}")

        self.failed_records += count

    # =====================================================================
    # Future Date Validation
    # =====================================================================

    def validate_future_date(self):

        if business.ALLOW_FUTURE_DATE:

            return

        invalid = (

            self.df

            .filter(

                col("sale_date") > current_date()

            )

        )

        count = invalid.count()

        if count > 0:

            print(f"Future Date Records : {count}")

        self.failed_records += count

    # =====================================================================
    # Main Validation
    # =====================================================================

    def validate(self):

        self.validate_quantity()

        self.validate_price()

        self.validate_sales_amount()

        self.validate_future_date()

        if self.failed_records > 0:

            raise Exception(

                f"Business Validation Failed. "

                f"Invalid Records : {self.failed_records}"

            )

        return build_validation_result(

            validation_name="Business Validation",

            status="SUCCESS",

            records_checked=self.total_records,

            failed_records=0,

            execution_time=0,

            message="Business validation successful."

        )
