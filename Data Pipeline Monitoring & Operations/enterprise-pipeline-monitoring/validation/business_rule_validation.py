"""
===============================================================================
File Name : business_rule_validation.py

Description:
    Performs Business Rule Validation on Sales dataset.

Business Rules
--------------
1. Quantity must be greater than zero
2. Price must be greater than zero
3. Sale Date cannot be a future date

Author:
Enterprise Data Engineering Team

Version:
1.0
===============================================================================
"""

from pathlib import Path
from datetime import datetime

import pandas as pd

from utils.logger import logger
from utils.config_loader import load_config

from validation.validation_utils import (
    load_csv,
    validation_success,
    validation_failure
)


# ==============================================================================
# BUSINESS RULE FUNCTIONS
# ==============================================================================

def validate_quantity(df: pd.DataFrame) -> pd.DataFrame:
    """
    Quantity must be greater than zero.
    """
    return df[df["quantity"] <= 0]


def validate_price(df: pd.DataFrame) -> pd.DataFrame:
    """
    Price must be greater than zero.
    """
    return df[df["price"] <= 0]


def validate_future_date(df: pd.DataFrame) -> pd.DataFrame:
    """
    Sale date should not be greater than today's date.
    """

    working_df = df.copy()

    working_df["sale_date"] = pd.to_datetime(
        working_df["sale_date"],
        errors="coerce"
    )

    today = pd.Timestamp(datetime.today().date())

    return working_df[
        working_df["sale_date"] > today
    ]


# ==============================================================================
# MAIN VALIDATION
# ==============================================================================

def business_validation():

    logger.info("=" * 80)
    logger.info("Starting Business Rule Validation")
    logger.info("=" * 80)

    config = load_config()

    sales_file = (
        Path(config["landing_path"])
        / "sales"
        / "sales_2026-07-01.csv"
    )

    if not sales_file.exists():
        raise FileNotFoundError(f"{sales_file} not found.")

    df = load_csv(str(sales_file))

    total_records = len(df)

    logger.info(f"Total Records : {total_records}")

    # --------------------------------------------------------------------------
    # Rule 1
    # --------------------------------------------------------------------------

    invalid_quantity = validate_quantity(df)

    logger.info(
        f"Quantity Validation Failed Records : {len(invalid_quantity)}"
    )

    # --------------------------------------------------------------------------
    # Rule 2
    # --------------------------------------------------------------------------

    invalid_price = validate_price(df)

    logger.info(
        f"Price Validation Failed Records : {len(invalid_price)}"
    )

    # --------------------------------------------------------------------------
    # Rule 3
    # --------------------------------------------------------------------------

    invalid_date = validate_future_date(df)

    logger.info(
        f"Future Date Validation Failed Records : {len(invalid_date)}"
    )

    # --------------------------------------------------------------------------
    # Combine Invalid Records
    # --------------------------------------------------------------------------

    invalid_records = pd.concat(
        [
            invalid_quantity,
            invalid_price,
            invalid_date
        ]
    ).drop_duplicates()

    failed_records = len(invalid_records)

    # --------------------------------------------------------------------------
    # SUCCESS
    # --------------------------------------------------------------------------

    if failed_records == 0:

        logger.info("Business Rule Validation Successful.")

        return validation_success(

            validation_name="Business Rule Validation",

            total_records=total_records,

            message="All business rules passed."

        )

    # --------------------------------------------------------------------------
    # FAILURE
    # --------------------------------------------------------------------------

    logger.error(
        f"{failed_records} business rule violations found."
    )

    result = validation_failure(

        validation_name="Business Rule Validation",

        total_records=total_records,

        failed_records=failed_records,

        failed_df=invalid_records,

        message=f"{failed_records} business rule violations found."

    )

    raise Exception(result.message)


# ==============================================================================
# LOCAL TESTING
# ==============================================================================

if __name__ == "__main__":

    try:

        result = business_validation()

        print(result)

    except Exception as ex:

        logger.exception(ex)

        raise
