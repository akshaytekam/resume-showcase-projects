"""
===============================================================================
File Name : null_validation.py

Description:
    Performs NULL validation on mandatory columns of the Sales dataset.

Business Purpose:
    NULL values in mandatory business columns can lead to
    incorrect reporting, failed joins, inaccurate aggregations,
    and downstream ETL failures.

Validation Rules
----------------
✔ Mandatory columns cannot contain NULL values
✔ Generates rejected records
✔ Produces validation statistics
✔ Logs detailed execution information

Author:
    Enterprise Data Engineering Team

Version:
    1.0
===============================================================================
"""

from pathlib import Path
import pandas as pd

from utils.logger import logger
from utils.config_loader import load_config

from validation.validation_constants import NON_NULL_COLUMNS

from validation.validation_utils import (
    load_csv,
    validation_success,
    validation_failure
)


# =============================================================================
# Helper Functions
# =============================================================================

def get_mandatory_columns():
    """
    Returns mandatory columns for Sales dataset.
    """

    return NON_NULL_COLUMNS["sales"]


def calculate_null_statistics(df, columns):
    """
    Calculates NULL statistics.

    Parameters
    ----------
    df : DataFrame

    columns : list

    Returns
    -------
    DataFrame
    """

    statistics = []

    total_records = len(df)

    for column in columns:

        null_count = df[column].isnull().sum()

        percentage = round((null_count / total_records) * 100, 2)

        statistics.append({

            "Column": column,

            "Null Count": null_count,

            "Null Percentage": percentage

        })

    return pd.DataFrame(statistics)


# =============================================================================
# Main Validation
# =============================================================================

def null_validation():

    logger.info("=" * 80)
    logger.info("Starting NULL Validation")
    logger.info("=" * 80)

    config = load_config()

    sales_file = (
        Path(config["landing_path"])
        / "sales"
        / "sales_2026-07-01.csv"
    )

    if not sales_file.exists():

        raise FileNotFoundError(
            f"{sales_file} not found."
        )

    df = load_csv(str(sales_file))

    total_records = len(df)

    mandatory_columns = get_mandatory_columns()

    logger.info(f"Total Records : {total_records}")

    logger.info(
        f"Mandatory Columns : {mandatory_columns}"
    )

    # -------------------------------------------------------------------------
    # NULL Statistics
    # -------------------------------------------------------------------------

    statistics = calculate_null_statistics(
        df,
        mandatory_columns
    )

    logger.info("NULL Statistics")

    logger.info("\n" + statistics.to_string(index=False))

    # -------------------------------------------------------------------------
    # Find Invalid Records
    # -------------------------------------------------------------------------

    invalid_records = df[
        df[mandatory_columns].isnull().any(axis=1)
    ]

    failed_records = len(invalid_records)

    # -------------------------------------------------------------------------
    # SUCCESS
    # -------------------------------------------------------------------------

    if failed_records == 0:

        logger.info(
            "NULL Validation Successful."
        )

        return validation_success(

            validation_name="NULL Validation",

            total_records=total_records,

            message="No NULL values found in mandatory columns."

        )

    # -------------------------------------------------------------------------
    # FAILURE
    # -------------------------------------------------------------------------

    logger.error(
        f"NULL Validation Failed. "
        f"{failed_records} invalid records detected."
    )

    result = validation_failure(

        validation_name="NULL Validation",

        total_records=total_records,

        failed_records=failed_records,

        failed_df=invalid_records,

        message=f"{failed_records} records contain NULL values."

    )

    # Save NULL statistics

    statistics.to_csv(

        "validation/reports/null_statistics.csv",

        index=False

    )

    raise Exception(result.message)


# =============================================================================
# Local Testing
# =============================================================================

if __name__ == "__main__":

    try:

        result = null_validation()

        print(result)

    except Exception as ex:

        logger.exception(ex)

        raise
