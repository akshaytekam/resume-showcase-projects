"""
===============================================================================
File Name : primary_key_validation.py

Description:
    Validates Primary Key integrity for the Sales dataset.

Business Purpose
----------------
Primary Keys uniquely identify every business transaction.

If duplicate or NULL Primary Keys exist,

    • Duplicate Orders
    • Incorrect Revenue
    • Join Failures
    • Data Warehouse Corruption

may occur.

Validation Rules
----------------

✓ Primary Key must NOT be NULL

✓ Primary Key must be UNIQUE

✓ Generates rejected records

✓ Generates validation report

✓ Integrates with Airflow

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

from validation.validation_constants import PRIMARY_KEYS

from validation.validation_utils import (
    load_csv,
    validation_success,
    validation_failure
)


# ==============================================================================
# Helper Functions
# ==============================================================================

def get_primary_key_columns():

    """
    Returns Primary Key columns
    """

    return PRIMARY_KEYS["sales"]


def validate_null_primary_keys(df, pk_columns):

    """
    Returns rows containing NULL PK values.
    """

    return df[
        df[pk_columns].isnull().any(axis=1)
    ]


def validate_duplicate_primary_keys(df, pk_columns):

    """
    Returns duplicate PK rows.
    """

    return df[
        df.duplicated(
            subset=pk_columns,
            keep=False
        )
    ]


# ==============================================================================
# Main Validation
# ==============================================================================

def primary_key_validation():

    logger.info("=" * 80)
    logger.info("Starting Primary Key Validation")
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

    pk_columns = get_primary_key_columns()

    logger.info(f"Primary Key Columns : {pk_columns}")

    logger.info(f"Total Records : {total_records}")

    # ----------------------------------------------------------------------
    # NULL PK CHECK
    # ----------------------------------------------------------------------

    null_pk = validate_null_primary_keys(
        df,
        pk_columns
    )

    logger.info(
        f"NULL PK Records : {len(null_pk)}"
    )

    # ----------------------------------------------------------------------
    # DUPLICATE PK CHECK
    # ----------------------------------------------------------------------

    duplicate_pk = validate_duplicate_primary_keys(
        df,
        pk_columns
    )

    logger.info(
        f"Duplicate PK Records : {len(duplicate_pk)}"
    )

    # ----------------------------------------------------------------------
    # MERGE INVALID RECORDS
    # ----------------------------------------------------------------------

    invalid_records = pd.concat(
        [
            null_pk,
            duplicate_pk
        ]
    ).drop_duplicates()

    failed_records = len(invalid_records)

    # ----------------------------------------------------------------------
    # SUCCESS
    # ----------------------------------------------------------------------

    if failed_records == 0:

        logger.info(
            "Primary Key Validation Successful."
        )

        return validation_success(

            validation_name="Primary Key Validation",

            total_records=total_records,

            message="Primary Key validation successful."

        )

    # ----------------------------------------------------------------------
    # FAILURE
    # ----------------------------------------------------------------------

    logger.error(
        f"{failed_records} invalid primary key records found."
    )

    result = validation_failure(

        validation_name="Primary Key Validation",

        total_records=total_records,

        failed_records=failed_records,

        failed_df=invalid_records,

        message=f"{failed_records} invalid primary key records found."

    )

    raise Exception(result.message)


# ==============================================================================
# LOCAL TESTING
# ==============================================================================

if __name__ == "__main__":

    try:

        result = primary_key_validation()

        print(result)

    except Exception as ex:

        logger.exception(ex)

        raise
