"""
===============================================================================
File Name : duplicate_validation.py

Description:
    This module validates duplicate records in the input dataset.

Business Purpose:
    Duplicate records can lead to incorrect aggregations,
    inaccurate reporting, duplicate customer billing,
    duplicate order processing, and inconsistent analytics.

    This validation checks:

        ✓ Complete duplicate rows
        ✓ Duplicate Primary Keys

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
    validation_failure,
)


def duplicate_validation():
    """
    Executes duplicate validation on Sales dataset.

    Returns
    -------
    ValidationResult
        SUCCESS if no duplicates found

    Raises
    ------
    Exception
        If duplicate records exist.
    """

    logger.info("=" * 80)
    logger.info("Starting Duplicate Validation")
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

    # -------------------------------------------------------------------------
    # STEP 1 : Complete Duplicate Rows
    # -------------------------------------------------------------------------

    duplicate_rows = df[df.duplicated(keep=False)]

    duplicate_row_count = len(duplicate_rows)

    logger.info(f"Duplicate Rows Found : {duplicate_row_count}")

    # -------------------------------------------------------------------------
    # STEP 2 : Duplicate Primary Keys
    # -------------------------------------------------------------------------

    pk_columns = PRIMARY_KEYS["sales"]

    duplicate_pk = df[df.duplicated(subset=pk_columns, keep=False)]

    duplicate_pk_count = len(duplicate_pk)

    logger.info(f"Duplicate Primary Keys : {duplicate_pk_count}")

    # -------------------------------------------------------------------------
    # Merge Duplicate Records
    # -------------------------------------------------------------------------

    duplicate_records = pd.concat(
        [duplicate_rows, duplicate_pk]
    ).drop_duplicates()

    failed_records = len(duplicate_records)

    # -------------------------------------------------------------------------
    # SUCCESS
    # -------------------------------------------------------------------------

    if failed_records == 0:

        logger.info("Duplicate Validation Successful.")

        return validation_success(
            validation_name="Duplicate Validation",
            total_records=total_records,
            message="No duplicate records found."
        )

    # -------------------------------------------------------------------------
    # FAILURE
    # -------------------------------------------------------------------------

    logger.error(
        f"Duplicate Validation Failed. "
        f"{failed_records} duplicate records detected."
    )

    result = validation_failure(
        validation_name="Duplicate Validation",
        total_records=total_records,
        failed_records=failed_records,
        failed_df=duplicate_records,
        message=f"{failed_records} duplicate records found."
    )

    raise Exception(result.message)


if __name__ == "__main__":

    try:

        result = duplicate_validation()

        print(result)

    except Exception as ex:

        logger.exception(ex)

        raise
