"""
===============================================================================
schema_validation.py

Description:
    Validates dataset schema before loading into Bronze Layer.

Checks

✓ Column Count

✓ Column Names

✓ Missing Columns

✓ Unexpected Columns

===============================================================================
"""

import pandas as pd

from pathlib import Path

from utils.logger import logger
from utils.config_loader import load_config

from validation.validation_constants import EXPECTED_SALES_COLUMNS

from validation.validation_utils import validation_success
from validation.validation_utils import validation_failure


def schema_validation():

    logger.info("=" * 80)
    logger.info("Starting Schema Validation")
    logger.info("=" * 80)

    config = load_config()

    sales_file = Path(
        config["landing_path"]
    ) / "sales" / "sales_2026-07-01.csv"

    df = pd.read_csv(sales_file)

    actual_columns = list(df.columns)

    expected_columns = EXPECTED_SALES_COLUMNS

    missing_columns = []

    unexpected_columns = []

    for column in expected_columns:

        if column not in actual_columns:

            missing_columns.append(column)

    for column in actual_columns:

        if column not in expected_columns:

            unexpected_columns.append(column)

    if len(missing_columns) == 0 and len(unexpected_columns) == 0:

        logger.info("Schema Validation Successful.")

        return validation_success(

            validation_name="Schema Validation",

            total_records=len(df),

            message="Schema matches expected structure."

        )

    report = pd.DataFrame({

        "Missing Columns": pd.Series(missing_columns),

        "Unexpected Columns": pd.Series(unexpected_columns)

    })

    result = validation_failure(

        validation_name="Schema Validation",

        total_records=len(df),

        failed_records=len(missing_columns) + len(unexpected_columns),

        failed_df=report,

        message="Schema mismatch detected."

    )

    raise Exception(result.message)
