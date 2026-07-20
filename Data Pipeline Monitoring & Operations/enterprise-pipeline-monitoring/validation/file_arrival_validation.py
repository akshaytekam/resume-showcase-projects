"""
===============================================================================
File Name : file_arrival_validation.py

Description:
    Validates whether all required source files have arrived before the ETL
    pipeline starts.

Business Purpose:
    Prevent downstream ETL failures caused by missing input files.

Author:
    Enterprise Data Engineering Team
===============================================================================
"""

from pathlib import Path

from utils.logger import logger
from utils.config_loader import load_config
from validation.validation_utils import validation_success
from validation.validation_utils import validation_failure


def file_validation():

    logger.info("=" * 80)
    logger.info("Starting File Arrival Validation")
    logger.info("=" * 80)

    config = load_config()

    landing_path = Path(config["landing_path"])

    required_files = [
        "sales/sales_2026-07-01.csv",
        "customers/customers_2026-07-01.csv",
        "products/products_2026-07-01.csv",
        "inventory/inventory_2026-07-01.csv"
    ]

    missing_files = []

    for file_name in required_files:

        file_path = landing_path / file_name

        if not file_path.exists():

            logger.error(f"Missing File : {file_name}")

            missing_files.append(file_name)

        else:

            logger.info(f"File Found : {file_name}")

    total_files = len(required_files)

    failed_files = len(missing_files)

    if failed_files == 0:

        logger.info("All Required Files Arrived Successfully.")

        return validation_success(
            validation_name="File Arrival Validation",
            total_records=total_files,
            message="All required files are available."
        )

    else:

        import pandas as pd

        failed_df = pd.DataFrame({

            "missing_file": missing_files

        })

        result = validation_failure(

            validation_name="File Arrival Validation",

            total_records=total_files,

            failed_records=failed_files,

            failed_df=failed_df,

            message=f"{failed_files} required file(s) are missing."

        )

        raise FileNotFoundError(result.message)
