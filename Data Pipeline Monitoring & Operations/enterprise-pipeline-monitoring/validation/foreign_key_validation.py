"""
===============================================================================
File Name : foreign_key_validation.py

Description:
    Validates referential integrity between Sales dataset and
    reference master datasets (Customers and Products).

Business Purpose
----------------
Foreign Keys ensure that transactional records reference valid
master data.

Example:
    customer_id in Sales must exist in Customers
    product_id  in Sales must exist in Products

If invalid foreign keys exist:

    • Invalid Sales Records
    • Join Failures
    • Incorrect Reports
    • Data Warehouse Integrity Issues

Validation Rules
----------------

✓ Customer ID must exist in Customers

✓ Product ID must exist in Products

✓ Generates rejected records

✓ Generates validation report

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

from validation.validation_utils import (
    load_csv,
    validation_success,
    validation_failure
)


def foreign_key_validation():

    logger.info("=" * 80)
    logger.info("Starting Foreign Key Validation")
    logger.info("=" * 80)

    config = load_config()

    landing = Path(config["landing_path"])

    sales_file = landing / "sales" / "sales_2026-07-01.csv"
    customer_file = landing / "customers" / "customers_2026-07-01.csv"
    product_file = landing / "products" / "products_2026-07-01.csv"

    for file in [sales_file, customer_file, product_file]:

        if not file.exists():
            raise FileNotFoundError(f"{file} not found.")

    sales_df = load_csv(str(sales_file))

    customer_df = load_csv(str(customer_file))

    product_df = load_csv(str(product_file))

    total_records = len(sales_df)

    logger.info(f"Sales Records : {total_records}")

    # ------------------------------------------------------------------
    # CUSTOMER FK
    # ------------------------------------------------------------------

    invalid_customer = sales_df[
        ~sales_df["customer_id"].isin(customer_df["customer_id"])
    ]

    logger.info(
        f"Invalid Customer IDs : {len(invalid_customer)}"
    )

    # ------------------------------------------------------------------
    # PRODUCT FK
    # ------------------------------------------------------------------

    invalid_product = sales_df[
        ~sales_df["product_id"].isin(product_df["product_id"])
    ]

    logger.info(
        f"Invalid Product IDs : {len(invalid_product)}"
    )

    # ------------------------------------------------------------------
    # MERGE
    # ------------------------------------------------------------------

    invalid_records = pd.concat(
        [
            invalid_customer,
            invalid_product
        ]
    ).drop_duplicates()

    failed_records = len(invalid_records)

    # ------------------------------------------------------------------
    # SUCCESS
    # ------------------------------------------------------------------

    if failed_records == 0:

        logger.info(
            "Foreign Key Validation Successful."
        )

        return validation_success(

            validation_name="Foreign Key Validation",

            total_records=total_records,

            message="Foreign key validation successful."

        )

    # ------------------------------------------------------------------
    # FAILURE
    # ------------------------------------------------------------------

    logger.error(
        f"{failed_records} invalid foreign key records found."
    )

    result = validation_failure(

        validation_name="Foreign Key Validation",

        total_records=total_records,

        failed_records=failed_records,

        failed_df=invalid_records,

        message=f"{failed_records} foreign key validation failures."

    )

    raise Exception(result.message)


if __name__ == "__main__":

    try:

        result = foreign_key_validation()

        print(result)

    except Exception as ex:

        logger.exception(ex)

        raise
