"""
===============================================================================

File Name   : validation_utils.py

Description :
Reusable utility functions for the Enterprise Data Validation Framework.

These utilities are shared across all validation modules.

Author      : Enterprise Data Engineering Team

Version     : 2.0

===============================================================================
"""

import time
import os
from datetime import datetime

from pyspark.sql.functions import col


# =============================================================================
# Timer Utilities
# =============================================================================

class ExecutionTimer:
    """
    Measures execution time for validation modules.
    """

    def __init__(self):

        self.start_time = None

        self.end_time = None

    def start(self):

        self.start_time = time.time()

    def stop(self):

        self.end_time = time.time()

        return round(

            self.end_time - self.start_time,

            2

        )


# =============================================================================
# DataFrame Utilities
# =============================================================================

def get_record_count(df):
    """
    Returns DataFrame row count.
    """

    return df.count()


def get_column_count(df):
    """
    Returns number of columns.
    """

    return len(df.columns)


def dataframe_is_empty(df):
    """
    Returns True if DataFrame is empty.
    """

    return df.rdd.isEmpty()


# =============================================================================
# Percentage Utility
# =============================================================================

def calculate_percentage(part, total):
    """
    Calculates percentage safely.
    """

    if total == 0:

        return 0.0

    return round(

        (part / total) * 100,

        2

    )


# =============================================================================
# Duplicate Utility
# =============================================================================

def get_duplicate_count(df, key_columns):
    """
    Returns duplicate record count.
    """

    duplicate_df = (

        df.groupBy(key_columns)

        .count()

        .filter(col("count") > 1)

    )

    return duplicate_df.count()


# =============================================================================
# Null Utility
# =============================================================================

def get_null_count(df, column_name):
    """
    Returns NULL count for a column.
    """

    return (

        df.filter(

            col(column_name).isNull()

        ).count()

    )


# =============================================================================
# File Utilities
# =============================================================================

def file_exists(path):
    """
    Checks if local file exists.

    NOTE:
    In production, use DBUtils or S3 APIs.
    """

    return os.path.exists(path)


def get_file_size(path):
    """
    Returns file size in bytes.
    """

    if not os.path.exists(path):

        return 0

    return os.path.getsize(path)


# =============================================================================
# Date Utility
# =============================================================================

def current_timestamp():

    return datetime.now()


def current_date():

    return datetime.now().date()


# =============================================================================
# Validation Result Helper
# =============================================================================

def build_validation_result(
        validation_name,
        status,
        records_checked,
        failed_records,
        execution_time,
        message
):
    """
    Standard validation result dictionary.
    """

    return {

        "validation_name": validation_name,

        "status": status,

        "records_checked": records_checked,

        "failed_records": failed_records,

        "execution_time_seconds": execution_time,

        "message": message,

        "timestamp": str(current_timestamp())

    }


# =============================================================================
# Pretty Printer
# =============================================================================

def print_validation_summary(result):
    """
    Prints validation result in a readable format.
    """

    print("=" * 80)

    print(f"Validation : {result['validation_name']}")

    print(f"Status     : {result['status']}")

    print(f"Checked    : {result['records_checked']}")

    print(f"Failed     : {result['failed_records']}")

    print(f"Duration   : {result['execution_time_seconds']} sec")

    print(f"Message    : {result['message']}")

    print("=" * 80)
