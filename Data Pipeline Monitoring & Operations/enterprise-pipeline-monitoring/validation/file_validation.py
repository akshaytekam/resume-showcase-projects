"""
===============================================================================

File Name   : file_validation.py

Description :
Enterprise File Validation Module.

Performs:
    • File Count Validation
    • Empty File Validation
    • Duplicate File Validation
    • File Extension Validation
    • File Naming Validation
    • File Size Validation

Author      : Enterprise Data Engineering Team

Version     : 2.0

===============================================================================
"""

import os
import re
from datetime import datetime

from validation.validation_config import (
    storage,
    pipeline
)

from validation.validation_utils import (
    build_validation_result
)


class FileValidation:

    """
    Validates source files before ingestion.
    """

    def __init__(self, spark):

        self.spark = spark

        self.landing_path = storage.LANDING_PATH

        self.expected_files = pipeline.EXPECTED_DAILY_FILES

        self.file_extension = pipeline.FILE_EXTENSION

    # =========================================================================
    # Get File List
    # =========================================================================

    def get_files(self):

        """
        Returns all files in landing directory.

        NOTE:
        Replace with S3 listing in production.
        """

        if not os.path.exists(self.landing_path):

            raise Exception(
                f"Landing path not found : {self.landing_path}"
            )

        files = [

            f

            for f in os.listdir(self.landing_path)

            if os.path.isfile(
                os.path.join(
                    self.landing_path,
                    f
                )
            )

        ]

        return files

    # =========================================================================
    # Expected File Count
    # =========================================================================

    def validate_file_count(self, files):

        actual = len(files)

        if actual != self.expected_files:

            raise Exception(

                f"Expected {self.expected_files} files "

                f"but received {actual}"

            )

    # =========================================================================
    # Duplicate File Names
    # =========================================================================

    def validate_duplicate_files(self, files):

        duplicates = []

        seen = set()

        for file in files:

            if file in seen:

                duplicates.append(file)

            seen.add(file)

        if duplicates:

            raise Exception(

                f"Duplicate files detected : {duplicates}"

            )

    # =========================================================================
    # File Extension
    # =========================================================================

    def validate_extension(self, files):

        invalid = [

            f

            for f in files

            if not f.endswith(self.file_extension)

        ]

        if invalid:

            raise Exception(

                f"Invalid extension files : {invalid}"

            )

    # =========================================================================
    # Empty Files
    # =========================================================================

    def validate_empty_files(self, files):

        empty_files = []

        for file in files:

            full_path = os.path.join(

                self.landing_path,

                file

            )

            if os.path.getsize(full_path) == 0:

                empty_files.append(file)

        if empty_files:

            raise Exception(

                f"Empty files detected : {empty_files}"

            )

    # =========================================================================
    # File Naming Convention
    # =========================================================================

    def validate_file_name(self, files):

        """
        Expected Pattern:

        sales_001_20260721.csv
        """

        pattern = r"^sales_\d{3}_\d{8}\.csv$"

        invalid = [

            f

            for f in files

            if not re.match(pattern, f)

        ]

        if invalid:

            raise Exception(

                f"Invalid filenames : {invalid}"

            )

    # =========================================================================
    # SLA Check
    # =========================================================================

    def validate_file_arrival(self, files):

        """
        Example:
        Ensure today's files are delivered.
        """

        today = datetime.now().strftime("%Y%m%d")

        invalid = [

            f

            for f in files

            if today not in f

        ]

        if invalid:

            raise Exception(

                f"Files not delivered for today : {invalid}"

            )

    # =========================================================================
    # Main Validation
    # =========================================================================

    def validate(self):

        files = self.get_files()

        self.validate_file_count(files)

        self.validate_duplicate_files(files)

        self.validate_extension(files)

        self.validate_empty_files(files)

        self.validate_file_name(files)

        self.validate_file_arrival(files)

        return build_validation_result(

            validation_name="File Validation",

            status="SUCCESS",

            records_checked=len(files),

            failed_records=0,

            execution_time=0,

            message="All file validations passed."

        )
