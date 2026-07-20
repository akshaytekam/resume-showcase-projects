"""
===============================================================================
File Name : data_quality_report.py

Description:
    Generates consolidated Data Quality Reports after executing
    all validations.

Business Purpose
----------------
Provides a centralized report containing:

✓ Validation Summary
✓ Success Count
✓ Failure Count
✓ Success Percentage
✓ Pipeline Status
✓ CSV Report
✓ JSON Report

Author:
Enterprise Data Engineering Team

Version:
2.0
===============================================================================
"""

from pathlib import Path
from datetime import datetime
import json

import pandas as pd

from utils.logger import logger


class DataQualityReport:

    def __init__(self):

        self.results = []

        self.report_directory = Path("validation/reports")

        self.report_directory.mkdir(
            parents=True,
            exist_ok=True
        )

    # -------------------------------------------------------------------------

    def add_result(self, validation_result):

        self.results.append(validation_result)

    # -------------------------------------------------------------------------

    def build_dataframe(self):

        report = []

        for item in self.results:

            report.append({

                "Validation Name": item.validation_name,

                "Status": item.status,

                "Total Records": item.total_records,

                "Failed Records": item.failed_records,

                "Message": item.message,

                "Execution Time": item.execution_time

            })

        return pd.DataFrame(report)

    # -------------------------------------------------------------------------

    def generate_csv(self):

        dataframe = self.build_dataframe()

        csv_path = self.report_directory / "data_quality_report.csv"

        dataframe.to_csv(
            csv_path,
            index=False
        )

        logger.info(f"CSV Report Generated : {csv_path}")

        return dataframe

    # -------------------------------------------------------------------------

    def generate_json(self):

        dataframe = self.build_dataframe()

        json_path = self.report_directory / "data_quality_report.json"

        dataframe.to_json(

            json_path,

            orient="records",

            indent=4

        )

        logger.info(f"JSON Report Generated : {json_path}")

    # -------------------------------------------------------------------------

    def generate_summary(self):

        passed = 0

        failed = 0

        for result in self.results:

            if result.status.upper() == "SUCCESS":

                passed += 1

            else:

                failed += 1

        total = passed + failed

        success_percentage = 0

        if total > 0:

            success_percentage = round(

                (passed / total) * 100,

                2

            )

        summary = {

            "Execution Time":

                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),

            "Total Validations":

                total,

            "Successful":

                passed,

            "Failed":

                failed,

            "Success Percentage":

                success_percentage,

            "Pipeline Status":

                "SUCCESS" if failed == 0 else "FAILED"

        }

        summary_path = self.report_directory / "summary.json"

        with open(summary_path, "w") as file:

            json.dump(

                summary,

                file,

                indent=4

            )

        logger.info(f"Summary Generated : {summary_path}")

        return summary

    # -------------------------------------------------------------------------

    def generate_reports(self):

        self.generate_csv()

        self.generate_json()

        summary = self.generate_summary()

        logger.info("=" * 80)

        logger.info("DATA QUALITY SUMMARY")

        logger.info("=" * 80)

        for key, value in summary.items():

            logger.info(f"{key} : {value}")

        logger.info("=" * 80)

        return summary
