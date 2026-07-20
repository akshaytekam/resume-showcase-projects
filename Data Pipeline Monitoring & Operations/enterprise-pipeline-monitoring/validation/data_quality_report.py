"""
===============================================================================

File Name   : quality_report.py

Description :
Enterprise Data Quality Reporting Framework.

Generates

• Validation Summary
• Data Quality Score
• Execution Summary
• Failed Validation Summary
• CSV Report
• Delta Audit Report
• Future Dashboard Integration

Author
------
Enterprise Data Engineering Team

Version
-------
2.0

===============================================================================
"""

import os

from datetime import datetime

from pyspark.sql import Row

from validation.validation_config import (
    storage
)


# =============================================================================
# Quality Report
# =============================================================================

class QualityReport:

    """
    Enterprise Data Quality Report
    """

    def __init__(

            self,

            spark,

            validation_results

    ):

        self.spark = spark

        self.results = validation_results

    # =========================================================================
    # Overall Score
    # =========================================================================

    def calculate_quality_score(self):

        total = len(self.results)

        passed = len(

            [

                x

                for x in self.results

                if x["status"] == "SUCCESS"

            ]

        )

        if total == 0:

            return 0

        return round(

            (passed / total) * 100,

            2

        )

    # =========================================================================
    # Overall Status
    # =========================================================================

    def overall_status(self):

        failed = [

            x

            for x in self.results

            if x["status"] == "FAILED"

        ]

        if len(failed) == 0:

            return "SUCCESS"

        return "FAILED"

    # =========================================================================
    # Console Report
    # =========================================================================

    def print_report(self):

        print()

        print("=" * 100)

        print("ENTERPRISE DATA QUALITY REPORT")

        print("=" * 100)

        print(

            f"{'Validation':30}"

            f"{'Status':15}"

            f"{'Execution(s)':15}"

        )

        print("-" * 100)

        for result in self.results:

            print(

                f"{result['validation_name']:30}"

                f"{result['status']:15}"

                f"{result['execution_time']:>10.2f}"

            )

        print("-" * 100)

        print(

            f"Overall Status : "

            f"{self.overall_status()}"

        )

        print(

            f"Quality Score : "

            f"{self.calculate_quality_score()}%"

        )

        print("=" * 100)

    # =========================================================================
    # CSV Report
    # =========================================================================

    def write_csv_report(

            self,

            output_folder

    ):

        import pandas as pd

        dataframe = pd.DataFrame(

            self.results

        )

        file_name = (

            f"quality_report_"

            f"{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

        )

        path = os.path.join(

            output_folder,

            file_name

        )

        dataframe.to_csv(

            path,

            index=False

        )

        print(

            f"CSV Report Saved : {path}"

        )

    # =========================================================================
    # Delta Audit Table
    # =========================================================================

    def write_delta_report(self):

        rows = [

            Row(**row)

            for row in self.results

        ]

        dataframe = self.spark.createDataFrame(

            rows

        )

        (

            dataframe.write

            .format("delta")

            .mode("append")

            .save(

                storage.QUALITY_REPORT_PATH

            )

        )

        print(

            "Delta Quality Report Saved."

        )

    # =========================================================================
    # Dashboard Hooks
    # =========================================================================

    def publish_cloudwatch(self):

        """
        Future CloudWatch Integration.
        """

        pass

    def publish_grafana(self):

        """
        Future Grafana Integration.
        """

        pass

    def send_email(self):

        """
        Future Email Summary.
        """

        pass

    def send_slack(self):

        """
        Future Slack Notification.
        """

        pass

    # =========================================================================
    # Generate Report
    # =========================================================================

    def generate(

            self,

            output_folder

    ):

        self.print_report()

        self.write_csv_report(

            output_folder

        )

        self.write_delta_report()

        self.publish_cloudwatch()

        self.publish_grafana()

        self.send_email()

        self.send_slack()
