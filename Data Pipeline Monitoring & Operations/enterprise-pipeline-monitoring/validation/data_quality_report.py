import pandas as pd

from pathlib import Path

from validation.validation_result import ValidationResult


class DataQualityReport:

    def __init__(self):

        self.results = []

    def add_result(self, result: ValidationResult):

        self.results.append(result)

    def generate_csv(self):

        report = []

        for item in self.results:

            report.append({

                "Validation":

                    item.validation_name,

                "Status":

                    item.status,

                "Total Records":

                    item.total_records,

                "Failed Records":

                    item.failed_records,

                "Message":

                    item.message,

                "Execution Time":

                    item.execution_time

            })

        df = pd.DataFrame(report)

        Path("validation/reports").mkdir(
            parents=True,
            exist_ok=True
        )

        df.to_csv(

            "validation/reports/data_quality_report.csv",

            index=False

        )

        return df
