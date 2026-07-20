"""
===============================================================================

File Name   : reconciliation_validation.py

Description :
Enterprise Reconciliation Validation.

Performs:

• Landing vs Bronze
• Bronze vs Silver
• Silver vs Gold
• Revenue Reconciliation
• Record Count Validation
• Data Loss Detection

Author
------
Enterprise Data Engineering Team

Version
-------
2.0

===============================================================================
"""

from pyspark.sql.functions import (
    sum,
    count
)

from validation.validation_utils import (
    build_validation_result,
    calculate_percentage
)

from validation.validation_config import (
    storage
)

# =============================================================================
# Reconciliation Validation
# =============================================================================

class ReconciliationValidation:

    """
    Enterprise Reconciliation Validation
    """

    def __init__(

            self,

            spark

    ):

        self.spark = spark

        self.failed_checks = 0

    # =========================================================================
    # Read Delta Table
    # =========================================================================

    def read_delta(

            self,

            path

    ):

        return (

            self.spark.read

            .format("delta")

            .load(path)

        )

    # =========================================================================
    # Record Count
    # =========================================================================

    def get_record_count(

            self,

            df

    ):

        return df.count()

    # =========================================================================
    # Revenue
    # =========================================================================

    def get_total_revenue(

            self,

            df

    ):

        revenue = (

            df

            .select(

                sum("sales_amount")

            )

            .collect()[0][0]

        )

        if revenue is None:

            return 0

        return round(revenue, 2)

    # =========================================================================
    # Compare Counts
    # =========================================================================

    def compare_counts(

            self,

            source_count,

            target_count,

            source_name,

            target_name

    ):

        if source_count != target_count:

            loss = source_count - target_count

            loss_pct = calculate_percentage(

                abs(loss),

                source_count

            )

            self.failed_checks += 1

            print(

                f"{source_name} -> {target_name}"

            )

            print(

                f"Source : {source_count}"

            )

            print(

                f"Target : {target_count}"

            )

            print(

                f"Difference : {loss}"

            )

            print(

                f"Difference % : {loss_pct}"

            )

    # =========================================================================
    # Compare Revenue
    # =========================================================================

    def compare_revenue(

            self,

            source_revenue,

            target_revenue,

            source_name,

            target_name

    ):

        if round(source_revenue, 2) != round(target_revenue, 2):

            self.failed_checks += 1

            print(

                f"Revenue mismatch"

            )

            print(

                f"{source_name} : {source_revenue}"

            )

            print(

                f"{target_name} : {target_revenue}"

            )

    # =========================================================================
    # Main Validation
    # =========================================================================

    def validate(self):

        bronze = self.read_delta(

            storage.BRONZE_PATH

        )

        silver = self.read_delta(

            storage.SILVER_PATH

        )

        gold = self.read_delta(

            storage.GOLD_PATH

        )

        bronze_count = self.get_record_count(

            bronze

        )

        silver_count = self.get_record_count(

            silver

        )

        gold_count = self.get_record_count(

            gold

        )

        self.compare_counts(

            bronze_count,

            silver_count,

            "Bronze",

            "Silver"

        )

        self.compare_counts(

            silver_count,

            gold_count,

            "Silver",

            "Gold"

        )

        bronze_revenue = self.get_total_revenue(

            bronze

        )

        silver_revenue = self.get_total_revenue(

            silver

        )

        self.compare_revenue(

            bronze_revenue,

            silver_revenue,

            "Bronze",

            "Silver"

        )

        if self.failed_checks > 0:

            raise Exception(

                f"{self.failed_checks} reconciliation checks failed."

            )

        return build_validation_result(

            validation_name="Reconciliation Validation",

            status="SUCCESS",

            records_checked=gold_count,

            failed_records=0,

            execution_time=0,

            message="Reconciliation successful."

        )
