"""
===============================================================================

File Name
    pipeline_health.py

Description
    Enterprise Pipeline Health Calculator

Features

    • Health Score
    • Pipeline Status
    • SLA Score
    • Validation Score
    • Data Quality Score

Author
    Enterprise Data Engineering Team

===============================================================================
"""

from monitoring.config import (

    SUCCESS_WEIGHT,

    SLA_WEIGHT,

    DATA_QUALITY_WEIGHT,

    VALIDATION_WEIGHT,

    HEALTHY_THRESHOLD,

    WARNING_THRESHOLD

)

from monitoring.monitoring_logger import (

    log_info,

    log_warning

)

###############################################################################
# Pipeline Health
###############################################################################

class PipelineHealth:

    """
    Calculates overall pipeline health.
    """

    def __init__(self):

        self.health_score = 0

        self.status = "UNKNOWN"

###############################################################################
# Success Score
###############################################################################

    def calculate_success_score(

        self,

        success_rate

    ):

        """
        Maximum score = SUCCESS_WEIGHT
        """

        return round(

            (success_rate / 100)

            * SUCCESS_WEIGHT,

            2

        )

###############################################################################
# SLA Score
###############################################################################

    def calculate_sla_score(

        self,

        sla_passed

    ):

        """
        SLA score.
        """

        if sla_passed:

            return SLA_WEIGHT

        return 0

###############################################################################
# Validation Score
###############################################################################

    def calculate_validation_score(

        self,

        validation_percentage

    ):

        """
        Validation score.
        """

        return round(

            (validation_percentage / 100)

            * VALIDATION_WEIGHT,

            2

        )

###############################################################################
# Data Quality Score
###############################################################################

    def calculate_quality_score(

        self,

        quality_percentage

    ):

        """
        Quality score.
        """

        return round(

            (quality_percentage / 100)

            * DATA_QUALITY_WEIGHT,

            2

        )


import json

###############################################################################
# Overall Health Score
###############################################################################

    def calculate_health(
        self,
        success_rate,
        sla_passed,
        validation_percentage,
        quality_percentage
    ):
        """
        Calculate overall pipeline health score.
        """

        success_score = self.calculate_success_score(
            success_rate
        )

        sla_score = self.calculate_sla_score(
            sla_passed
        )

        validation_score = self.calculate_validation_score(
            validation_percentage
        )

        quality_score = self.calculate_quality_score(
            quality_percentage
        )

        self.health_score = round(

            success_score
            +
            sla_score
            +
            validation_score
            +
            quality_score,

            2

        )

        self.status = self.get_health_status()

        log_info(

            f"Pipeline Health Score : {self.health_score}"

        )

        log_info(

            f"Pipeline Status : {self.status}"

        )

        return self.health_score

###############################################################################
# Health Status
###############################################################################

    def get_health_status(self):
        """
        Determine pipeline health status.
        """

        if self.health_score >= HEALTHY_THRESHOLD:

            return "HEALTHY"

        elif self.health_score >= WARNING_THRESHOLD:

            return "WARNING"

        return "CRITICAL"

###############################################################################
# Health Summary
###############################################################################

    def build_summary(self):
        """
        Return health summary.
        """

        return {

            "health_score": self.health_score,

            "status": self.status

        }

###############################################################################
# JSON Export
###############################################################################

    def export_json(
        self,
        output_file
    ):
        """
        Export health summary.
        """

        with open(
            output_file,
            "w"
        ) as file:

            json.dump(

                self.build_summary(),

                file,

                indent=4

            )

        log_info(

            f"Health summary exported to {output_file}"

        )

###############################################################################
# Dashboard Payload
###############################################################################

    def dashboard_payload(self):
        """
        Payload used by Grafana/API.
        """

        return {

            "Pipeline Health": self.health_score,

            "Pipeline Status": self.status

        }

###############################################################################
# Print Health
###############################################################################

    def print_health(self):
        """
        Print health summary.
        """

        log_info("=" * 80)

        log_info(

            f"Pipeline Health Score : {self.health_score}"

        )

        log_info(

            f"Pipeline Status : {self.status}"

        )

        log_info("=" * 80)


###############################################################################
# Health Trend
###############################################################################

    def calculate_trend(
        self,
        previous_score
    ):
        """
        Compare current health score with previous run.
        """

        difference = round(

            self.health_score - previous_score,

            2

        )

        if difference > 0:

            trend = "IMPROVING"

        elif difference < 0:

            trend = "DECLINING"

        else:

            trend = "STABLE"

        return {

            "previous_score": previous_score,

            "current_score": self.health_score,

            "difference": difference,

            "trend": trend

        }

###############################################################################
# Risk Level
###############################################################################

    def calculate_risk_level(self):
        """
        Determine operational risk level.
        """

        if self.health_score >= 95:

            return "LOW"

        elif self.health_score >= 80:

            return "MEDIUM"

        return "HIGH"

###############################################################################
# Recommendations
###############################################################################

    def generate_recommendations(
        self,
        success_rate,
        sla_passed,
        validation_percentage,
        quality_percentage
    ):
        """
        Generate operational recommendations.
        """

        recommendations = []

        if success_rate < 99:

            recommendations.append(

                "Investigate failed pipeline executions."

            )

        if not sla_passed:

            recommendations.append(

                "Review task bottlenecks causing SLA breaches."

            )

        if validation_percentage < 98:

            recommendations.append(

                "Investigate data validation failures."

            )

        if quality_percentage < 98:

            recommendations.append(

                "Review duplicate and null records."

            )

        if len(recommendations) == 0:

            recommendations.append(

                "Pipeline operating normally."

            )

        return recommendations

###############################################################################
# Health Report
###############################################################################

    def build_health_report(
        self,
        success_rate,
        sla_passed,
        validation_percentage,
        quality_percentage,
        previous_score
    ):
        """
        Build complete health report.
        """

        trend = self.calculate_trend(

            previous_score

        )

        report = {

            "health_score": self.health_score,

            "status": self.status,

            "risk_level": self.calculate_risk_level(),

            "trend": trend,

            "recommendations":

                self.generate_recommendations(

                    success_rate,

                    sla_passed,

                    validation_percentage,

                    quality_percentage

                )

        }

        return report

###############################################################################
# Print Health Report
###############################################################################

    def print_health_report(
        self,
        report
    ):
        """
        Print health report.
        """

        log_info("=" * 80)

        log_info("PIPELINE HEALTH REPORT")

        log_info("=" * 80)

        log_info(

            f"Health Score : {report['health_score']}"

        )

        log_info(

            f"Status : {report['status']}"

        )

        log_info(

            f"Risk : {report['risk_level']}"

        )

        log_info(

            f"Trend : {report['trend']['trend']}"

        )

        log_info("Recommendations:")

        for item in report["recommendations"]:

            log_info(

                f" - {item}"

            )

        log_info("=" * 80)
