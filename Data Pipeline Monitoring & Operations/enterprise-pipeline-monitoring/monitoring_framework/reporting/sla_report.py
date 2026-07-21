"""
===============================================================================

File Name
    sla_report.py

Description
    Enterprise SLA Reporting Engine

Features

    • SLA Compliance
    • SLA Breach Analysis
    • Delay Calculation
    • Executive SLA Dashboard

Author
    Enterprise Data Engineering Team

===============================================================================
"""

from datetime import datetime

from statistics import mean

from monitoring_framework.reporting.report_logger import (

    log_report_started,

    log_report_completed,

    log_report_failed

)

###############################################################################
# SLA Report
###############################################################################

class SLAReport:

    """
    Generate SLA compliance reports.
    """

    def __init__(self):

        self.generated_time = datetime.utcnow()

###############################################################################
# SLA Compliance Percentage
###############################################################################

    def compliance_percentage(
        self,
        pipelines
    ):
        """
        Calculate SLA compliance percentage.
        """

        if not pipelines:

            return 0.0

        compliant = sum(

            1

            for pipeline in pipelines

            if pipeline.get("sla_met", False)

        )

        return round(

            (compliant / len(pipelines)) * 100,

            2

        )

###############################################################################
# SLA Breach Count
###############################################################################

    def breach_count(
        self,
        pipelines
    ):
        """
        Count pipelines that breached SLA.
        """

        return sum(

            1

            for pipeline in pipelines

            if not pipeline.get("sla_met", False)

        )

###############################################################################
# Delay Minutes
###############################################################################

    def average_delay(
        self,
        pipelines
    ):
        """
        Calculate average delay for breached pipelines.
        """

        delays = [

            pipeline.get(

                "delay_minutes",

                0

            )

            for pipeline in pipelines

            if not pipeline.get(

                "sla_met",

                False

            )

        ]

        if not delays:

            return 0

        return round(

            mean(delays),

            2

        )


###############################################################################
# Pipeline SLA Summary
###############################################################################

    def pipeline_sla_summary(
        self,
        pipeline
    ):
        """
        Generate SLA summary for a single pipeline.
        """

        return {

            "pipeline_name":

                pipeline.get(

                    "pipeline_name"

                ),

            "sla_minutes":

                pipeline.get(

                    "sla_minutes",

                    0

                ),

            "runtime_minutes":

                pipeline.get(

                    "runtime_minutes",

                    0

                ),

            "sla_met":

                pipeline.get(

                    "sla_met",

                    False

                ),

            "delay_minutes":

                pipeline.get(

                    "delay_minutes",

                    0

                )

        }


###############################################################################
# Top SLA Violators
###############################################################################

    def top_sla_violators(
        self,
        pipelines,
        limit=5
    ):
        """
        Return pipelines with the highest SLA delay.
        """

        breached = [

            pipeline

            for pipeline in pipelines

            if not pipeline.get(

                "sla_met",

                False

            )

        ]

        ranked = sorted(

            breached,

            key=lambda pipeline:

                pipeline.get(

                    "delay_minutes",

                    0

                ),

            reverse=True

        )

        return [

            {

                "pipeline_name":

                    pipeline.get(

                        "pipeline_name"

                    ),

                "delay_minutes":

                    pipeline.get(

                        "delay_minutes",

                        0

                    )

            }

            for pipeline in ranked[:limit]

        ]


###############################################################################
# Delay Distribution
###############################################################################

    def delay_distribution(
        self,
        pipelines
    ):
        """
        Categorize SLA delays.
        """

        distribution = {

            "0-15 Minutes": 0,

            "16-30 Minutes": 0,

            "31-60 Minutes": 0,

            "60+ Minutes": 0

        }

        for pipeline in pipelines:

            delay = pipeline.get(

                "delay_minutes",

                0

            )

            if delay <= 15:

                distribution["0-15 Minutes"] += 1

            elif delay <= 30:

                distribution["16-30 Minutes"] += 1

            elif delay <= 60:

                distribution["31-60 Minutes"] += 1

            else:

                distribution["60+ Minutes"] += 1

        return distribution


###############################################################################
# SLA Recommendation Engine
###############################################################################

    def recommendations(
        self,
        pipelines
    ):
        """
        Generate SLA improvement recommendations.
        """

        recommendations = []

        if self.compliance_percentage(

            pipelines

        ) < 95:

            recommendations.append(

                "Review pipelines with recurring SLA breaches."

            )

        if self.average_delay(

            pipelines

        ) > 20:

            recommendations.append(

                "Investigate long-running pipelines."

            )

        if self.breach_count(

            pipelines

        ) > 10:

            recommendations.append(

                "Increase monitoring for critical pipelines."

            )

        if not recommendations:

            recommendations.append(

                "Overall SLA performance is satisfactory."

            )

        return recommendations


###############################################################################
# Executive SLA Summary
###############################################################################

    def executive_summary(
        self,
        pipelines
    ):
        """
        Generate executive SLA dashboard summary.
        """

        return {

            "generated_time":

                datetime.utcnow(),

            "pipeline_count":

                len(pipelines),

            "sla_compliance":

                self.compliance_percentage(

                    pipelines

                ),

            "sla_breaches":

                self.breach_count(

                    pipelines

                ),

            "average_delay":

                self.average_delay(

                    pipelines

                ),

            "delay_distribution":

                self.delay_distribution(

                    pipelines

                ),

            "top_sla_violators":

                self.top_sla_violators(

                    pipelines

                ),

            "recommendations":

                self.recommendations(

                    pipelines

                )

        }


###############################################################################
# Detailed Pipeline Reports
###############################################################################

    def pipeline_reports(
        self,
        pipelines
    ):
        """
        Generate SLA report for every pipeline.
        """

        return [

            self.pipeline_sla_summary(

                pipeline

            )

            for pipeline in pipelines

        ]


###############################################################################
# Generate Complete SLA Report
###############################################################################

    def generate_report(
        self,
        pipelines,
        report_type="DAILY"
    ):
        """
        Generate complete SLA report.
        """

        report_name = f"{report_type} SLA Report"

        log_report_started(

            report_name=report_name,

            report_type=report_type

        )

        try:

            report = {

                "report_name":

                    report_name,

                "generated_time":

                    datetime.utcnow(),

                "report_type":

                    report_type,

                "executive_summary":

                    self.executive_summary(

                        pipelines

                    ),

                "pipeline_reports":

                    self.pipeline_reports(

                        pipelines

                    )

            }

            log_report_completed(

                report_name=report_name,

                duration_seconds=0.0

            )

            return report

        except Exception as ex:

            log_report_failed(

                report_name=report_name,

                error=ex

            )

            raise


###############################################################################
# Report Metadata
###############################################################################

    def report_metadata(
        self
    ):
        """
        Return metadata describing this report.
        """

        return {

            "report_engine":

                "Enterprise SLA Reporting Engine",

            "version":

                "1.0.0",

            "generated_at":

                datetime.utcnow(),

            "module":

                "sla_report.py"

        }


###############################################################################
# Health Indicator
###############################################################################

    def platform_sla_status(
        self,
        pipelines
    ):
        """
        Return overall platform SLA status.
        """

        compliance = self.compliance_percentage(

            pipelines

        )

        if compliance >= 99:

            return "EXCELLENT"

        elif compliance >= 95:

            return "GOOD"

        elif compliance >= 90:

            return "WARNING"

        return "CRITICAL"
