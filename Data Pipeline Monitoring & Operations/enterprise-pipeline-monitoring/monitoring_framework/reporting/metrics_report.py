"""
===============================================================================

File Name
    metrics_report.py

Description
    Enterprise Metrics Reporting Engine

Features

    • Daily KPI Report
    • Pipeline Statistics
    • Success Rate Analysis
    • Runtime Analysis
    • Executive Summary

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
# Metrics Report Generator
###############################################################################

class MetricsReport:

    """
    Generate KPI reports from collected pipeline metrics.
    """

    def __init__(self):

        self.generated_time = datetime.utcnow()

###############################################################################
# Calculate Success Rate
###############################################################################

    def calculate_success_rate(
        self,
        metrics
    ):
        """
        Calculate pipeline success percentage.
        """

        if not metrics:

            return 0.0

        successful = sum(

            1

            for metric in metrics

            if metric["status"] == "SUCCESS"

        )

        return round(

            (successful / len(metrics)) * 100,

            2

        )

###############################################################################
# Calculate Failure Rate
###############################################################################

    def calculate_failure_rate(
        self,
        metrics
    ):
        """
        Calculate pipeline failure percentage.
        """

        if not metrics:

            return 0.0

        failed = sum(

            1

            for metric in metrics

            if metric["status"] == "FAILED"

        )

        return round(

            (failed / len(metrics)) * 100,

            2

        )

###############################################################################
# Average Runtime
###############################################################################

    def calculate_average_runtime(
        self,
        metrics
    ):
        """
        Calculate average pipeline runtime.
        """

        runtimes = [

            metric["runtime_minutes"]

            for metric in metrics

        ]

        if not runtimes:

            return 0

        return round(

            mean(runtimes),

            2

        )


###############################################################################
# Total Pipeline Executions
###############################################################################

    def total_pipeline_runs(
        self,
        metrics
    ):
        """
        Return total number of pipeline executions.
        """

        return len(metrics)


###############################################################################
# Status Distribution
###############################################################################

    def status_distribution(
        self,
        metrics
    ):
        """
        Count pipeline executions by status.
        """

        distribution = {

            "SUCCESS": 0,
            "FAILED": 0,
            "RUNNING": 0,
            "WARNING": 0

        }

        for metric in metrics:

            status = metric.get("status", "UNKNOWN")

            if status in distribution:

                distribution[status] += 1

            else:

                distribution[status] = (

                    distribution.get(status, 0) + 1

                )

        return distribution


###############################################################################
# Runtime Statistics
###############################################################################

    def runtime_statistics(
        self,
        metrics
    ):
        """
        Calculate runtime statistics.
        """

        runtimes = [

            metric["runtime_minutes"]

            for metric in metrics

            if "runtime_minutes" in metric

        ]

        if not runtimes:

            return {

                "minimum": 0,
                "maximum": 0,
                "average": 0

            }

        return {

            "minimum": min(runtimes),

            "maximum": max(runtimes),

            "average": round(

                mean(runtimes),

                2

            )

        }


###############################################################################
# Top Failed Pipelines
###############################################################################

    def top_failed_pipelines(
        self,
        metrics
    ):
        """
        Return failed pipeline names.
        """

        failures = {}

        for metric in metrics:

            if metric["status"] != "FAILED":

                continue

            pipeline = metric["pipeline"]

            failures[pipeline] = (

                failures.get(pipeline, 0) + 1

            )

        return sorted(

            failures.items(),

            key=lambda item: item[1],

            reverse=True

        )


###############################################################################
# Pipeline Summary
###############################################################################

    def pipeline_summary(
        self,
        metrics
    ):
        """
        Generate executive KPI summary.
        """

        return {

            "generated_time":

                datetime.utcnow(),

            "total_runs":

                self.total_pipeline_runs(

                    metrics

                ),

            "success_rate":

                self.calculate_success_rate(

                    metrics

                ),

            "failure_rate":

                self.calculate_failure_rate(

                    metrics

                ),

            "average_runtime":

                self.calculate_average_runtime(

                    metrics

                ),

            "status_distribution":

                self.status_distribution(

                    metrics

                ),

            "runtime_statistics":

                self.runtime_statistics(

                    metrics

                ),

            "top_failed_pipelines":

                self.top_failed_pipelines(

                    metrics

                )

        }


###############################################################################
# Mean Time To Recovery (MTTR)
###############################################################################

    def calculate_mttr(
        self,
        metrics
    ):
        """
        Calculate Mean Time To Recovery (minutes).
        """

        recovery_times = [

            metric["recovery_minutes"]

            for metric in metrics

            if metric.get("recovery_minutes") is not None

        ]

        if not recovery_times:

            return 0.0

        return round(

            mean(recovery_times),

            2

        )


###############################################################################
# Mean Time To Acknowledge (MTTA)
###############################################################################

    def calculate_mtta(
        self,
        metrics
    ):
        """
        Calculate Mean Time To Acknowledge (minutes).
        """

        acknowledgement_times = [

            metric["acknowledgement_minutes"]

            for metric in metrics

            if metric.get("acknowledgement_minutes") is not None

        ]

        if not acknowledgement_times:

            return 0.0

        return round(

            mean(acknowledgement_times),

            2

        )


###############################################################################
# Pipeline Availability
###############################################################################

    def calculate_availability(
        self,
        metrics
    ):
        """
        Calculate pipeline availability percentage.
        """

        total_runs = len(metrics)

        if total_runs == 0:

            return 0.0

        successful_runs = sum(

            1

            for metric in metrics

            if metric.get("status") == "SUCCESS"

        )

        return round(

            (successful_runs / total_runs) * 100,

            2

        )


###############################################################################
# Generate Metrics Report
###############################################################################

    def generate_report(
        self,
        metrics,
        report_type="DAILY"
    ):
        """
        Generate complete metrics report.
        """

        log_report_started(

            report_name=f"{report_type} Metrics Report",

            report_type=report_type

        )

        try:

            report = {

                "report_type":

                    report_type,

                "generated_time":

                    datetime.utcnow(),

                "pipeline_summary":

                    self.pipeline_summary(metrics),

                "mttr_minutes":

                    self.calculate_mttr(metrics),

                "mtta_minutes":

                    self.calculate_mtta(metrics),

                "availability_percent":

                    self.calculate_availability(metrics)

            }

            log_report_completed(

                report_name=f"{report_type} Metrics Report",

                duration_seconds=0.0

            )

            return report

        except Exception as ex:

            log_report_failed(

                report_name=f"{report_type} Metrics Report",

                error=ex

            )

            raise
