"""
===============================================================================

File Name
    pipeline_health_report.py

Description
    Enterprise Pipeline Health Reporting Engine

Features

    • Pipeline Health Score
    • Pipeline Status
    • SLA Status
    • Runtime Analysis
    • Operational Recommendations

Author
    Enterprise Data Engineering Team

===============================================================================
"""

from datetime import datetime

from monitoring_framework.reporting.report_logger import (

    log_report_started,

    log_report_completed,

    log_report_failed

)

###############################################################################
# Pipeline Health Report
###############################################################################

class PipelineHealthReport:

    """
    Generate health reports for individual pipelines.
    """

    def __init__(self):

        self.generated_time = datetime.utcnow()

###############################################################################
# Calculate Health Score
###############################################################################

    def calculate_health_score(
        self,
        pipeline
    ):
        """
        Calculate pipeline health score.

        Maximum score = 100
        """

        score = 100

        score -= (

            pipeline.get(

                "failure_count",

                0

            ) * 10

        )

        score -= (

            pipeline.get(

                "retry_count",

                0

            ) * 5

        )

        if not pipeline.get(

            "sla_met",

            True

        ):

            score -= 20

        score = max(

            score,

            0

        )

        return score

###############################################################################
# Health Category
###############################################################################

    def health_category(
        self,
        score
    ):
        """
        Convert health score into category.
        """

        if score >= 90:

            return "HEALTHY"

        elif score >= 75:

            return "GOOD"

        elif score >= 60:

            return "WARNING"

        else:

            return "CRITICAL"

###############################################################################
# Runtime Status
###############################################################################

    def runtime_status(
        self,
        pipeline
    ):
        """
        Determine runtime status.
        """

        runtime = pipeline.get(

            "runtime_minutes",

            0

        )

        sla = pipeline.get(

            "sla_minutes",

            60

        )

        if runtime <= sla:

            return "WITHIN_SLA"

        return "SLA_BREACHED"


###############################################################################
# Failure Summary
###############################################################################

    def failure_summary(
        self,
        pipeline
    ):
        """
        Return pipeline failure statistics.
        """

        return {

            "failure_count":

                pipeline.get(

                    "failure_count",

                    0

                ),

            "last_failure":

                pipeline.get(

                    "last_failure",

                    "N/A"

                )

        }

###############################################################################
# Retry Summary
###############################################################################

    def retry_summary(
        self,
        pipeline
    ):
        """
        Return retry information.
        """

        retries = pipeline.get(

            "retry_count",

            0

        )

        return {

            "retry_count": retries,

            "retry_status":

                "HIGH"

                if retries >= 3

                else "NORMAL"

        }

###############################################################################
# Recommendation Engine
###############################################################################

    def recommendation(
        self,
        pipeline,
        health_score
    ):
        """
        Generate operational recommendation.
        """

        recommendations = []

        if pipeline.get(

            "failure_count",

            0

        ) > 0:

            recommendations.append(

                "Investigate recurring failures."

            )

        if pipeline.get(

            "retry_count",

            0

        ) >= 3:

            recommendations.append(

                "Review retry configuration."

            )

        if not pipeline.get(

            "sla_met",

            True

        ):

            recommendations.append(

                "Optimize pipeline runtime."

            )

        if health_score < 60:

            recommendations.append(

                "Immediate operational review required."

            )

        if not recommendations:

            recommendations.append(

                "Pipeline operating normally."

            )

        return recommendations

###############################################################################
# Pipeline Health Summary
###############################################################################

    def pipeline_summary(
        self,
        pipeline
    ):
        """
        Generate complete pipeline health summary.
        """

        score = self.calculate_health_score(

            pipeline

        )

        return {

            "pipeline_name":

                pipeline.get(

                    "pipeline_name"

                ),

            "generated_time":

                datetime.utcnow(),

            "health_score":

                score,

            "health_status":

                self.health_category(

                    score

                ),

            "runtime_status":

                self.runtime_status(

                    pipeline

                ),

            "runtime_minutes":

                pipeline.get(

                    "runtime_minutes",

                    0

                ),

            "failure_summary":

                self.failure_summary(

                    pipeline

                ),

            "retry_summary":

                self.retry_summary(

                    pipeline

                ),

            "recommendations":

                self.recommendation(

                    pipeline,

                    score

                )

        }


###############################################################################
# Overall Platform Health
###############################################################################

    def platform_health_score(
        self,
        pipelines
    ):
        """
        Calculate average health score across all pipelines.
        """

        if not pipelines:

            return 0.0

        scores = [

            self.calculate_health_score(pipeline)

            for pipeline in pipelines

        ]

        return round(

            sum(scores) / len(scores),

            2

        )


###############################################################################
# Health Distribution
###############################################################################

    def health_distribution(
        self,
        pipelines
    ):
        """
        Count pipelines by health category.
        """

        distribution = {

            "HEALTHY": 0,

            "GOOD": 0,

            "WARNING": 0,

            "CRITICAL": 0

        }

        for pipeline in pipelines:

            score = self.calculate_health_score(

                pipeline

            )

            category = self.health_category(

                score

            )

            distribution[category] += 1

        return distribution


###############################################################################
# Top Unhealthy Pipelines
###############################################################################

    def top_unhealthy_pipelines(
        self,
        pipelines,
        limit=5
    ):
        """
        Return the least healthy pipelines.
        """

        ranked = sorted(

            pipelines,

            key=lambda pipeline:

                self.calculate_health_score(

                    pipeline

                )

        )

        return [

            {

                "pipeline_name":

                    pipeline.get(

                        "pipeline_name"

                    ),

                "health_score":

                    self.calculate_health_score(

                        pipeline

                    )

            }

            for pipeline in ranked[:limit]

        ]


###############################################################################
# Generate Platform Health Report
###############################################################################

    def generate_platform_report(
        self,
        pipelines
    ):
        """
        Generate complete platform health report.
        """

        report_name = "Pipeline Health Report"

        log_report_started(

            report_name=report_name,

            report_type="DAILY"

        )

        try:

            report = {

                "generated_time":

                    datetime.utcnow(),

                "pipeline_count":

                    len(pipelines),

                "platform_health_score":

                    self.platform_health_score(

                        pipelines

                    ),

                "health_distribution":

                    self.health_distribution(

                        pipelines

                    ),

                "top_unhealthy_pipelines":

                    self.top_unhealthy_pipelines(

                        pipelines

                    ),

                "pipelines": [

                    self.pipeline_summary(

                        pipeline

                    )

                    for pipeline in pipelines

                ]

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
