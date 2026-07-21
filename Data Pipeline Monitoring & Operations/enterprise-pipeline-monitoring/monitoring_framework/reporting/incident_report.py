"""
===============================================================================

File Name
    incident_report.py

Description
    Enterprise Incident Reporting Engine

Features

    • Incident Summary
    • Severity Distribution
    • Status Distribution
    • MTTR Reporting
    • Executive Dashboard

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
# Incident Report
###############################################################################

class IncidentReport:

    """
    Generate incident reports for production operations.
    """

    def __init__(self):

        self.generated_time = datetime.utcnow()

###############################################################################
# Total Incidents
###############################################################################

    def total_incidents(
        self,
        incidents
    ):
        """
        Return total incident count.
        """

        return len(incidents)

###############################################################################
# Severity Distribution
###############################################################################

    def severity_distribution(
        self,
        incidents
    ):
        """
        Count incidents by severity.
        """

        distribution = {

            "LOW": 0,

            "MEDIUM": 0,

            "HIGH": 0,

            "CRITICAL": 0

        }

        for incident in incidents:

            severity = incident.get(

                "severity",

                "LOW"

            )

            distribution[severity] = (

                distribution.get(

                    severity,

                    0

                ) + 1

            )

        return distribution

###############################################################################
# Status Distribution
###############################################################################

    def status_distribution(
        self,
        incidents
    ):
        """
        Count incidents by current status.
        """

        distribution = {

            "OPEN": 0,

            "IN_PROGRESS": 0,

            "RESOLVED": 0,

            "CLOSED": 0

        }

        for incident in incidents:

            status = incident.get(

                "status",

                "OPEN"

            )

            distribution[status] = (

                distribution.get(

                    status,

                    0

                ) + 1

            )

        return distribution

###############################################################################
# Average MTTR
###############################################################################

    def average_mttr(
        self,
        incidents
    ):
        """
        Calculate Mean Time To Recovery.
        """

        recovery_times = [

            incident.get(

                "recovery_minutes",

                0

            )

            for incident in incidents

            if incident.get(

                "status"

            ) == "RESOLVED"

        ]

        if not recovery_times:

            return 0

        return round(

            mean(recovery_times),

            2

        )


###############################################################################
# Incident Summary
###############################################################################

    def incident_summary(
        self,
        incident
    ):
        """
        Generate summary for a single incident.
        """

        return {

            "incident_id":

                incident.get(

                    "incident_id"

                ),

            "pipeline_name":

                incident.get(

                    "pipeline_name"

                ),

            "severity":

                incident.get(

                    "severity",

                    "LOW"

                ),

            "status":

                incident.get(

                    "status",

                    "OPEN"

                ),

            "root_cause":

                incident.get(

                    "root_cause",

                    "UNKNOWN"

                ),

            "recovery_minutes":

                incident.get(

                    "recovery_minutes",

                    0

                )

        }


###############################################################################
# Root Cause Distribution
###############################################################################

    def root_cause_distribution(
        self,
        incidents
    ):
        """
        Count incidents by root cause.
        """

        distribution = {}

        for incident in incidents:

            cause = incident.get(

                "root_cause",

                "UNKNOWN"

            )

            distribution[cause] = (

                distribution.get(

                    cause,

                    0

                ) + 1

            )

        return distribution


###############################################################################
# Top Recurring Pipelines
###############################################################################

    def top_recurring_pipelines(
        self,
        incidents,
        limit=5
    ):
        """
        Return pipelines with the highest incident count.
        """

        counts = {}

        for incident in incidents:

            pipeline = incident.get(

                "pipeline_name",

                "UNKNOWN"

            )

            counts[pipeline] = (

                counts.get(

                    pipeline,

                    0

                ) + 1

            )

        ranked = sorted(

            counts.items(),

            key=lambda item: item[1],

            reverse=True

        )

        return [

            {

                "pipeline_name": name,

                "incident_count": count

            }

            for name, count in ranked[:limit]

        ]


###############################################################################
# Recommendation Engine
###############################################################################

    def recommendations(
        self,
        incidents
    ):
        """
        Generate operational recommendations.
        """

        recommendations = []

        if self.average_mttr(

            incidents

        ) > 20:

            recommendations.append(

                "Reduce incident recovery time."

            )

        critical_count = self.severity_distribution(

            incidents

        ).get(

            "CRITICAL",

            0

        )

        if critical_count > 0:

            recommendations.append(

                "Prioritize critical incident prevention."

            )

        if self.total_incidents(

            incidents

        ) > 10:

            recommendations.append(

                "Review recurring operational issues."

            )

        if not recommendations:

            recommendations.append(

                "Incident levels are within acceptable limits."

            )

        return recommendations


###############################################################################
# Executive Incident Summary
###############################################################################

    def executive_summary(
        self,
        incidents
    ):
        """
        Generate executive incident dashboard summary.
        """

        return {

            "generated_time":

                datetime.utcnow(),

            "total_incidents":

                self.total_incidents(

                    incidents

                ),

            "severity_distribution":

                self.severity_distribution(

                    incidents

                ),

            "status_distribution":

                self.status_distribution(

                    incidents

                ),

            "average_mttr":

                self.average_mttr(

                    incidents

                ),

            "root_cause_distribution":

                self.root_cause_distribution(

                    incidents

                ),

            "top_recurring_pipelines":

                self.top_recurring_pipelines(

                    incidents

                ),

            "recommendations":

                self.recommendations(

                    incidents

                )

        }


###############################################################################
# Incident Reports
###############################################################################

    def incident_reports(
        self,
        incidents
    ):
        """
        Generate report for every incident.
        """

        return [

            self.incident_summary(

                incident

            )

            for incident in incidents

        ]


###############################################################################
# Platform Incident Status
###############################################################################

    def platform_incident_status(
        self,
        incidents
    ):
        """
        Determine overall incident health.
        """

        critical = self.severity_distribution(

            incidents

        ).get(

            "CRITICAL",

            0

        )

        open_incidents = self.status_distribution(

            incidents

        ).get(

            "OPEN",

            0

        )

        if critical > 0:

            return "CRITICAL"

        if open_incidents > 5:

            return "WARNING"

        return "STABLE"


###############################################################################
# Generate Complete Incident Report
###############################################################################

    def generate_report(
        self,
        incidents,
        report_type="DAILY"
    ):
        """
        Generate complete incident report.
        """

        report_name = f"{report_type} Incident Report"

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

                "platform_status":

                    self.platform_incident_status(

                        incidents

                    ),

                "executive_summary":

                    self.executive_summary(

                        incidents

                    ),

                "incident_reports":

                    self.incident_reports(

                        incidents

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
        Return report metadata.
        """

        return {

            "report_engine":

                "Enterprise Incident Reporting Engine",

            "version":

                "1.0.0",

            "generated_at":

                datetime.utcnow(),

            "module":

                "incident_report.py"

        }
