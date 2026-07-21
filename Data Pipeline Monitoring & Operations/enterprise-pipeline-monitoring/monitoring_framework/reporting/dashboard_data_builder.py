"""
===============================================================================

File Name
    dashboard_data_builder.py

Description
    Enterprise Dashboard Data Builder

Features

    • KPI Cards
    • Dashboard Summary
    • Chart Datasets
    • Unified Dashboard Payload

Author
    Enterprise Data Engineering Team

===============================================================================
"""

from datetime import datetime

###############################################################################
# Dashboard Data Builder
###############################################################################

class DashboardDataBuilder:
    """
    Build dashboard-ready data structures from report outputs.
    """

    def __init__(self):

        self.generated_time = datetime.utcnow()

###############################################################################
# KPI Cards
###############################################################################

    def build_kpi_cards(
        self,
        metrics_summary,
        health_report,
        sla_report,
        incident_report
    ):
        """
        Build dashboard KPI cards.
        """

        return {

            "total_pipelines":

                metrics_summary.get(

                    "pipeline_count",

                    0

                ),

            "platform_health":

                health_report.get(

                    "platform_health_score",

                    0

                ),

            "sla_compliance":

                sla_report.get(

                    "sla_compliance",

                    0

                ),

            "total_incidents":

                incident_report.get(

                    "total_incidents",

                    0

                )

        }

###############################################################################
# Dashboard Summary
###############################################################################

    def build_summary(
        self,
        metrics_summary,
        health_report
    ):
        """
        Build top summary section.
        """

        return {

            "generated_time":

                datetime.utcnow(),

            "platform_health":

                health_report.get(

                    "platform_health_score",

                    0

                ),

            "successful_runs":

                metrics_summary.get(

                    "successful_runs",

                    0

                ),

            "failed_runs":

                metrics_summary.get(

                    "failed_runs",

                    0

                )

        }

###############################################################################
# Health Widget
###############################################################################

    def build_health_widget(
        self,
        health_report
    ):
        """
        Prepare health widget.
        """

        return {

            "health_score":

                health_report.get(

                    "platform_health_score",

                    0

                ),

            "distribution":

                health_report.get(

                    "health_distribution",

                    {}

                )

        }

###############################################################################
# SLA Widget
###############################################################################

    def build_sla_widget(
        self,
        sla_report
    ):
        """
        Prepare SLA widget.
        """

        return {

            "compliance":

                sla_report.get(

                    "sla_compliance",

                    0

                ),

            "breaches":

                sla_report.get(

                    "sla_breaches",

                    0

                )

        }


###############################################################################
# Incident Widget
###############################################################################

    def build_incident_widget(
        self,
        incident_report
    ):
        """
        Prepare incident dashboard widget.
        """

        return {

            "total_incidents":

                incident_report.get(

                    "total_incidents",

                    0

                ),

            "platform_status":

                incident_report.get(

                    "platform_status",

                    "UNKNOWN"

                ),

            "severity_distribution":

                incident_report.get(

                    "severity_distribution",

                    {}

                )

        }


###############################################################################
# Health Distribution Chart
###############################################################################

    def build_health_chart(
        self,
        health_report
    ):
        """
        Prepare health distribution chart data.
        """

        distribution = health_report.get(

            "health_distribution",

            {}

        )

        return {

            "labels":

                list(

                    distribution.keys()

                ),

            "values":

                list(

                    distribution.values()

                )

        }


###############################################################################
# SLA Distribution Chart
###############################################################################

    def build_sla_chart(
        self,
        sla_report
    ):
        """
        Prepare SLA delay distribution chart.
        """

        distribution = sla_report.get(

            "delay_distribution",

            {}

        )

        return {

            "labels":

                list(

                    distribution.keys()

                ),

            "values":

                list(

                    distribution.values()

                )

        }


###############################################################################
# Top Unhealthy Pipelines
###############################################################################

    def build_top_unhealthy_table(
        self,
        health_report
    ):
        """
        Prepare unhealthy pipeline table.
        """

        return health_report.get(

            "top_unhealthy_pipelines",

            []

        )


###############################################################################
# Top SLA Violators
###############################################################################

    def build_sla_violators_table(
        self,
        sla_report
    ):
        """
        Prepare SLA violators table.
        """

        return sla_report.get(

            "top_sla_violators",

            []

        )


###############################################################################
# Alert Summary Widget
###############################################################################

    def build_alert_summary(
        self,
        metrics_summary
    ):
        """
        Prepare alert summary widget.
        """

        return {

            "active_alerts":

                metrics_summary.get(

                    "active_alerts",

                    0

                ),

            "critical_alerts":

                metrics_summary.get(

                    "critical_alerts",

                    0

                ),

            "warning_alerts":

                metrics_summary.get(

                    "warning_alerts",

                    0

                )

        }


###############################################################################
# Dashboard Metadata
###############################################################################

    def dashboard_metadata(
        self
    ):
        """
        Return dashboard metadata.
        """

        return {

            "dashboard_name":

                "Enterprise Pipeline Monitoring Dashboard",

            "version":

                "1.0.0",

            "generated_at":

                datetime.utcnow(),

            "module":

                "dashboard_data_builder.py"

        }


###############################################################################
# Build Complete Dashboard Payload
###############################################################################

    def build_dashboard(
        self,
        metrics_summary,
        health_report,
        sla_report,
        incident_report
    ):
        """
        Build a complete dashboard payload.
        """

        return {

            "metadata":

                self.dashboard_metadata(),

            "summary":

                self.build_summary(

                    metrics_summary,

                    health_report

                ),

            "kpi_cards":

                self.build_kpi_cards(

                    metrics_summary,

                    health_report,

                    sla_report,

                    incident_report

                ),

            "widgets": {

                "health":

                    self.build_health_widget(

                        health_report

                    ),

                "sla":

                    self.build_sla_widget(

                        sla_report

                    ),

                "incidents":

                    self.build_incident_widget(

                        incident_report

                    ),

                "alerts":

                    self.build_alert_summary(

                        metrics_summary

                    )

            },

            "charts": {

                "health_distribution":

                    self.build_health_chart(

                        health_report

                    ),

                "sla_distribution":

                    self.build_sla_chart(

                        sla_report

                    )

            },

            "tables": {

                "top_unhealthy_pipelines":

                    self.build_top_unhealthy_table(

                        health_report

                    ),

                "top_sla_violators":

                    self.build_sla_violators_table(

                        sla_report

                    )

            }

        }


###############################################################################
# Export Payload
###############################################################################

    def export_payload(
        self,
        dashboard
    ):
        """
        Return dashboard payload for external systems.
        """

        return {

            "status": "SUCCESS",

            "generated_time":

                datetime.utcnow(),

            "dashboard":

                dashboard

        }


###############################################################################
# Dashboard Version
###############################################################################

    def dashboard_version(
        self
    ):
        """
        Return dashboard version.
        """

        return "1.0.0"
