"""
===============================================================================

File Name
    monitoring_dashboard.py

Description
    Enterprise Monitoring Dashboard Builder

Features

    • Dashboard Summary
    • Health Metrics
    • SLA Metrics
    • Pipeline Metrics
    • Recommendations

Author
    Enterprise Data Engineering Team

===============================================================================
"""

from datetime import datetime

from monitoring.monitoring_logger import (
    log_info
)

###############################################################################
# Dashboard Builder
###############################################################################

class MonitoringDashboard:

    """
    Builds dashboard data for Operations.
    """

    def __init__(self):

        self.dashboard = {

            "generated_at": None,

            "pipeline": {},

            "metrics": {},

            "health": {},

            "sla": {},

            "recommendations": []

        }

###############################################################################
# Pipeline Information
###############################################################################

    def set_pipeline_info(
        self,
        pipeline_name,
        execution_date,
        environment
    ):
        """
        Store pipeline metadata.
        """

        self.dashboard["generated_at"] = datetime.utcnow().isoformat()

        self.dashboard["pipeline"] = {

            "pipeline_name": pipeline_name,

            "execution_date": execution_date,

            "environment": environment

        }

        log_info(

            f"Dashboard initialized for {pipeline_name}"

        )

###############################################################################
# Metrics Section
###############################################################################

    def set_metrics(
        self,
        metrics_summary
    ):
        """
        Store metrics collected during execution.
        """

        self.dashboard["metrics"] = metrics_summary

###############################################################################
# Health Section
###############################################################################

    def set_health(
        self,
        health_summary
    ):
        """
        Store pipeline health information.
        """

        self.dashboard["health"] = health_summary

###############################################################################
# SLA Section
###############################################################################

    def set_sla(
        self,
        sla_summary
    ):
        """
        Store SLA monitoring summary.
        """

        self.dashboard["sla"] = sla_summary


import json

###############################################################################
# Recommendations
###############################################################################

    def set_recommendations(
        self,
        recommendations
    ):
        """
        Store operational recommendations.
        """

        self.dashboard["recommendations"] = recommendations

###############################################################################
# Dashboard Summary
###############################################################################

    def build_summary(self):
        """
        Build executive dashboard summary.
        """

        metrics = self.dashboard.get("metrics", {})

        health = self.dashboard.get("health", {})

        sla = self.dashboard.get("sla", {})

        summary = {

            "pipeline_name":

                self.dashboard["pipeline"].get(
                    "pipeline_name"
                ),

            "environment":

                self.dashboard["pipeline"].get(
                    "environment"
                ),

            "health_score":

                health.get(
                    "health_score"
                ),

            "health_status":

                health.get(
                    "status"
                ),

            "pipeline_status":

                sla.get(
                    "pipeline_status"
                ),

            "sla_compliance":

                sla.get(
                    "task_compliance"
                ),

            "records_processed":

                metrics.get(
                    "records_written"
                )

        }

        return summary

###############################################################################
# Dashboard KPI Cards
###############################################################################

    def build_kpis(self):
        """
        Build KPI cards for dashboards.
        """

        metrics = self.dashboard["metrics"]

        health = self.dashboard["health"]

        sla = self.dashboard["sla"]

        return {

            "Records Read":

                metrics.get(
                    "records_read",
                    0
                ),

            "Records Written":

                metrics.get(
                    "records_written",
                    0
                ),

            "Failed Records":

                metrics.get(
                    "records_failed",
                    0
                ),

            "Health Score":

                health.get(
                    "health_score",
                    0
                ),

            "SLA Compliance":

                sla.get(
                    "task_compliance",
                    0
                )

        }

###############################################################################
# Export Dashboard JSON
###############################################################################

    def export_json(
        self,
        output_file
    ):
        """
        Export dashboard.
        """

        with open(
            output_file,
            "w"
        ) as file:

            json.dump(

                self.dashboard,

                file,

                indent=4,

                default=str

            )

        log_info(

            f"Dashboard exported to {output_file}"

        )

###############################################################################
# Dashboard Payload
###############################################################################

    def dashboard_payload(self):
        """
        Payload for Grafana/API.
        """

        return {

            "summary":

                self.build_summary(),

            "kpis":

                self.build_kpis(),

            "recommendations":

                self.dashboard["recommendations"]

        }

###############################################################################
# Print Dashboard
###############################################################################

    def print_dashboard(self):
        """
        Print dashboard summary.
        """

        summary = self.build_summary()

        kpis = self.build_kpis()

        log_info("=" * 80)

        log_info("PIPELINE MONITORING DASHBOARD")

        log_info("=" * 80)

        for key, value in summary.items():

            log_info(

                f"{key:<22}: {value}"

            )

        log_info("-" * 80)

        log_info("KPI SUMMARY")

        for key, value in kpis.items():

            log_info(

                f"{key:<22}: {value}"

            )

        log_info("-" * 80)

        log_info("RECOMMENDATIONS")

        for item in self.dashboard["recommendations"]:

            log_info(

                f" - {item}"

            )

        log_info("=" * 80)

###############################################################################
# Dashboard History
###############################################################################

    def add_history(
        self,
        previous_runs
    ):
        """
        Store previous pipeline execution summaries.
        """

        self.dashboard["history"] = previous_runs

###############################################################################
# Incident Summary
###############################################################################

    def set_incident_summary(
        self,
        open_incidents,
        resolved_incidents
    ):
        """
        Store incident statistics.
        """

        self.dashboard["incidents"] = {

            "open": open_incidents,

            "resolved": resolved_incidents,

            "total": open_incidents + resolved_incidents

        }

###############################################################################
# Executive Report
###############################################################################

    def build_executive_report(self):
        """
        Build executive-level dashboard report.
        """

        summary = self.build_summary()

        report = {

            "generated_at": self.dashboard["generated_at"],

            "pipeline": summary["pipeline_name"],

            "environment": summary["environment"],

            "pipeline_status": summary["pipeline_status"],

            "health_status": summary["health_status"],

            "health_score": summary["health_score"],

            "sla_compliance": summary["sla_compliance"],

            "recommendations":

                self.dashboard["recommendations"],

            "incidents":

                self.dashboard.get(

                    "incidents",

                    {}

                )

        }

        return report

###############################################################################
# API Response
###############################################################################

    def api_response(self):
        """
        Build response for REST API.
        """

        return {

            "status": "SUCCESS",

            "dashboard": self.dashboard

        }

###############################################################################
# Reset Dashboard
###############################################################################

    def reset(self):
        """
        Prepare dashboard for next pipeline execution.
        """

        self.dashboard = {

            "generated_at": None,

            "pipeline": {},

            "metrics": {},

            "health": {},

            "sla": {},

            "recommendations": [],

            "history": [],

            "incidents": {}

        }

        log_info(

            "Monitoring dashboard reset."

        )

###############################################################################
# Print Executive Report
###############################################################################

    def print_executive_report(self):
        """
        Print executive dashboard.
        """

        report = self.build_executive_report()

        log_info("=" * 80)

        log_info("EXECUTIVE PIPELINE REPORT")

        log_info("=" * 80)

        for key, value in report.items():

            log_info(

                f"{key:<20}: {value}"

            )

        log_info("=" * 80)
