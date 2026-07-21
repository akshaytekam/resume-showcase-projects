"""
===============================================================================

File Name
    sla_monitor.py

Description
    Enterprise SLA Monitoring Framework

Features

    • Task SLA Monitoring
    • Pipeline SLA Monitoring
    • SLA Compliance
    • SLA Breach Detection

Author
    Enterprise Data Engineering Team

===============================================================================
"""

from datetime import datetime

from monitoring.monitoring_logger import (

    log_info,

    log_warning

)

###############################################################################
# SLA Monitor
###############################################################################

class SLAMonitor:

    """
    Enterprise SLA Monitor.
    """

    def __init__(self):

        self.pipeline_name = None

        self.pipeline_start = None

        self.pipeline_end = None

        self.pipeline_duration = 0

        self.pipeline_sla = 0

        self.tasks = {}

###############################################################################
# Pipeline Start
###############################################################################

    def start_pipeline(

        self,

        pipeline_name,

        pipeline_sla_minutes

    ):

        self.pipeline_name = pipeline_name

        self.pipeline_sla = pipeline_sla_minutes

        self.pipeline_start = datetime.utcnow()

        log_info(

            f"SLA Monitoring Started : {pipeline_name}"

        )

###############################################################################
# Pipeline End
###############################################################################

    def end_pipeline(self):

        self.pipeline_end = datetime.utcnow()

        duration = (

            self.pipeline_end

            -

            self.pipeline_start

        ).total_seconds() / 60

        self.pipeline_duration = round(

            duration,

            2

        )

        log_info(

            f"Pipeline Duration : {self.pipeline_duration} Minutes"

        )

###############################################################################
# Register Task
###############################################################################

    def register_task(

        self,

        task_name,

        sla_minutes

    ):

        self.tasks[task_name] = {

            "sla": sla_minutes,

            "duration": 0,

            "status": "NOT_STARTED"

        }

###############################################################################
# Record Task Duration
###############################################################################

    def record_task(

        self,

        task_name,

        duration_minutes

    ):

        if task_name not in self.tasks:

            raise Exception(

                f"{task_name} not registered."

            )

        self.tasks[task_name]["duration"] = duration_minutes

###############################################################################
# Evaluate Task SLA
###############################################################################

    def evaluate_task(
        self,
        task_name
    ):
        """
        Evaluate SLA for a single task.
        """

        if task_name not in self.tasks:

            raise Exception(

                f"{task_name} not registered."

            )

        task = self.tasks[task_name]

        if task["duration"] <= task["sla"]:

            task["status"] = "PASSED"

        else:

            task["status"] = "BREACHED"

            log_warning(

                f"SLA Breach : {task_name}"

            )

        return task["status"]

###############################################################################
# Evaluate All Tasks
###############################################################################

    def evaluate_all_tasks(self):
        """
        Evaluate SLA for all registered tasks.
        """

        for task_name in self.tasks.keys():

            self.evaluate_task(

                task_name

            )

###############################################################################
# Evaluate Pipeline SLA
###############################################################################

    def evaluate_pipeline(self):
        """
        Evaluate overall pipeline SLA.
        """

        if self.pipeline_duration <= self.pipeline_sla:

            pipeline_status = "PASSED"

        else:

            pipeline_status = "BREACHED"

            log_warning(

                "Pipeline SLA Breached."

            )

        return pipeline_status

###############################################################################
# SLA Compliance Percentage
###############################################################################

    def calculate_compliance(self):
        """
        Calculate percentage of tasks meeting SLA.
        """

        total = len(

            self.tasks

        )

        if total == 0:

            return 0.0

        passed = sum(

            1

            for task in self.tasks.values()

            if task["status"] == "PASSED"

        )

        compliance = (

            passed

            / total

        ) * 100

        return round(

            compliance,

            2

        )

###############################################################################
# Breached Tasks
###############################################################################

    def get_breached_tasks(self):
        """
        Return all breached tasks.
        """

        breached = []

        for name, task in self.tasks.items():

            if task["status"] == "BREACHED":

                breached.append(

                    {

                        "task": name,

                        "sla": task["sla"],

                        "duration": task["duration"]

                    }

                )

        return breached

import json

###############################################################################
# SLA Summary
###############################################################################

    def build_summary(self):
        """
        Build SLA execution summary.
        """

        summary = {

            "pipeline_name": self.pipeline_name,

            "pipeline_duration": self.pipeline_duration,

            "pipeline_sla": self.pipeline_sla,

            "pipeline_status": self.evaluate_pipeline(),

            "task_compliance": self.calculate_compliance(),

            "breached_tasks": self.get_breached_tasks()

        }

        return summary

###############################################################################
# Dashboard Payload
###############################################################################

    def dashboard_payload(self):
        """
        Build dashboard payload.
        """

        summary = self.build_summary()

        return {

            "Pipeline": summary["pipeline_name"],

            "Pipeline SLA": summary["pipeline_sla"],

            "Pipeline Duration": summary["pipeline_duration"],

            "Pipeline Status": summary["pipeline_status"],

            "Compliance %": summary["task_compliance"],

            "Breached Tasks": len(summary["breached_tasks"])

        }

###############################################################################
# Export JSON
###############################################################################

    def export_json(
        self,
        output_file
    ):
        """
        Export SLA summary.
        """

        with open(
            output_file,
            "w"
        ) as file:

            json.dump(

                self.build_summary(),

                file,

                indent=4,

                default=str

            )

        log_info(

            f"SLA report exported to {output_file}"

        )

###############################################################################
# Alert Recommendations
###############################################################################

    def generate_alerts(self):
        """
        Generate operational alerts.
        """

        alerts = []

        if self.evaluate_pipeline() == "BREACHED":

            alerts.append(

                "Pipeline exceeded SLA."

            )

        for task in self.get_breached_tasks():

            alerts.append(

                f"{task['task']} exceeded SLA."

            )

        if len(alerts) == 0:

            alerts.append(

                "No SLA violations detected."

            )

        return alerts

###############################################################################
# Print SLA Report
###############################################################################

    def print_summary(self):
        """
        Print SLA summary.
        """

        summary = self.build_summary()

        log_info("=" * 80)

        log_info("PIPELINE SLA REPORT")

        log_info("=" * 80)

        log_info(

            f"Pipeline : {summary['pipeline_name']}"

        )

        log_info(

            f"Duration : {summary['pipeline_duration']} Minutes"

        )

        log_info(

            f"SLA : {summary['pipeline_sla']} Minutes"

        )

        log_info(

            f"Status : {summary['pipeline_status']}"

        )

        log_info(

            f"Compliance : {summary['task_compliance']}%"

        )

        if summary["breached_tasks"]:

            log_info("Breached Tasks:")

            for task in summary["breached_tasks"]:

                log_info(

                    f" - {task['task']} "

                    f"(SLA={task['sla']} min, "

                    f"Actual={task['duration']} min)"

                )

        else:

            log_info(

                "No SLA breaches."

            )

        log_info("=" * 80)

###############################################################################
# Reset Monitor
###############################################################################

    def reset(self):
        """
        Prepare monitor for next execution.
        """

        self.pipeline_name = None

        self.pipeline_start = None

        self.pipeline_end = None

        self.pipeline_duration = 0

        self.pipeline_sla = 0

        self.tasks = {}

        log_info(

            "SLA monitor reset."

        )
