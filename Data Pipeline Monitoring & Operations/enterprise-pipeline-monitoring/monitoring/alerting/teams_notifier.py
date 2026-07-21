"""
===============================================================================

File Name
    teams_notifier.py

Description
    Enterprise Microsoft Teams Notification Service

Features

    • Incoming Webhooks
    • Severity-Based Alerts
    • MessageCard Payloads
    • Retry Support
    • Operational Notifications

Author
    Enterprise Data Engineering Team

===============================================================================
"""

import json

import time

import requests

from datetime import datetime

from alerting.alert_config import (

    APPLICATION_NAME,

    ENVIRONMENT,

    TEAMS_ENABLED,

    TEAMS_WEBHOOK,

    MAX_NOTIFICATION_RETRIES,

    RETRY_WAIT_SECONDS

)

from monitoring.monitoring_logger import (

    log_info,

    log_error

)

###############################################################################
# Teams Notifier
###############################################################################

class TeamsNotifier:

    """
    Enterprise Microsoft Teams notification service.
    """

    def __init__(self):

        self.webhook = TEAMS_WEBHOOK

###############################################################################
# Theme Color
###############################################################################

    def get_theme_color(
        self,
        severity
    ):
        """
        Return MessageCard theme color.
        """

        colors = {

            "INFO": "36A64F",

            "WARNING": "FFC107",

            "ERROR": "FF9800",

            "CRITICAL": "D32F2F"

        }

        return colors.get(

            severity,

            "36A64F"

        )

###############################################################################
# Build MessageCard Payload
###############################################################################

    def build_payload(
        self,
        severity,
        title,
        message
    ):
        """
        Build Microsoft Teams MessageCard payload.
        """

        generated = datetime.utcnow().strftime(

            "%Y-%m-%d %H:%M:%S UTC"

        )

        payload = {

            "@type": "MessageCard",

            "@context": "https://schema.org/extensions",

            "summary": title,

            "themeColor": self.get_theme_color(

                severity

            ),

            "title": f"{severity} - {title}",

            "sections": [

                {

                    "facts": [

                        {

                            "name": "Pipeline",

                            "value": APPLICATION_NAME

                        },

                        {

                            "name": "Environment",

                            "value": ENVIRONMENT

                        },

                        {

                            "name": "Severity",

                            "value": severity

                        },

                        {

                            "name": "Generated",

                            "value": generated

                        }

                    ],

                    "text": message

                }

            ]

        }

        return payload


###############################################################################
# Send Notification
###############################################################################

    def send(
        self,
        severity,
        title,
        message
    ):
        """
        Send Microsoft Teams notification.
        """

        if not TEAMS_ENABLED:

            log_info(

                "Microsoft Teams notifications are disabled."

            )

            return False

        payload = self.build_payload(

            severity,

            title,

            message

        )

        retry = 1

        while retry <= MAX_NOTIFICATION_RETRIES:

            try:

                response = requests.post(

                    self.webhook,

                    headers={

                        "Content-Type": "application/json"

                    },

                    data=json.dumps(payload),

                    timeout=30

                )

                if response.status_code in (200, 202):

                    log_info(

                        f"Teams notification delivered: {title}"

                    )

                    return True

                log_error(

                    f"Teams returned "

                    f"{response.status_code}: "

                    f"{response.text}"

                )

            except Exception as ex:

                log_error(

                    f"Teams attempt {retry} failed: {ex}"

                )

            retry += 1

            time.sleep(

                RETRY_WAIT_SECONDS

            )

        log_error(

            "Microsoft Teams notification delivery failed."

        )

        return False

###############################################################################
# INFO Notification
###############################################################################

    def send_info(
        self,
        title,
        message
    ):
        """
        Send INFO notification.
        """

        return self.send(

            severity="INFO",

            title=title,

            message=message

        )

###############################################################################
# WARNING Notification
###############################################################################

    def send_warning(
        self,
        title,
        message
    ):
        """
        Send WARNING notification.
        """

        return self.send(

            severity="WARNING",

            title=title,

            message=message

        )

###############################################################################
# ERROR Notification
###############################################################################

    def send_error(
        self,
        title,
        message
    ):
        """
        Send ERROR notification.
        """

        return self.send(

            severity="ERROR",

            title=title,

            message=message

        )

###############################################################################
# CRITICAL Notification
###############################################################################

    def send_critical(
        self,
        title,
        message
    ):
        """
        Send CRITICAL notification.
        """

        return self.send(

            severity="CRITICAL",

            title=title,

            message=message

        )


###############################################################################
# Pipeline Health Notification
###############################################################################

    def send_health_report(
        self,
        health_summary
    ):
        """
        Send pipeline health report.
        """

        title = "Pipeline Health Report"

        message = f"""
**Health Score:** {health_summary.get('health_score')}

**Status:** {health_summary.get('status')}

**Risk Level:** {health_summary.get('risk_level')}

**Failed Tasks:** {health_summary.get('failed_tasks', 0)}

**Warning Tasks:** {health_summary.get('warning_tasks', 0)}
"""

        return self.send_info(

            title=title,

            message=message

        )

###############################################################################
# SLA Breach Notification
###############################################################################

    def send_sla_breach(
        self,
        sla_summary
    ):
        """
        Send SLA breach notification.
        """

        breached_tasks = sla_summary.get(

            "breached_tasks",

            []

        )

        task_details = "\n".join(

            [

                f"- {task['task']} "

                f"(Actual={task['duration']} min, "

                f"SLA={task['sla']} min)"

                for task in breached_tasks

            ]

        )

        message = f"""
Pipeline SLA Breached

Pipeline Status:
{sla_summary.get('pipeline_status')}

Compliance:
{sla_summary.get('task_compliance')}%

Breached Tasks:

{task_details}
"""

        return self.send_critical(

            title="Pipeline SLA Breached",

            message=message

        )

###############################################################################
# Dashboard Notification
###############################################################################

    def send_dashboard_summary(
        self,
        dashboard_summary
    ):
        """
        Send monitoring dashboard summary.
        """

        message = f"""
Pipeline:
{dashboard_summary.get('pipeline_name')}

Environment:
{dashboard_summary.get('environment')}

Health Status:
{dashboard_summary.get('health_status')}

Health Score:
{dashboard_summary.get('health_score')}

Pipeline Status:
{dashboard_summary.get('pipeline_status')}

SLA Compliance:
{dashboard_summary.get('sla_compliance')}%

Records Processed:
{dashboard_summary.get('records_processed')}
"""

        return self.send_info(

            title="Daily Monitoring Dashboard",

            message=message

        )

###############################################################################
# Bulk Notifications
###############################################################################

    def send_bulk(
        self,
        notifications
    ):
        """
        Send multiple Teams notifications.
        """

        results = []

        for notification in notifications:

            status = self.send(

                severity=notification["severity"],

                title=notification["title"],

                message=notification["message"]

            )

            results.append(status)

        return results

###############################################################################
# Shutdown
###############################################################################

    def close(self):
        """
        Shutdown Teams notifier.
        """

        log_info(

            "Microsoft Teams notifier stopped."

        )


