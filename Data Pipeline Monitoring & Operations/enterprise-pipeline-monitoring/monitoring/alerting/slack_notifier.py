"""
===============================================================================

File Name
    slack_notifier.py

Description
    Enterprise Slack Notification Service

Features

    • Incoming Webhooks
    • Severity-Based Alerts
    • Rich Message Formatting
    • Retry Support
    • JSON Payloads

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

    SLACK_ENABLED,

    SLACK_WEBHOOK,

    SLACK_CHANNEL,

    MAX_NOTIFICATION_RETRIES,

    RETRY_WAIT_SECONDS

)

from monitoring.monitoring_logger import (

    log_info,

    log_error

)

###############################################################################
# Slack Notifier
###############################################################################

class SlackNotifier:

    """
    Enterprise Slack notification service.
    """

    def __init__(self):

        self.webhook = SLACK_WEBHOOK

###############################################################################
# Color Mapping
###############################################################################

    def get_color(
        self,
        severity
    ):
        """
        Slack attachment color.
        """

        colors = {

            "INFO": "#36a64f",

            "WARNING": "#ffcc00",

            "ERROR": "#ff6600",

            "CRITICAL": "#ff0000"

        }

        return colors.get(

            severity,

            "#36a64f"

        )

###############################################################################
# Build Slack Payload
###############################################################################

    def build_payload(
        self,
        severity,
        title,
        message
    ):
        """
        Build Slack webhook payload.
        """

        timestamp = datetime.utcnow().strftime(

            "%Y-%m-%d %H:%M:%S UTC"

        )

        payload = {

            "channel": SLACK_CHANNEL,

            "attachments": [

                {

                    "color": self.get_color(

                        severity

                    ),

                    "title": title,

                    "text": message,

                    "fields": [

                        {

                            "title": "Pipeline",

                            "value": APPLICATION_NAME,

                            "short": True

                        },

                        {

                            "title": "Environment",

                            "value": ENVIRONMENT,

                            "short": True

                        },

                        {

                            "title": "Severity",

                            "value": severity,

                            "short": True

                        },

                        {

                            "title": "Generated",

                            "value": timestamp,

                            "short": True

                        }

                    ]

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
        Send Slack notification.
        """

        if not SLACK_ENABLED:

            log_info(

                "Slack notifications are disabled."

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

                if response.status_code == 200:

                    log_info(

                        f"Slack notification delivered: {title}"

                    )

                    return True

                log_error(

                    f"Slack returned "

                    f"{response.status_code}: "

                    f"{response.text}"

                )

            except Exception as ex:

                log_error(

                    f"Slack attempt {retry} failed: {ex}"

                )

            retry += 1

            time.sleep(

                RETRY_WAIT_SECONDS

            )

        log_error(

            "Slack notification delivery failed."

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
• Health Score : {health_summary.get('health_score')}

• Status : {health_summary.get('status')}

• Risk Level : {health_summary.get('risk_level')}

• Failed Tasks : {health_summary.get('failed_tasks', 0)}

• Warning Tasks : {health_summary.get('warning_tasks', 0)}
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

        breached = sla_summary.get(

            "breached_tasks",

            []

        )

        task_list = "\n".join(

            [

                f"• {task['task']} "

                f"(Actual={task['duration']} min, "

                f"SLA={task['sla']} min)"

                for task in breached

            ]

        )

        message = f"""
Pipeline SLA Breached

Pipeline Status:
{sla_summary.get('pipeline_status')}

Compliance:
{sla_summary.get('task_compliance')}%

Breached Tasks:

{task_list}
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
        Send dashboard summary.
        """

        message = f"""
Pipeline:
{dashboard_summary.get('pipeline_name')}

Environment:
{dashboard_summary.get('environment')}

Health:
{dashboard_summary.get('health_status')}

Health Score:
{dashboard_summary.get('health_score')}

SLA:
{dashboard_summary.get('pipeline_status')}

Compliance:
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
        Send multiple notifications.
        """

        results = []

        for notification in notifications:

            result = self.send(

                severity=notification["severity"],

                title=notification["title"],

                message=notification["message"]

            )

            results.append(result)

        return results

###############################################################################
# Shutdown
###############################################################################

    def close(self):
        """
        Close Slack notifier.
        """

        log_info(

            "Slack notifier stopped."

        )
