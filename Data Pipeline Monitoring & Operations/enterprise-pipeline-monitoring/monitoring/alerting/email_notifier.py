"""
===============================================================================

File Name
    email_notifier.py

Description
    Enterprise Email Notification Service

Features

    • HTML Email
    • Multiple Recipients
    • Retry Support
    • SMTP Authentication
    • Alert Templates

Author
    Enterprise Data Engineering Team

===============================================================================
"""

import smtplib

from email.mime.text import MIMEText

from email.mime.multipart import MIMEMultipart

from datetime import datetime

from alerting.alert_config import (

    APPLICATION_NAME,

    ENVIRONMENT,

    SMTP_SERVER,

    SMTP_PORT,

    SMTP_USERNAME,

    SMTP_PASSWORD,

    EMAIL_FROM,

    EMAIL_TO,

    EMAIL_ENABLED

)

from monitoring.monitoring_logger import (

    log_info,

    log_error

)

###############################################################################
# Email Notifier
###############################################################################

class EmailNotifier:

    """
    Enterprise email notification service.
    """

    def __init__(self):

        self.server = None

###############################################################################
# Connect SMTP
###############################################################################

    def connect(self):
        """
        Connect to SMTP server.
        """

        if not EMAIL_ENABLED:

            log_info(

                "Email notifications are disabled."

            )

            return

        self.server = smtplib.SMTP(

            SMTP_SERVER,

            SMTP_PORT

        )

        self.server.starttls()

        self.server.login(

            SMTP_USERNAME,

            SMTP_PASSWORD

        )

        log_info(

            "SMTP connection established."

        )

###############################################################################
# Disconnect SMTP
###############################################################################

    def disconnect(self):
        """
        Close SMTP connection.
        """

        if self.server:

            self.server.quit()

            self.server = None

            log_info(

                "SMTP connection closed."

            )

###############################################################################
# Create Message
###############################################################################

    def create_message(
        self,
        subject,
        body
    ):
        """
        Create MIME email message.
        """

        message = MIMEMultipart()

        message["From"] = EMAIL_FROM

        message["To"] = ", ".join(

            EMAIL_TO

        )

        message["Subject"] = subject

        message.attach(

            MIMEText(

                body,

                "html"

            )

        )

        return message


import time

from alerting.alert_config import (

    MAX_NOTIFICATION_RETRIES,

    RETRY_WAIT_SECONDS

)

###############################################################################
# HTML Template
###############################################################################

    def build_html_message(
        self,
        severity,
        title,
        message,
        pipeline_name=APPLICATION_NAME,
        environment=ENVIRONMENT
    ):
        """
        Build HTML email body.
        """

        generated_time = datetime.utcnow().strftime(

            "%Y-%m-%d %H:%M:%S UTC"

        )

        html = f"""
        <html>

        <body>

            <h2>{severity} Alert</h2>

            <table border="1" cellpadding="8">

                <tr>

                    <td><b>Pipeline</b></td>

                    <td>{pipeline_name}</td>

                </tr>

                <tr>

                    <td><b>Environment</b></td>

                    <td>{environment}</td>

                </tr>

                <tr>

                    <td><b>Generated</b></td>

                    <td>{generated_time}</td>

                </tr>

            </table>

            <br>

            <h3>{title}</h3>

            <p>{message}</p>

            <hr>

            <p>

            Enterprise Data Engineering Monitoring Framework

            </p>

        </body>

        </html>
        """

        return html

###############################################################################
# Build Subject
###############################################################################

    def build_subject(
        self,
        severity,
        title
    ):
        """
        Build email subject.
        """

        return (

            f"[{severity}] "

            f"{APPLICATION_NAME} - {title}"

        )

###############################################################################
# Send Email
###############################################################################

    def send(
        self,
        severity,
        title,
        message
    ):
        """
        Send email notification.
        """

        if not EMAIL_ENABLED:

            log_info(

                "Email disabled."

            )

            return

        subject = self.build_subject(

            severity,

            title

        )

        html = self.build_html_message(

            severity,

            title,

            message

        )

        mime_message = self.create_message(

            subject,

            html

        )

        retry = 1

        while retry <= MAX_NOTIFICATION_RETRIES:

            try:

                if self.server is None:

                    self.connect()

                self.server.sendmail(

                    EMAIL_FROM,

                    EMAIL_TO,

                    mime_message.as_string()

                )

                log_info(

                    f"Email delivered: {subject}"

                )

                return True

            except Exception as ex:

                log_error(

                    f"Email attempt {retry} failed: {ex}"

                )

                retry += 1

                time.sleep(

                    RETRY_WAIT_SECONDS

                )

        log_error(

            "Email delivery failed."

        )

        return False

###############################################################################
# Send INFO Alert
###############################################################################

    def send_info(
        self,
        title,
        message
    ):
        """
        Send INFO alert.
        """

        return self.send(

            severity="INFO",

            title=title,

            message=message

        )

###############################################################################
# Send WARNING Alert
###############################################################################

    def send_warning(
        self,
        title,
        message
    ):
        """
        Send WARNING alert.
        """

        return self.send(

            severity="WARNING",

            title=title,

            message=message

        )

###############################################################################
# Send ERROR Alert
###############################################################################

    def send_error(
        self,
        title,
        message
    ):
        """
        Send ERROR alert.
        """

        return self.send(

            severity="ERROR",

            title=title,

            message=message

        )

###############################################################################
# Send CRITICAL Alert
###############################################################################

    def send_critical(
        self,
        title,
        message
    ):
        """
        Send CRITICAL alert.
        """

        return self.send(

            severity="CRITICAL",

            title=title,

            message=message

        )

###############################################################################
# Send Health Report
###############################################################################

    def send_health_report(
        self,
        report
    ):
        """
        Send pipeline health report.
        """

        title = "Daily Pipeline Health Report"

        body = f"""
        <h3>Pipeline Health Report</h3>

        <table border="1" cellpadding="6">

            <tr>
                <td><b>Health Score</b></td>
                <td>{report.get("health_score")}</td>
            </tr>

            <tr>
                <td><b>Status</b></td>
                <td>{report.get("status")}</td>
            </tr>

            <tr>
                <td><b>Risk Level</b></td>
                <td>{report.get("risk_level")}</td>
            </tr>

        </table>
        """

        return self.send(

            severity="INFO",

            title=title,

            message=body

        )

###############################################################################
# Send Daily Summary
###############################################################################

    def send_daily_summary(
        self,
        dashboard_summary
    ):
        """
        Send daily operational summary.
        """

        title = "Daily Pipeline Summary"

        body = f"""
        <h3>Pipeline Summary</h3>

        <pre>

{dashboard_summary}

        </pre>
        """

        return self.send(

            severity="INFO",

            title=title,

            message=body

        )

###############################################################################
# Close Service
###############################################################################

    def close(self):
        """
        Close notifier.
        """

        self.disconnect()

        log_info(

            "Email notifier stopped."

        )
