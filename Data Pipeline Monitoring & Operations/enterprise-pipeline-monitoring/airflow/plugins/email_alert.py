"""
===============================================================================

File Name
    email_alert.py

Description
    Enterprise Email Notification Framework

Features

    • Success Email
    • Failure Email
    • HTML Email Templates
    • Pipeline Summary
    • Error Summary
    • Airflow Log Links

Author
    Enterprise Data Engineering Team

===============================================================================
"""

import smtplib

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from include.airflow_config import (

    EMAIL_RECIPIENTS

)

###############################################################################
# SMTP Configuration
###############################################################################

SMTP_SERVER = "smtp.company.com"

SMTP_PORT = 587

SMTP_USERNAME = "airflow@company.com"

SMTP_PASSWORD = "CHANGE_ME"

###############################################################################
# HTML Header
###############################################################################

def html_header(title):

    return f"""

    <html>

    <head>

    <style>

    body {{

        font-family: Arial;

        font-size:14px;

    }}

    table {{

        border-collapse: collapse;

        width:100%;

    }}

    td, th {{

        border:1px solid #cccccc;

        padding:8px;

    }}

    th {{

        background:#eeeeee;

    }}

    </style>

    </head>

    <body>

    <h2>{title}</h2>

    """

###############################################################################
# HTML Footer
###############################################################################

def html_footer():

    return """

    <br>

    Regards,

    <br>

    Enterprise Data Engineering Team

    </body>

    </html>

    """

###############################################################################
# Email Sender
###############################################################################

def send_email(

    subject,

    html_content

):

    """
    Send HTML Email.
    """

    message = MIMEMultipart("alternative")

    message["Subject"] = subject

    message["From"] = SMTP_USERNAME

    message["To"] = ",".join(

        EMAIL_RECIPIENTS

    )

    message.attach(

        MIMEText(

            html_content,

            "html"

        )

    )

    smtp = smtplib.SMTP(

        SMTP_SERVER,

        SMTP_PORT

    )

    smtp.starttls()

    smtp.login(

        SMTP_USERNAME,

        SMTP_PASSWORD

    )

    smtp.sendmail(

        SMTP_USERNAME,

        EMAIL_RECIPIENTS,

        message.as_string()

    )

    smtp.quit()

    print(

        "Email sent successfully."

    )

###############################################################################
# Success Email
###############################################################################

def send_success_email(metadata):
    """
    Send pipeline success notification.
    """

    subject = (

        f"[SUCCESS] "

        f"{metadata['pipeline_name']}"

    )

    html = html_header(

        "Pipeline Execution Successful"

    )

    html += f"""

    <table>

        <tr>

            <th>Pipeline</th>

            <td>{metadata['pipeline_name']}</td>

        </tr>

        <tr>

            <th>DAG</th>

            <td>{metadata['dag_id']}</td>

        </tr>

        <tr>

            <th>Task</th>

            <td>{metadata['task_id']}</td>

        </tr>

        <tr>

            <th>Status</th>

            <td style="color:green;">
                SUCCESS
            </td>

        </tr>

        <tr>

            <th>Execution Date</th>

            <td>{metadata['execution_date']}</td>

        </tr>

        <tr>

            <th>Duration</th>

            <td>{metadata['duration']} seconds</td>

        </tr>

        <tr>

            <th>Log URL</th>

            <td>

                <a href="{metadata['log_url']}">

                Airflow Logs

                </a>

            </td>

        </tr>

    </table>

    """

    html += html_footer()

    send_email(

        subject,

        html

    )

###############################################################################
# Failure Email
###############################################################################

def send_failure_email(
    metadata,
    error_message
):
    """
    Send failure notification.
    """

    subject = (

        f"[FAILED] "

        f"{metadata['pipeline_name']}"

    )

    html = html_header(

        "Pipeline Execution Failed"

    )

    html += f"""

    <table>

        <tr>

            <th>Pipeline</th>

            <td>{metadata['pipeline_name']}</td>

        </tr>

        <tr>

            <th>DAG</th>

            <td>{metadata['dag_id']}</td>

        </tr>

        <tr>

            <th>Task</th>

            <td>{metadata['task_id']}</td>

        </tr>

        <tr>

            <th>Status</th>

            <td style="color:red;">
                FAILED
            </td>

        </tr>

        <tr>

            <th>Error</th>

            <td>

                {error_message}

            </td>

        </tr>

        <tr>

            <th>Retry</th>

            <td>{metadata['try_number']}</td>

        </tr>

        <tr>

            <th>Execution Date</th>

            <td>{metadata['execution_date']}</td>

        </tr>

        <tr>

            <th>Log URL</th>

            <td>

                <a href="{metadata['log_url']}">

                Airflow Logs

                </a>

            </td>

        </tr>

    </table>

    """

    html += html_footer()

    send_email(

        subject,

        html

    )
