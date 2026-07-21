"""
===============================================================================

File Name
    alert_config.py

Description
    Enterprise Alert Configuration

Author
    Enterprise Data Engineering Team

===============================================================================
"""

###############################################################################
# Environment
###############################################################################

ENVIRONMENT = "PROD"

APPLICATION_NAME = "Retail Sales Pipeline"

###############################################################################
# Email Configuration
###############################################################################

EMAIL_ENABLED = True

SMTP_SERVER = "smtp.company.com"

SMTP_PORT = 587

SMTP_USERNAME = "dataops@company.com"

SMTP_PASSWORD = "change_me"

EMAIL_FROM = "dataops@company.com"

EMAIL_TO = [

    "dataops@company.com",

    "platform.support@company.com"

]

###############################################################################
# Slack Configuration
###############################################################################

SLACK_ENABLED = True

SLACK_WEBHOOK = (

    "https://hooks.slack.com/services/"

    "XXXXXXXX/XXXXXXXX/XXXXXXXX"

)

SLACK_CHANNEL = "#dataops-alerts"

###############################################################################
# Microsoft Teams
###############################################################################

TEAMS_ENABLED = True

TEAMS_WEBHOOK = (

    "https://company.webhook.office.com/"

    "xxxxxxxx"

)

###############################################################################
# Incident Management
###############################################################################

INCIDENT_ENABLED = True

INCIDENT_SYSTEM = "ServiceNow"

DEFAULT_ASSIGNMENT_GROUP = "Data Engineering Support"

###############################################################################
# Severity Levels
###############################################################################

INFO = "INFO"

WARNING = "WARNING"

ERROR = "ERROR"

CRITICAL = "CRITICAL"

###############################################################################
# Escalation Delay (Minutes)
###############################################################################

WARNING_DELAY = 30

ERROR_DELAY = 15

CRITICAL_DELAY = 5

###############################################################################
# Duplicate Alert Protection
###############################################################################

SUPPRESS_DUPLICATE_ALERTS = True

DUPLICATE_WINDOW_MINUTES = 20

###############################################################################
# Retry Configuration
###############################################################################

MAX_NOTIFICATION_RETRIES = 3

RETRY_WAIT_SECONDS = 30
