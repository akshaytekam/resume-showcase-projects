"""
===============================================================================

File Name
    config.py

Description
    Enterprise Configuration Management

Features

    • Environment Configuration
    • Monitoring Configuration
    • Alert Configuration
    • Reporting Configuration
    • Cloud Configuration
    • Global Constants

Author
    Enterprise Data Engineering Team

===============================================================================
"""

import os

###############################################################################
# Environment
###############################################################################

ENVIRONMENT = os.getenv(

    "MONITORING_ENV",

    "DEV"

).upper()


PROJECT_NAME = (

    "Enterprise Data Pipeline "

    "Monitoring Framework"

)

APPLICATION_NAME = "pipeline-monitor"

APPLICATION_VERSION = "1.0.0"

###############################################################################
# Cloud Configuration
###############################################################################

CLOUD_PROVIDER = "AWS"

AWS_REGION = "ap-south-1"

S3_BUCKET = "enterprise-data-monitoring"

###############################################################################
# Databricks Configuration
###############################################################################

DATABRICKS_WORKSPACE = (

    "https://adb-company.cloud.databricks.com"

)

DATABRICKS_CLUSTER = "monitoring-cluster"

###############################################################################
# Snowflake Configuration
###############################################################################

SNOWFLAKE_ACCOUNT = "company"

SNOWFLAKE_DATABASE = "MONITORING_DB"

SNOWFLAKE_SCHEMA = "PIPELINE_MONITORING"

SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"

###############################################################################
# Monitoring Configuration
###############################################################################

PIPELINE_CHECK_INTERVAL = 300

METRICS_COLLECTION_INTERVAL = 60

HEALTH_CHECK_INTERVAL = 120

###############################################################################
# SLA Configuration
###############################################################################

DEFAULT_SLA_MINUTES = 60

WARNING_THRESHOLD = 80

CRITICAL_THRESHOLD = 95

###############################################################################
# Email Configuration
###############################################################################

EMAIL_ENABLED = True

SMTP_SERVER = "smtp.company.com"

SMTP_PORT = 587

SMTP_USERNAME = "pipeline-monitor@company.com"

SMTP_SENDER = "pipeline-monitor@company.com"

DEFAULT_EMAIL_RECIPIENTS = [

    "dataengineering@company.com",

    "operations@company.com"

]

###############################################################################
# Slack Configuration
###############################################################################

SLACK_ENABLED = True

SLACK_WEBHOOK_URL = (

    "https://hooks.slack.com/services/"

    "XXXXXXXX/XXXXXXXX/XXXXXXXX"

)

SLACK_CHANNEL = "#pipeline-monitoring"

###############################################################################
# Microsoft Teams Configuration
###############################################################################

TEAMS_ENABLED = True

TEAMS_WEBHOOK_URL = (

    "https://company.webhook.office.com/"

    "incomingwebhook/xxxxxxxx"

)

###############################################################################
# Reporting Configuration
###############################################################################

REPORT_OUTPUT_DIRECTORY = "./reports"

REPORT_FORMAT = "CSV"

ENABLE_DAILY_REPORT = True

ENABLE_WEEKLY_REPORT = True

ENABLE_MONTHLY_REPORT = True

###############################################################################
# Dashboard Configuration
###############################################################################

ENABLE_POWERBI_EXPORT = True

ENABLE_GRAFANA_EXPORT = True

ENABLE_CLOUDWATCH_EXPORT = True

DASHBOARD_REFRESH_INTERVAL = 300

###############################################################################
# Logging Configuration
###############################################################################

LOG_DIRECTORY = "./logs"

LOG_LEVEL = "INFO"

LOG_FILE_NAME = "monitoring_framework.log"

ENABLE_CONSOLE_LOGGING = True

ENABLE_FILE_LOGGING = True

###############################################################################
# Retry Configuration
###############################################################################

MAX_RETRY_COUNT = 3

RETRY_INTERVAL_SECONDS = 30

EXPONENTIAL_BACKOFF = True

###############################################################################
# Archive Configuration
###############################################################################

ARCHIVE_DIRECTORY = "./archive"

ARCHIVE_AFTER_DAYS = 30

ENABLE_AUTO_ARCHIVE = True

###############################################################################
# Metrics Retention
###############################################################################

METRICS_RETENTION_DAYS = 90

INCIDENT_RETENTION_DAYS = 365

ALERT_RETENTION_DAYS = 180

###############################################################################
# Pipeline Configuration
###############################################################################

PIPELINE_DEFAULT_TIMEOUT_MINUTES = 120

MAX_PIPELINE_RETRIES = 2

ENABLE_INCREMENTAL_MONITORING = True

DEFAULT_PIPELINE_OWNER = "Data Engineering Team"

###############################################################################
# Health Score Configuration
###############################################################################

HEALTH_SCORE_EXCELLENT = 95

HEALTH_SCORE_GOOD = 85

HEALTH_SCORE_WARNING = 70

HEALTH_SCORE_CRITICAL = 50

###############################################################################
# Validation Configuration
###############################################################################

ENABLE_SCHEMA_VALIDATION = True

ENABLE_NULL_VALIDATION = True

ENABLE_DUPLICATE_VALIDATION = True

ENABLE_BUSINESS_VALIDATION = True

ENABLE_RECONCILIATION_VALIDATION = True

QUALITY_SCORE_THRESHOLD = 95

###############################################################################
# Feature Flags
###############################################################################

FEATURE_ALERTING = True

FEATURE_REPORTING = True

FEATURE_DASHBOARD = True

FEATURE_INCIDENT_MANAGEMENT = True

FEATURE_ESCALATION = True

###############################################################################
# Utility Functions
###############################################################################

def is_production():
    """
    Return True if running in PROD.
    """

    return ENVIRONMENT == "PROD"


def is_development():
    """
    Return True if running in DEV.
    """

    return ENVIRONMENT == "DEV"


def get_environment():
    """
    Return current environment.
    """

    return ENVIRONMENT


###############################################################################
# Configuration Validation
###############################################################################

def validate_configuration():
    """
    Validate critical configuration values.
    """

    if DEFAULT_SLA_MINUTES <= 0:

        raise ValueError(

            "DEFAULT_SLA_MINUTES must be greater than zero."

        )


    if MAX_RETRY_COUNT < 0:

        raise ValueError(

            "MAX_RETRY_COUNT cannot be negative."

        )


    if QUALITY_SCORE_THRESHOLD < 0 or QUALITY_SCORE_THRESHOLD > 100:

        raise ValueError(

            "QUALITY_SCORE_THRESHOLD must be between 0 and 100."

        )


    return True


###############################################################################
# Configuration Summary
###############################################################################

def configuration_summary():
    """
    Return configuration summary.
    """

    return {

        "project":

            PROJECT_NAME,

        "version":

            APPLICATION_VERSION,

        "environment":

            ENVIRONMENT,

        "cloud":

            CLOUD_PROVIDER,

        "region":

            AWS_REGION,

        "sla_minutes":

            DEFAULT_SLA_MINUTES,

        "report_format":

            REPORT_FORMAT,

        "log_level":

            LOG_LEVEL,

        "alerting":

            FEATURE_ALERTING,

        "reporting":

            FEATURE_REPORTING

    }


###############################################################################
# Startup Validation
###############################################################################

validate_configuration()
