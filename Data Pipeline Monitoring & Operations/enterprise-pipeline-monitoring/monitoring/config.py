"""
===============================================================================

File Name
    config.py

Description
    Monitoring Configuration

Used By

    • Metrics Collector
    • SLA Monitor
    • Pipeline Health
    • CloudWatch
    • Incident Manager
    • Grafana
    • Health Report

Author
    Enterprise Data Engineering Team

===============================================================================
"""

###########################################################################
# Project Information
###########################################################################

PROJECT_NAME = "Enterprise Retail Data Platform"

PIPELINE_NAME = "Retail Sales Pipeline"

BUSINESS_DOMAIN = "Retail"

ENVIRONMENT = "DEV"

###########################################################################
# Monitoring
###########################################################################

ENABLE_MONITORING = True

MONITORING_INTERVAL_SECONDS = 300

HEALTH_REFRESH_INTERVAL = 60

###########################################################################
# SLA Thresholds
###########################################################################

BRONZE_SLA_MINUTES = 20

SILVER_SLA_MINUTES = 35

GOLD_SLA_MINUTES = 20

VALIDATION_SLA_MINUTES = 10

ARCHIVE_SLA_MINUTES = 10

###########################################################################
# Pipeline Health Score
###########################################################################

MAX_PIPELINE_HEALTH_SCORE = 100

SUCCESS_WEIGHT = 50

VALIDATION_WEIGHT = 20

SLA_WEIGHT = 20

DATA_QUALITY_WEIGHT = 10

###########################################################################
# Health Status Thresholds
###########################################################################

HEALTHY_THRESHOLD = 90

WARNING_THRESHOLD = 75

CRITICAL_THRESHOLD = 50

###########################################################################
# CloudWatch
###########################################################################

ENABLE_CLOUDWATCH = True

AWS_REGION = "ap-south-1"

CLOUDWATCH_NAMESPACE = "RetailPipeline"

METRIC_RETENTION_DAYS = 30

###########################################################################
# Grafana Dashboard
###########################################################################

ENABLE_GRAFANA = True

GRAFANA_URL = "http://grafana.company.com"

GRAFANA_DASHBOARD_NAME = "Retail Pipeline Dashboard"

GRAFANA_REFRESH_SECONDS = 30

###########################################################################
# Dashboard Metrics
###########################################################################

ENABLE_PIPELINE_HEALTH = True

ENABLE_SLA_MONITORING = True

ENABLE_DATA_QUALITY = True

ENABLE_CLUSTER_MONITORING = True

###########################################################################
# Email Notifications
###########################################################################

ENABLE_EMAIL_ALERTS = True

EMAIL_FROM = "airflow@company.com"

EMAIL_RECIPIENTS = [

    "dataops@company.com",

    "dataengineering@company.com"

]

EMAIL_SUBJECT_PREFIX = "[Retail Pipeline]"

###########################################################################
# Incident Management
###########################################################################

ENABLE_INCIDENTS = True

INCIDENT_PRIORITY = "HIGH"

AUTO_CREATE_INCIDENT = True

AUTO_CLOSE_INCIDENT = False

INCIDENT_OWNER = "Data Operations"

###########################################################################
# Retry Configuration
###########################################################################

MAX_RETRY_COUNT = 3

RETRY_DELAY_SECONDS = 300

###########################################################################
# Logging
###########################################################################

LOG_LEVEL = "INFO"

LOG_DIRECTORY = "./logs"

LOG_FILE_NAME = "pipeline_monitor.log"

ENABLE_CONSOLE_LOGGING = True

###########################################################################
# CloudWatch Alarm Thresholds
###########################################################################

PIPELINE_FAILURE_THRESHOLD = 1

VALIDATION_FAILURE_THRESHOLD = 5

SLA_BREACH_THRESHOLD = 1

MAX_TASK_DURATION_SECONDS = 1800

###########################################################################
# Data Quality Thresholds
###########################################################################

MIN_DATA_QUALITY_SCORE = 95

MAX_DUPLICATE_PERCENTAGE = 1.0

MAX_NULL_PERCENTAGE = 2.0

MIN_RECONCILIATION_PERCENTAGE = 99.5

###########################################################################
# Cluster Monitoring
###########################################################################

ENABLE_CLUSTER_METRICS = True

CPU_WARNING_PERCENT = 80

MEMORY_WARNING_PERCENT = 80

DISK_WARNING_PERCENT = 85

###########################################################################
# Health Report
###########################################################################

GENERATE_DAILY_HEALTH_REPORT = True

HEALTH_REPORT_DIRECTORY = "./reports"

REPORT_RETENTION_DAYS = 30

###########################################################################
# Archive
###########################################################################

ARCHIVE_MONITORING_REPORTS = True

ARCHIVE_DIRECTORY = "./archive"

###########################################################################
# Feature Flags
###########################################################################

ENABLE_SLACK_ALERTS = False

ENABLE_TEAMS_ALERTS = False

ENABLE_SMS_ALERTS = False

ENABLE_PAGERDUTY = False
