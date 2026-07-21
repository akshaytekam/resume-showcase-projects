"""
===============================================================================

File Name

    airflow_config.py

Description

    Enterprise Airflow Configuration

Author

    Enterprise Data Engineering Team

===============================================================================
"""

import os

from datetime import timedelta

###############################################################################
# Environment
###############################################################################

ENVIRONMENT = os.getenv(

    "ENVIRONMENT",

    "DEV"

)

PROJECT_NAME = "Retail Sales Pipeline"

PIPELINE_NAME = "Enterprise Retail Pipeline"

###############################################################################
# AWS
###############################################################################

AWS_REGION = "ap-south-1"

S3_BUCKET = "enterprise-retail-data"

LANDING_PATH = "landing/sales/"

ARCHIVE_PATH = "archive/sales/"

BRONZE_PATH = "bronze/sales/"

SILVER_PATH = "silver/sales/"

GOLD_PATH = "gold/sales/"

###############################################################################
# Airflow
###############################################################################

DAG_ID = "enterprise_retail_sales_pipeline"

SCHEDULE = "0 1 * * *"

OWNER = "data_engineering"

MAX_ACTIVE_RUNS = 1

CATCHUP = False

###############################################################################
# Retry
###############################################################################

RETRIES = 3

RETRY_DELAY = timedelta(

    minutes=5

)

MAX_RETRY_DELAY = timedelta(

    minutes=30

)

RETRY_EXPONENTIAL_BACKOFF = True

###############################################################################
# SLA
###############################################################################

PIPELINE_TIMEOUT = timedelta(

    hours=2

)

BRONZE_SLA = timedelta(

    minutes=30

)

SILVER_SLA = timedelta(

    minutes=45

)

GOLD_SLA = timedelta(

    minutes=20

)

QUALITY_REPORT_SLA = timedelta(

    minutes=10

)

###############################################################################
# Email
###############################################################################

EMAIL_RECIPIENTS = [

    "dataops@company.com",

    "etl.support@company.com"

]

EMAIL_ON_FAILURE = True

EMAIL_ON_RETRY = False

###############################################################################
# Databricks
###############################################################################

DATABRICKS_HOST = os.getenv(

    "DATABRICKS_HOST"

)

DATABRICKS_TOKEN = os.getenv(

    "DATABRICKS_TOKEN"

)

JOB_CLUSTER_ID = os.getenv(

    "JOB_CLUSTER_ID"

)

NOTEBOOK_BRONZE = "/Retail/Bronze"

NOTEBOOK_SILVER = "/Retail/Silver"

NOTEBOOK_GOLD = "/Retail/Gold"

###############################################################################
# CloudWatch
###############################################################################

ENABLE_CLOUDWATCH = True

METRIC_NAMESPACE = "RetailPipeline"

###############################################################################
# Grafana
###############################################################################

ENABLE_GRAFANA = True

GRAFANA_JOB_NAME = "Retail Sales Monitoring"

###############################################################################
# Logging
###############################################################################

LOG_LEVEL = "INFO"

LOG_RETENTION_DAYS = 30

###############################################################################
# Validation
###############################################################################

ENABLE_VALIDATION = True

ENABLE_RECONCILIATION = True

ENABLE_EMAIL_ALERT = True

###############################################################################
# Archive
###############################################################################

DELETE_SOURCE_AFTER_ARCHIVE = False

###############################################################################
# Runtime Tags
###############################################################################

PIPELINE_TAGS = [

    "Retail",

    "Databricks",

    "Delta",

    "Airflow",

    "Production"

]
