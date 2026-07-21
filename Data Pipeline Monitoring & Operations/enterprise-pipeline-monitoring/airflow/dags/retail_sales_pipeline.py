"""
===============================================================================

File Name   : retail_sales_pipeline.py

Description :
Enterprise Retail Sales ETL Pipeline

Pipeline Flow

Landing
    ↓
File Validation
    ↓
Bronze Load
    ↓
Bronze Validation
    ↓
Silver Transform
    ↓
Silver Validation
    ↓
Gold Load
    ↓
Reconciliation
    ↓
Quality Report
    ↓
Email Notification

Author      : Enterprise Data Engineering Team
Version     : 2.0

===============================================================================
"""

from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

# -------------------------------------------------------------------------
# Project Scripts
# -------------------------------------------------------------------------

from scripts.bronze_load import run_bronze_load

from scripts.silver_transform import run_silver_transform

from scripts.gold_load import run_gold_load

from validation.validation_runner import ValidationRunner

from validation.quality_report import QualityReport

# -------------------------------------------------------------------------
# Future Custom Components
# -------------------------------------------------------------------------

# from plugins.custom_callbacks import (
#     on_failure_callback,
#     on_success_callback
# )

# from plugins.email_alert import send_pipeline_email

# from plugins.cloudwatch_metrics import publish_metrics

# -------------------------------------------------------------------------
# Default Arguments
# -------------------------------------------------------------------------

default_args = {

    "owner": "data_engineering",

    "depends_on_past": False,

    "email": [

        "dataops@company.com"

    ],

    "email_on_failure": True,

    "email_on_retry": False,

    "retries": 3,

    "retry_delay": timedelta(minutes=5),

    "retry_exponential_backoff": True,

    "max_retry_delay": timedelta(minutes=30),

    "execution_timeout": timedelta(hours=2)

}

# -------------------------------------------------------------------------
# DAG Definition
# -------------------------------------------------------------------------

with DAG(

    dag_id="enterprise_retail_sales_pipeline",

    description="Enterprise Retail Sales ETL Pipeline",

    schedule="0 1 * * *",

    start_date=datetime(2026, 1, 1),

    catchup=False,

    max_active_runs=1,

    default_args=default_args,

    tags=[

        "Retail",

        "Databricks",

        "Delta",

        "Monitoring",

        "Production"

    ]

) as dag:

      # =====================================================================
    # START & END
    # =====================================================================

    start = EmptyOperator(

        task_id="start"

    )

    end = EmptyOperator(

        task_id="end",

        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS

    )

    # =====================================================================
    # LANDING GROUP
    # =====================================================================

    with TaskGroup(

        group_id="landing_group"

    ) as landing_group:

        landing_file_check = PythonOperator(

            task_id="landing_file_check",

            python_callable=check_landing_files

        )

        file_validation = PythonOperator(

            task_id="file_validation",

            python_callable=run_file_validation

        )

        landing_file_check >> file_validation

    # =====================================================================
    # BRONZE GROUP
    # =====================================================================

    with TaskGroup(

        group_id="bronze_group"

    ) as bronze_group:

        bronze_load = PythonOperator(

            task_id="bronze_load",

            python_callable=run_bronze_load

        )

        bronze_validation = PythonOperator(

            task_id="bronze_validation",

            python_callable=run_bronze_validation

        )

        bronze_load >> bronze_validation

    # =====================================================================
    # SILVER GROUP
    # =====================================================================

    with TaskGroup(

        group_id="silver_group"

    ) as silver_group:

        silver_transform = PythonOperator(

            task_id="silver_transform",

            python_callable=run_silver_transform

        )

        silver_validation = PythonOperator(

            task_id="silver_validation",

            python_callable=run_silver_validation

        )

        silver_transform >> silver_validation

    # =====================================================================
    # GOLD GROUP
    # =====================================================================

    with TaskGroup(

        group_id="gold_group"

    ) as gold_group:

        gold_load = PythonOperator(

            task_id="gold_load",

            python_callable=run_gold_load

        )

        reconciliation_validation = PythonOperator(

            task_id="reconciliation_validation",

            python_callable=run_reconciliation_validation

        )

        gold_load >> reconciliation_validation

    # =====================================================================
    # MONITORING GROUP
    # =====================================================================

    with TaskGroup(

        group_id="monitoring_group"

    ) as monitoring_group:

        quality_report = PythonOperator(

            task_id="quality_report",

            python_callable=generate_quality_report

        )

        publish_cloudwatch = PythonOperator(

            task_id="publish_cloudwatch",

            python_callable=publish_metrics

        )

        email_notification = PythonOperator(

            task_id="email_notification",

            python_callable=send_pipeline_email

        )

        archive_files = PythonOperator(

            task_id="archive_processed_files",

            python_callable=archive_source_files

        )

        quality_report >> publish_cloudwatch

        publish_cloudwatch >> email_notification

        email_notification >> archive_files

      # =====================================================================
    # Pipeline Dependencies
    # =====================================================================

    start >> landing_group

    landing_group >> bronze_group

    bronze_group >> silver_group

    silver_group >> gold_group

    gold_group >> monitoring_group

    monitoring_group >> end

    # =====================================================================
    # Runtime Configuration
    # =====================================================================

    dag.doc_md = """
    # Enterprise Retail Sales Pipeline

    ## Pipeline Flow

    Landing Files
        ↓
    File Validation
        ↓
    Bronze Load
        ↓
    Bronze Validation
        ↓
    Silver Transformation
        ↓
    Silver Validation
        ↓
    Gold Load
        ↓
    Reconciliation
        ↓
    Quality Report
        ↓
    CloudWatch Metrics
        ↓
    Email Notification

    ## Owner

    Data Engineering Team

    ## SLA

    Daily execution before 05:00 AM

    ## Schedule

    Every day at 01:00 AM

    """

    # =====================================================================
    # Task Level SLA
    # =====================================================================

    bronze_load.sla = timedelta(

        minutes=30

    )

    silver_transform.sla = timedelta(

        minutes=45

    )

    gold_load.sla = timedelta(

        minutes=20

    )

    reconciliation_validation.sla = timedelta(

        minutes=15

    )

    quality_report.sla = timedelta(

        minutes=10

    )

    # =====================================================================
    # Failure Callback
    # =====================================================================

    for task in dag.tasks:

        task.on_failure_callback = on_failure_callback

    # =====================================================================
    # Success Callback
    # =====================================================================

    for task in dag.tasks:

        task.on_success_callback = on_success_callback

    # =====================================================================
    # Execution Timeout
    # =====================================================================

    for task in dag.tasks:

        task.execution_timeout = timedelta(

            hours=2

        )

    # =====================================================================
    # Retry Policy
    # =====================================================================

    for task in dag.tasks:

        task.retries = 3

        task.retry_delay = timedelta(

            minutes=5

        )

    # =====================================================================
    # Priority Weight
    # =====================================================================

    bronze_load.priority_weight = 100

    silver_transform.priority_weight = 90

    gold_load.priority_weight = 80

    reconciliation_validation.priority_weight = 70

    quality_report.priority_weight = 60

    # =====================================================================
    # Pool Configuration
    # =====================================================================

    bronze_load.pool = "databricks_pool"

    silver_transform.pool = "databricks_pool"

    gold_load.pool = "databricks_pool"

    # =====================================================================
    # Queue Configuration
    # =====================================================================

    bronze_load.queue = "spark"

    silver_transform.queue = "spark"

    gold_load.queue = "spark"

    quality_report.queue = "default"

    # =====================================================================
    # Documentation
    # =====================================================================

    start.doc_md = """
    Pipeline Start
    """

    landing_group.doc_md = """
    Landing File Checks

    • File Arrival
    • File Count
    • File Naming
    • File Validation
    """

    bronze_group.doc_md = """
    Bronze Layer Processing
    """

    silver_group.doc_md = """
    Silver Layer Processing
    """

    gold_group.doc_md = """
    Gold Layer Processing
    """

    monitoring_group.doc_md = """
    Monitoring

    • Quality Report
    • CloudWatch
    • Email
    • Archive
    """

    end.doc_md = """
    Pipeline Completed Successfully
    """
