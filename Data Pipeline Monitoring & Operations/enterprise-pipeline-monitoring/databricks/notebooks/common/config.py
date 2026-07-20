"""
===============================================================================
File Name : config.py

Description:
    Centralized configuration for Databricks ETL Pipeline.

Business Purpose
----------------
Stores all configurable values used by Bronze, Silver,
Gold and Monitoring notebooks.

Changing values here automatically affects the
entire project.

Author:
Enterprise Data Engineering Team

Version:
1.0
===============================================================================
"""

import os

# =============================================================================
# ENVIRONMENT
# =============================================================================

ENVIRONMENT = os.getenv("PIPELINE_ENV", "DEV")

# =============================================================================
# STORAGE PATHS
# =============================================================================

CONFIG = {

    "DEV": {

        "landing_path": "/mnt/raw-data",

        "bronze_path": "/mnt/delta/bronze",

        "silver_path": "/mnt/delta/silver",

        "gold_path": "/mnt/delta/gold",

        "checkpoint_path": "/mnt/checkpoints",

        "audit_path": "/mnt/audit",

        "database": "retail_dev",

        "catalog": "main"

    },

    "QA": {

        "landing_path": "/mnt/qa/raw-data",

        "bronze_path": "/mnt/qa/delta/bronze",

        "silver_path": "/mnt/qa/delta/silver",

        "gold_path": "/mnt/qa/delta/gold",

        "checkpoint_path": "/mnt/qa/checkpoints",

        "audit_path": "/mnt/qa/audit",

        "database": "retail_qa",

        "catalog": "main"

    },

    "PROD": {

        "landing_path": "/mnt/prod/raw-data",

        "bronze_path": "/mnt/prod/delta/bronze",

        "silver_path": "/mnt/prod/delta/silver",

        "gold_path": "/mnt/prod/checkpoints",

        "checkpoint_path": "/mnt/prod/checkpoints",

        "audit_path": "/mnt/prod/audit",

        "database": "retail_prod",

        "catalog": "main"

    }

}

# =============================================================================
# SPARK CONFIGURATION
# =============================================================================

SPARK_CONFIG = {

    "spark.sql.shuffle.partitions": "200",

    "spark.sql.adaptive.enabled": "true",

    "spark.sql.adaptive.coalescePartitions.enabled": "true",

    "spark.sql.optimizer.dynamicPartitionPruning.enabled": "true",

    "spark.sql.session.timeZone": "UTC"

}

# =============================================================================
# DELTA CONFIGURATION
# =============================================================================

DELTA_CONFIG = {

    "mergeSchema": "true",

    "overwriteSchema": "true",

    "autoOptimize.optimizeWrite": "true",

    "autoOptimize.autoCompact": "true"

}

# =============================================================================
# PIPELINE CONFIGURATION
# =============================================================================

PIPELINE_CONFIG = {

    "batch_size": 100000,

    "max_retry": 3,

    "retry_interval_seconds": 60,

    "partition_column": "sale_date"

}

# =============================================================================
# MONITORING
# =============================================================================

MONITORING = {

    "enable_cloudwatch": True,

    "enable_grafana": True,

    "enable_email_alert": True,

    "sla_minutes": 60

}

# =============================================================================
# FUNCTIONS
# =============================================================================

def get_environment():
    """
    Returns current environment.
    """

    return ENVIRONMENT


def get_config():
    """
    Returns environment specific configuration.
    """

    return CONFIG[ENVIRONMENT]


def get_spark_config():
    """
    Returns Spark configuration.
    """

    return SPARK_CONFIG


def get_delta_config():
    """
    Returns Delta Lake configuration.
    """

    return DELTA_CONFIG


def get_pipeline_config():
    """
    Returns pipeline configuration.
    """

    return PIPELINE_CONFIG


def get_monitoring_config():
    """
    Returns monitoring configuration.
    """

    return MONITORING


# =============================================================================
# LOCAL TESTING
# =============================================================================

if __name__ == "__main__":

    print("Environment")

    print(get_environment())

    print()

    print("Pipeline Config")

    print(get_config())

    print()

    print("Spark Config")

    print(get_spark_config())
