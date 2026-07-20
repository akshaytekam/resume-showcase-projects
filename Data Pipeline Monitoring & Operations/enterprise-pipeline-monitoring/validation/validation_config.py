"""
===============================================================================

File Name   : validation_config.py

Description :
Central configuration file for the Enterprise Data Validation Framework.

All validation modules should import this file instead of
hardcoding values.

Author      : Enterprise Data Engineering Team

Version     : 2.0

===============================================================================
"""

from dataclasses import dataclass
from typing import List


# =============================================================================
# Storage Configuration
# =============================================================================

@dataclass(frozen=True)
class StorageConfig:

    LANDING_PATH: str = "s3://retail-data/landing/sales"

    BRONZE_PATH: str = "s3://retail-data/bronze/sales"

    SILVER_PATH: str = "s3://retail-data/silver/sales"

    GOLD_PATH: str = "s3://retail-data/gold/sales"

    BAD_RECORD_PATH: str = "s3://retail-data/quarantine"

    AUDIT_PATH: str = "s3://retail-data/audit"

    VALIDATION_REPORT_PATH: str = \
        "s3://retail-data/validation_reports"


# =============================================================================
# Pipeline Configuration
# =============================================================================

@dataclass(frozen=True)
class PipelineConfig:

    PIPELINE_NAME: str = "Retail Batch Pipeline"

    ENVIRONMENT: str = "PROD"

    BUSINESS_DOMAIN: str = "Retail"

    EXPECTED_DAILY_FILES: int = 500

    FILE_EXTENSION: str = ".csv"

    DATE_FORMAT: str = "yyyy-MM-dd"

    TIMEZONE: str = "UTC"


# =============================================================================
# Schema Configuration
# =============================================================================

@dataclass(frozen=True)
class SchemaConfig:

    REQUIRED_COLUMNS: List[str] = (
        "sale_id",
        "store_id",
        "customer_id",
        "product_id",
        "sale_date",
        "quantity",
        "price",
        "sales_amount"
    )

    PRIMARY_KEY: List[str] = (
        "sale_id",
    )


# =============================================================================
# Validation Thresholds
# =============================================================================

@dataclass(frozen=True)
class ValidationThresholds:

    MAX_NULL_PERCENTAGE: float = 5.0

    MAX_DUPLICATE_PERCENTAGE: float = 0.0

    MAX_REJECTED_RECORD_PERCENTAGE: float = 10.0

    MIN_RECORD_COUNT: int = 100

    MAX_RECORD_COUNT: int = 10000000

    MAX_RUNTIME_MINUTES: int = 30


# =============================================================================
# Business Rules
# =============================================================================

@dataclass(frozen=True)
class BusinessRules:

    MIN_PRICE: float = 0.01

    MAX_PRICE: float = 100000.00

    MIN_QUANTITY: int = 1

    MAX_QUANTITY: int = 1000

    ALLOW_FUTURE_DATE: bool = False

    ALLOW_NEGATIVE_REVENUE: bool = False


# =============================================================================
# Alert Configuration
# =============================================================================

@dataclass(frozen=True)
class AlertConfig:

    ENABLE_EMAIL_ALERTS: bool = True

    ENABLE_CLOUDWATCH_ALERTS: bool = True

    ENABLE_GRAFANA_ALERTS: bool = True

    ENABLE_SLACK_ALERTS: bool = False

    EMAIL_RECIPIENTS = [

        "dataops@company.com",

        "support@company.com"

    ]


# =============================================================================
# Logging Configuration
# =============================================================================

@dataclass(frozen=True)
class LoggingConfig:

    LOG_LEVEL: str = "INFO"

    LOG_FORMAT: str = (
        "%(asctime)s "
        "%(levelname)s "
        "%(message)s"
    )

    ENABLE_CONSOLE_LOG: bool = True

    ENABLE_FILE_LOG: bool = True


# =============================================================================
# Validation Names
# =============================================================================

@dataclass(frozen=True)
class ValidationNames:

    FILE_VALIDATION = "File Validation"

    SCHEMA_VALIDATION = "Schema Validation"

    NULL_VALIDATION = "NULL Validation"

    DUPLICATE_VALIDATION = "Duplicate Validation"

    BUSINESS_VALIDATION = "Business Validation"

    RECONCILIATION_VALIDATION = "Reconciliation Validation"

    QUALITY_REPORT = "Quality Report"


# =============================================================================
# Singleton Config Objects
# =============================================================================

storage = StorageConfig()

pipeline = PipelineConfig()

schema = SchemaConfig()

threshold = ValidationThresholds()

business = BusinessRules()

alerts = AlertConfig()

logging_config = LoggingConfig()

validation_names = ValidationNames()
