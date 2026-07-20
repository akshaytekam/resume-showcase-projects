"""
===============================================================================
File Name : spark_session.py

Description:
    Creates and configures a SparkSession for all Databricks notebooks.

Business Purpose
----------------
Provides a centralized SparkSession configuration so every notebook
uses the same settings.

Features
--------
✓ Delta Lake support
✓ Adaptive Query Execution
✓ Dynamic Partition Pruning
✓ Shuffle optimization
✓ Standard Spark configs

Author:
Enterprise Data Engineering Team

Version:
1.0
===============================================================================
"""

from pyspark.sql import SparkSession


def get_spark(app_name: str = "EnterpriseDataPipeline") -> SparkSession:
    """
    Creates and returns a configured SparkSession.

    Parameters
    ----------
    app_name : str
        Name of the Spark application.

    Returns
    -------
    SparkSession
    """

    spark = (
        SparkSession.builder
        .appName(app_name)

        # ------------------------------------------------------------------
        # Delta Lake
        # ------------------------------------------------------------------
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )

        # ------------------------------------------------------------------
        # Performance
        # ------------------------------------------------------------------
        .config(
            "spark.sql.adaptive.enabled",
            "true"
        )
        .config(
            "spark.sql.adaptive.coalescePartitions.enabled",
            "true"
        )
        .config(
            "spark.sql.optimizer.dynamicPartitionPruning.enabled",
            "true"
        )
        .config(
            "spark.sql.shuffle.partitions",
            "200"
        )

        # ------------------------------------------------------------------
        # Legacy Compatibility
        # ------------------------------------------------------------------
        .config(
            "spark.sql.legacy.timeParserPolicy",
            "LEGACY"
        )

        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


if __name__ == "__main__":

    spark = get_spark()

    print("Spark Version :", spark.version)

    print("Spark Session Created Successfully")
