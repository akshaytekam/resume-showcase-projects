# Databricks notebook source

catalog_name = "quince_dev"

schemas = [
    "bronze",
    "silver",
    "gold",
    "audit"
]

for schema_name in schemas:
    spark.sql(
        f"CREATE SCHEMA IF NOT EXISTS "
        f"{catalog_name}.{schema_name}"
    )

print("Catalog:", catalog_name)

display(
    spark.sql(
        f"SHOW SCHEMAS IN {catalog_name}"
    )
)
