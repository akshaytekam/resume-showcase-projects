from pyspark.sql import functions as F

# Replace with your actual ADLS storage account name.
storage_account = "<storage-account-name>"

landing_base = (
    f"abfss://landing@{storage_account}.dfs.core.windows.net"
)

# These paths match our ADF landing layout.
source_files = {
    "orders": (
        f"{landing_base}/orders/"
        "extract_date=2026-10-19/orders.csv"
    ),
    "order_lines": (
        f"{landing_base}/order_lines/"
        "extract_date=2026-10-19/order_lines.csv"
    ),
}

validation_results = []

for dataset_name, file_path in source_files.items():
    try:
        # Read the header without inferring data types.
        df = (
            spark.read
            .option("header", True)
            .option("inferSchema", False)
            .csv(file_path)
        )

        columns = df.columns
        normalized_columns = [c.strip().lower() for c in columns]

        issues = []

        if not columns:
            issues.append("No column headers found")

        if any(not c.strip() for c in columns):
            issues.append("Blank column name found")

        if len(normalized_columns) != len(set(normalized_columns)):
            issues.append("Duplicate column names found")

        # Check whether the file contains data rows.
        if not issues and df.limit(1).count() == 0:
            issues.append("File contains headers but no data rows")

        status = "PASS" if not issues else "FAIL"

        validation_results.append({
            "dataset_name": dataset_name,
            "file_path": file_path,
            "columns": columns,
            "status": status,
            "issues": issues
        })

    except Exception as exc:
        validation_results.append({
            "dataset_name": dataset_name,
            "file_path": file_path,
            "columns": [],
            "status": "FAIL",
            "issues": [str(exc)[:500]]
        })

for result in validation_results:
    print(f"\nDataset: {result['dataset_name']}")
    print(f"Status: {result['status']}")
    print(f"Columns: {result['columns']}")
    print(f"Issues: {result['issues']}")

failed_datasets = [
    r["dataset_name"]
    for r in validation_results
    if r["status"] != "PASS"
]

if failed_datasets:
    raise ValueError(
        f"Schema validation failed for: {failed_datasets}"
    )
