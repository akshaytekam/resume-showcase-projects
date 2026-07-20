from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os

dag = DAG(
    "daily_pipeline_monitor",
    start_date=datetime(2025,1,1),
    schedule="@daily",
    catchup=False
)

# Task 1 check files
def check_files():
    # File Arrival Validation
    required_files = [

"sales.csv",

"customer.csv",

"product.csv",

"inventory.csv"

]

missing=[]

for file in required_files:

    if not os.path.exists(f"datasets/{file}"):

        missing.append(file)

print(missing)

    print("Checking arrival of files...")

# Task 2 validate data
def validate():

    print("Running validation...")

# Task 3 load Databricks
def load():

    print("Loading Bronze Layer")





