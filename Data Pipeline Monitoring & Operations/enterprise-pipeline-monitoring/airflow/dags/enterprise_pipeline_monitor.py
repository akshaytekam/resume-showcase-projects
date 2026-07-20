from airflow import DAG

from airflow.operators.python import PythonOperator

from airflow.sensors.filesystem import FileSensor

from datetime import datetime,timedelta

from airflow.callbacks.callbacks import success_callback,failure_callback

from validation.file_arrival_validation import file_validation

from validation.schema_validation import schema_validation

from validation.duplicate_validation import duplicate_validation

from validation.business_rule_validation import business_validation

from monitoring.execution_logger import log_execution

from monitoring.email_alert import send_email

default_args={

"owner":"DataEngineering",

"depends_on_past":False,

"email_on_failure":True,

"retries":3,

"retry_delay":timedelta(minutes=5),

"on_success_callback":success_callback,

"on_failure_callback":failure_callback

}

with DAG(

dag_id="enterprise_pipeline_monitor",

start_date=datetime(2026,1,1),

schedule="@daily",

catchup=False,

default_args=default_args,

sla=timedelta(minutes=90)

) as dag:

    wait_sales=FileSensor(

        task_id="wait_sales_file",

        filepath="/opt/airflow/datasets/sales/sales_2026-07-01.csv",

        poke_interval=60,

        timeout=600

    )

    validate_file=PythonOperator(

        task_id="validate_file",

        python_callable=file_validation

    )

    schema_check=PythonOperator(

        task_id="schema_validation",

        python_callable=schema_validation

    )

    duplicate_check=PythonOperator(

        task_id="duplicate_validation",

        python_callable=duplicate_validation

    )

    business_rule=PythonOperator(

        task_id="business_validation",

        python_callable=business_validation

    )

    execution=PythonOperator(

        task_id="execution_logger",

        python_callable=log_execution

    )

    alert=PythonOperator(

        task_id="email_alert",

        python_callable=send_email

    )

    wait_sales>>validate_file>>schema_check>>duplicate_check>>business_rule>>execution>>alert
