from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from data_generator.extract_raw_to_s3_daily import extract_one_day_to_unprocessed

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
}

dag = DAG(
    "prepare_raw_data_daily",
    default_args=default_args,
    schedule_interval="* * * * *",
    catchup=False,
)

task = PythonOperator(
    task_id="extract_one_day_to_unprocessed",
    python_callable=extract_one_day_to_unprocessed,
    dag=dag,
)