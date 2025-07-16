from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from dags.tasks.raw_data_generator.pipeline import generate_next_daily_raw_batch

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
    python_callable=generate_next_daily_raw_batch,
    dag=dag,
)