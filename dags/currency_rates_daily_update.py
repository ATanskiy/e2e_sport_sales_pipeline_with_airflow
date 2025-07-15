from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Import the main function
from etl.etl_currencies import run_currency_pipeline

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),  # Backfill from here
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

dag = DAG(
    dag_id="currency_rates_daily_update",
    default_args=default_args,
    schedule_interval="@daily",         
    catchup=False,
    tags=["etl", "currency", "frankfurter"],
    description="Fetch and upsert daily currency rates into Postgres"
)

task = PythonOperator(
    task_id="run_currency_pipeline",
    python_callable=run_currency_pipeline,
    dag=dag
)