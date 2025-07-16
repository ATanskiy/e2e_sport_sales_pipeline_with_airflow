from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from dags.tasks.currencies.airflow_tasks import fetch_currency_rates_task,\
                                                 load_currency_rates_task

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),  # Backfill from here
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="currency_rates_daily_update",
    default_args=default_args,
    schedule_interval="@daily",         
    catchup=False,
    tags=["etl", "currency", "frankfurter"],
    description="Fetch and upsert daily currency rates into Postgres"
) as dag:

    fetch_task = PythonOperator(
        task_id="fetch_currency_rates_task",
        python_callable=fetch_currency_rates_task,
        provide_context=True
    )

    load_task = PythonOperator(
        task_id="load_currency_rates_task",
        python_callable=load_currency_rates_task,
        provide_context=True
    )

    fetch_task >> load_task