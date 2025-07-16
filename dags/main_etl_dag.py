from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from dags.tasks.etl.detect_new_files_task import detect_new_files
from dags.tasks.etl.customers.transform_task import transform_customers_task
from dags.tasks.etl.customers.upsert_task import upsert_customers_task
from dags.tasks.etl.sales.transform_task import transform_sales_task
from dags.tasks.etl.sales.upsert_task import upsert_sales_task
from dags.tasks.etl.move_to_processed_task import move_to_processed_task

default_args = {
    'owner': 'alex',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='etl_pipeline',
    default_args=default_args,
    schedule_interval='*/2 * * * *',  # every 2 minutes
    catchup=False
) as dag:
    
    t1_detect = PythonOperator(
    task_id='collect_new_files',
    python_callable=detect_new_files,
    provide_context=True
)
    
    t2_transform_customers = PythonOperator(
    task_id='transform_customers',
    python_callable=transform_customers_task,
    provide_context=True
)
    
    t3_upsert_customers = PythonOperator(
    task_id='upsert_customers',
    python_callable=upsert_customers_task,
    provide_context=True
)
    
    t4_transform_sales = PythonOperator(
    task_id='transform_sales',
    python_callable=transform_sales_task,
    provide_context=True
)
    
    t5_upsert_sales = PythonOperator(
    task_id='upsert_sales',
    python_callable=upsert_sales_task,
    provide_context=True
)
    
    t6_move_to_processed = PythonOperator(
    task_id='move_to_processed',
    python_callable=move_to_processed_task,
    provide_context=True
)
    
    t1_detect >> t2_transform_customers >> t3_upsert_customers >>\
         t4_transform_sales >> t5_upsert_sales >> t6_move_to_processed