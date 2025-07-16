from datetime import datetime
from configs.config import S3, MINIO_TEMP
from dags.tasks.etl.customers.transform_logic import transform_customers
from dags.tasks.etl.prepare_files import download_and_concat

def transform_customers_task(**kwargs):
    new_files = kwargs['ti'].xcom_pull(key='new_files')
    online_files = [f for f in new_files if f.endswith("online.csv")]
    offline_files = [f for f in new_files if f.endswith("offline.csv")]

    online_df = download_and_concat(online_files).sort_values(by="tmstmp")
    offline_df = download_and_concat(offline_files).sort_values(by="date")

    df = transform_customers(online_df, offline_df)

    # Save to MinIO temp bucket
    key = f"temp/customers_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    S3.put_object(Bucket=MINIO_TEMP, Key=key, Body=df.to_csv(index=False))
    
    kwargs['ti'].xcom_push(key='customers_temp_file', value=key)