import pandas as pd
from db.connection import get_connection
from configs.config import S3, MINIO_TEMP, SCHEMAS
from dags.tasks.etl.sales.upsert_logic import upsert_sales

def upsert_sales_task(**kwargs):
    key = kwargs['ti'].xcom_pull(key='sales_temp_file')
    obj = S3.get_object(Bucket=MINIO_TEMP, Key=key)
    df = pd.read_csv(obj['Body'])

    with get_connection() as conn:
        for schema in SCHEMAS:
            upsert_sales(df, conn, schema)