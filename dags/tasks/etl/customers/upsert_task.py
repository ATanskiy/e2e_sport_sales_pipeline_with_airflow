import pandas as pd
from db.connection import get_connection
from configs.config import S3, MINIO_TEMP, SCHEMAS
from dags.tasks.etl.customers.upsert_logic import upsert_customers
from dags.tasks.etl.customers.clean_before_upsert import clean_customers_df

def upsert_customers_task(**kwargs):
    key = kwargs['ti'].xcom_pull(key='customers_temp_file')
    obj = S3.get_object(Bucket=MINIO_TEMP, Key=key)
    df = pd.read_csv(obj['Body'])
    df = clean_customers_df(df)

    with get_connection() as conn:
        for schema in SCHEMAS:
            upsert_customers(df, conn, schema)