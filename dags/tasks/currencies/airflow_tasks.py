import pandas as pd
from .db_utils import get_start_date_from_db
from .extract import fetch_currency_rates
from .load import upsert_currency_rates
from db.connection import get_connection
from configs.config import END_DATE, SCHEMAS

def fetch_currency_rates_task(**context):
    start_date = get_start_date_from_db()
    df = fetch_currency_rates(start_date, END_DATE)
    df["date"] = df["date"].dt.strftime("%Y-%m-%d")
    context['ti'].xcom_push(key="currency_data", value=df.to_dict(orient="records"))
    print(f"Pushed {len(df)} records to XCom")

def load_currency_rates_task(**context):
    records = context['ti'].xcom_pull(key="currency_data", task_ids="fetch_currency_rates_task")
    df = pd.DataFrame(records)
    df["date"] = pd.to_datetime(df["date"])

    if df.empty:
        print("No data to upsert")
        return

    with get_connection() as conn:
        for schema in SCHEMAS:
            upsert_currency_rates(df, conn, schema)
            
