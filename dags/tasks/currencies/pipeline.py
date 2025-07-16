from .db_utils import get_start_date_from_db
from .extract import fetch_currency_rates
from .load import upsert_currency_rates
from db.connection import get_connection
from configs.config import END_DATE, SCHEMAS

def run_currency_pipeline():
    print("Checking last date in prod table...")
    start_date = get_start_date_from_db()
    print(f"Fetching currency rates from {start_date} to {END_DATE}...")

    df = fetch_currency_rates(start_date, END_DATE)
    print(f"Retrieved {len(df)} rows.")

    if not df.empty:
        with get_connection() as conn:
            for schema in SCHEMAS:
                upsert_currency_rates(df, conn, schema)
    else:
        print("No new data to insert.")