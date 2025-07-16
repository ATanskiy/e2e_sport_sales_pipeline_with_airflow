"""
Currency ETL Pipeline for Frankfurter API

This module handles the end-to-end process of fetching daily exchange rates
from the Frankfurter API and inserting them into a PostgreSQL database using
the `psycopg2` driver.

It supports both:
- Local CLI-style execution (`run_currency_pipeline`)
- Modular Airflow execution using two atomic tasks:
    1. `fetch_currency_rates_task`: Fetches rates and pushes them to XCom
    2. `load_currency_rates_task`: Pulls from XCom and upserts to DB

Main Components:
- `get_start_date_from_db()`: Detects the most recent date in the database
- `fetch_currency_rates()`: Calls the API for each day in the date range
- `upsert_currency_rates()`: Inserts or updates data in one or more schemas
- `run_currency_pipeline()`: Local one-click execution for testing
- Airflow task functions (`*_task`) for scheduled daily ingestion

Configuration is loaded via `config.py` and `.env`, supporting multiple schemas
and target currencies.

Usage:
- In production, schedule as an Airflow DAG
- For local testing, run `run_currency_pipeline()`
"""

import pandas as pd
import requests
from datetime import date, datetime, timedelta

# Load config
from configs.config import (
    FRANKFURTER_API_URL, BASE_CURRENCY, TARGET_CURRENCIES,
    SCHEMAS, START_DATE, END_DATE, CURRENCY_TABLE
)

# Load connection
from db.connection import get_connection

# Chech the latest date for currency rates
def get_start_date_from_db():
    """Return the next start date based on max date in prod table, or START_DATE if empty."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT MAX(date) FROM prod.{CURRENCY_TABLE};")
            result = cur.fetchone()[0]
            if result and result < date.today():
                next_day = result + timedelta(days=1)
                return next_day.strftime("%Y-%m-%d")
            if result:
                return result.strftime("%Y-%m-%d")
            else:
                return START_DATE


def fetch_currency_for_day(day: str) -> dict:
    url = f"{FRANKFURTER_API_URL}/{day}?from={BASE_CURRENCY}&to={','.join(TARGET_CURRENCIES)}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json().get("rates", {})
        return {
            "date": day,
            "to_euro": data.get("EUR"),
            "to_isl": data.get("ILS"),
            "currency": BASE_CURRENCY
        }
    except Exception as e:
        # If it's a weekend or holiday, skip (we'll fill later)
        return {
            "date": day,
            "to_euro": None,
            "to_isl": None,
            "currency": BASE_CURRENCY
        }


def fetch_currency_rates(start_date: str, end_date: str) -> pd.DataFrame:
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    delta = timedelta(days=1)

    all_records = []
    current = start
    while current <= end:
        day_str = current.strftime("%Y-%m-%d")
        print(f"Fetching {day_str}...")
        record = fetch_currency_for_day(day_str)
        all_records.append(record)
        current += delta

    df = pd.DataFrame(all_records)
    df["date"] = pd.to_datetime(df["date"])
    df.sort_values("date", inplace=True)

    # Forward fill weekends/holidays
    df.ffill(inplace=True)

    return df.reset_index(drop=True)


def upsert_currency_rates(df, conn, schema):
    """Upsert the currency DataFrame into the given schema."""
    if df.empty:
        print(f"No currency records to insert for schema '{schema}'.")
        return
    
    df = df[["currency", "to_euro", "to_isl", "date"]]

    newly_inserted_count = 0
    upserted_count = 0

    sql = f"""
        INSERT INTO {schema}.{CURRENCY_TABLE} 
            (currency, to_euro, to_isl, date)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (currency, date) DO UPDATE 
        SET 
            to_euro = EXCLUDED.to_euro,
            to_isl = EXCLUDED.to_isl
        RETURNING xmax;
    """

    with conn.cursor() as cur:
        for row in df.itertuples(index=False, name=None):
            cur.execute(sql, row)
            result = cur.fetchone()
            upserted_count += 1
            if result and result[0] == 0:
                newly_inserted_count += 1

    conn.commit()
    print(f"Upserted: {upserted_count} total records into {schema}.{CURRENCY_TABLE}")
    print(f"Newly inserted: {newly_inserted_count}")
    print(f"Updated: {upserted_count - newly_inserted_count}")

def run_currency_pipeline():
    """For local dev/testing. Equivalent of fetch + upsert"""
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
