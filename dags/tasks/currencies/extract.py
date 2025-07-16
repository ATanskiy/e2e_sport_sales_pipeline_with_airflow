import pandas as pd
import requests
from datetime import datetime, timedelta
from configs.config import FRANKFURTER_API_URL, BASE_CURRENCY, TARGET_CURRENCIES

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
    except Exception:
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
    df.ffill(inplace=True)

    return df.reset_index(drop=True)