"""
This script extracts daily slices from full raw sales data stored in MinIO.

It performs the following steps:
- Loads complete raw CSVs (online & offline) from the raw bucket
- Identifies the earliest date in the data
- Checks which dates already have per-day CSVs saved in the unprocessed-data bucket
- Finds the next missing day
- Filters data for that specific day
- Saves two daily CSVs (online and offline) under the structure: YYYY/MM/DD/

This enables downstream ETL processes to consume raw data incrementally,
one day at a time.
"""

import pandas as pd
from datetime import timedelta
from io import StringIO
from botocore.exceptions import ClientError
from configs.config import ONLINE_FILE_NAME, OFFLINE_FILE_NAME, S3, TMSTMP,\
                     DATE, DATE_FORMAT, MINIO_UNPROCESSED, MINIO_RAW

# List all YYYY/MM/DD folders already saved to unprocessed-data
def list_extracted_days():
    result = S3.list_objects_v2(Bucket=MINIO_UNPROCESSED)
    extracted = set()

    for obj in result.get("Contents", []):
        key = obj["Key"]
        parts = key.split("/")
        if len(parts) >= 3:
            year, month, day = parts[:3]
            extracted.add(f"{year}/{month}/{day}")

    return sorted(extracted)

# Returns the next day not already extracted, starting from the earliest raw date
def get_next_unextracted_date(start_date, extracted_days):
    current = start_date
    while current.strftime(DATE_FORMAT) in extracted_days:
        current += timedelta(days=1)
    return current

# Load and parse raw CSV from S3, sorted by date
def read_csv_from_s3(bucket, key, date_column):
    obj = S3.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(obj['Body'])
    df[date_column] = pd.to_datetime(df[date_column], errors='coerce')
    return df.sort_values(by=date_column)

# Save DataFrame as CSV to S3 under the specified key
def write_csv_to_s3(df, bucket, key):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    S3.put_object(Bucket=bucket, Key=key, Body=csv_buffer.getvalue())
    print(f"Saved {key}")

# Filter both dataframes by a specific day and write daily CSVs
def save_daily_batch(raw_online, raw_offline, target_date):
    online_day = raw_online[raw_online[TMSTMP].dt.date == target_date.date()]
    offline_day = raw_offline[raw_offline[DATE].dt.date == target_date.date()]

    year, month, day = target_date.strftime(DATE_FORMAT).split("/")
    prefix = f"{year}/{month}/{day}/"

    write_csv_to_s3(online_day, MINIO_UNPROCESSED, f"{prefix}online.csv")
    write_csv_to_s3(offline_day, MINIO_UNPROCESSED, f"{prefix}offline.csv")

# Main orchestration: extracts the next daily slice of raw data
def generate_next_daily_raw_batch():
    try:
        S3.head_bucket(Bucket=MINIO_UNPROCESSED)
    except ClientError:
        print(f"Creating bucket '{MINIO_UNPROCESSED}'...")
        S3.create_bucket(Bucket=MINIO_UNPROCESSED)

    print("Loading raw CSV files...")
    raw_online = read_csv_from_s3(MINIO_RAW, ONLINE_FILE_NAME, TMSTMP)
    raw_offline = read_csv_from_s3(MINIO_RAW, OFFLINE_FILE_NAME, DATE)

    earliest_date = min(raw_online[TMSTMP].min(), raw_offline[DATE].min())
    earliest_date = earliest_date.replace(hour=0, minute=0, second=0, microsecond=0)

    print("Checking already extracted days...")
    extracted_days = list_extracted_days()

    next_date = get_next_unextracted_date(earliest_date, extracted_days)
    print(f"Processing next day: {next_date.strftime(DATE_FORMAT)}")

    save_daily_batch(raw_online, raw_offline, next_date)

    print(f"✅ Done: Generated raw daily files for {next_date.strftime(DATE_FORMAT)}")
