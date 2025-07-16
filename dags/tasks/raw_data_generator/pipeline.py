from datetime import datetime
from botocore.exceptions import ClientError
from configs.config import MINIO_UNPROCESSED, MINIO_RAW, ONLINE_FILE_NAME, OFFLINE_FILE_NAME, TMSTMP, DATE
from configs.config import S3
from .s3_io import read_csv_from_s3
from .extract import list_extracted_days, get_next_unextracted_date
from .transform import save_daily_batch

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
    print(f"Processing next day: {next_date.strftime('%Y/%m/%d')}")

    save_daily_batch(raw_online, raw_offline, next_date)

    print(f"\Done: Generated raw daily files for {next_date.strftime('%Y/%m/%d')}")