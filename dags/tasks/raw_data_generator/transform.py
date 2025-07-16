from configs.config import TMSTMP, DATE, DATE_FORMAT, MINIO_UNPROCESSED
from .s3_io import write_csv_to_s3

def save_daily_batch(raw_online, raw_offline, target_date):
    online_day = raw_online[raw_online[TMSTMP].dt.date == target_date.date()]
    offline_day = raw_offline[raw_offline[DATE].dt.date == target_date.date()]

    year, month, day = target_date.strftime(DATE_FORMAT).split("/")
    prefix = f"{year}/{month}/{day}/"

    write_csv_to_s3(online_day, MINIO_UNPROCESSED, f"{prefix}online.csv")
    write_csv_to_s3(offline_day, MINIO_UNPROCESSED, f"{prefix}offline.csv")