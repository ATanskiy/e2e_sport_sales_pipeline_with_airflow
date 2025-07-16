from configs.config import MINIO_UNPROCESSED
from configs.config import DATE_FORMAT, S3
from datetime import timedelta

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

def get_next_unextracted_date(start_date, extracted_days):
    current = start_date
    while current.strftime(DATE_FORMAT) in extracted_days:
        current += timedelta(days=1)
    return current
