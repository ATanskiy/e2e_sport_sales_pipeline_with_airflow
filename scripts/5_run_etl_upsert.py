import os
import sys
import pandas as pd
from botocore.exceptions import ClientError
from db.connection import get_connection
from etl.etl_customers import transform_customers, upsert_customers
from etl.etl_sales import transform_sales, upsert_sales

import warnings
warnings.filterwarnings("ignore", category=UserWarning, module="pandas.io.sql")

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from configs.config import S3, MINIO_PROCESSED, MINIO_UNPROCESSED

def list_all_keys(bucket_name):
    keys = set()
    paginator = S3.get_paginator('list_objects_v2')
    try:
        for page in paginator.paginate(Bucket=bucket_name):
            for obj in page.get('Contents', []):
                keys.add(obj['Key'])
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchBucket':
            print(f"Bucket '{bucket_name}' does not exist.")
        else:
            raise
    return keys

def ensure_processed_bucket_exists():
    try:
        S3.head_bucket(Bucket=MINIO_PROCESSED)
    except ClientError:
        print(f"Creating bucket '{MINIO_PROCESSED}'...")
        S3.create_bucket(Bucket=MINIO_PROCESSED)

def download_and_concat(file_list):
    dfs = []
    for key in file_list:
        obj = S3.get_object(Bucket=MINIO_UNPROCESSED, Key=key)
        df = pd.read_csv(obj["Body"])
        dfs.append(df)
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

def main():
    ensure_processed_bucket_exists()

    print("Listing files in unprocessed-data...")
    unprocessed_files = list_all_keys(MINIO_UNPROCESSED)

    print("Listing files in processed-data...")
    processed_files = list_all_keys(MINIO_PROCESSED)

    new_files = unprocessed_files - processed_files

    if not new_files:
        print("No new files to process.")
        return

    print("New files to process:")
    for file in sorted(new_files):
        print(f"  • {file}")

    online_files = [f for f in new_files if f.endswith("online.csv")]
    offline_files = [f for f in new_files if f.endswith("offline.csv")]

    if not online_files and not offline_files:
        print("No online or offline files found in the new files.")
        return

    # collect new data for online and offline sales
    online_df = download_and_concat(online_files).sort_values(by="tmstmp")
    offline_df = download_and_concat(offline_files).sort_values(by="date")

    # process new data for customers, upsert --> for sales
    
    with get_connection() as conn:
        customers_df = transform_customers(online_df, offline_df)
        for schema in ["prod", "playground"]:
            upsert_customers(customers_df, conn, schema)

        sales_df = transform_sales(online_df, offline_df, conn, schema='prod')
        for schema in ["prod", "playground"]:
            upsert_sales(sales_df, conn, schema)


    # Move processed files to processed-data bucket
    for key in sorted(new_files):
        copy_source = {"Bucket": MINIO_UNPROCESSED, "Key": key}
        S3.copy(copy_source, MINIO_PROCESSED, key)
        #s3.delete_object(Bucket=MINIO_UNPROCESSED, Key=key) turned off for a while
        print(f"Moved {key} to '{MINIO_PROCESSED}'")

    print("All done. Kol Hakavod")

if __name__ == "__main__":
    main()
