"""
Kaggle Data Loader and Uploader to MinIO (S3-Compatible)

This module handles the retrieval and preparation of raw datasets 
from a Kaggle dataset, and uploads them to a MinIO (S3-compatible) storage bucket.

Workflow Overview:
1. Checks if raw data files already exist in MinIO
2. If not:
   - Checks if local raw files exist
   - If local files are missing, downloads from Kaggle using `kaggle.json` credentials
   - Sorts the files by relevant date columns (timestamp/date)
3. Uploads the raw files (online & offline) to the `MINIO_RAW` bucket

Key Functions:
- `bucket_exists()` / `file_exists_in_bucket()` — Check for bucket and file presence in MinIO
- `upload_file_to_s3()` — Uploads a single file to MinIO
- `download_and_prepare_kaggle_files()` — Downloads and cleans files from Kaggle
- `download_to_s3_raw()` — Main entry point that ensures bucket exists, fetches missing files, and uploads them
"""

import os
import shutil
import pandas as pd

# Tells the Kaggle API where to find Kaggle credentials (kaggle.json).
os.environ['KAGGLE_CONFIG_DIR'] = os.path.join(os.path.dirname(__file__), '../.kaggle')

from kaggle.api.kaggle_api_extended import KaggleApi
from botocore.exceptions import ClientError
from configs.config import ONLINE_FILE_NAME, OFFLINE_FILE_NAME, RAW_DATA_FOLDER,\
                    DOWNLOAD_TEMP, S3, DATASET, TMSTMP, DATE, MINIO_RAW

def bucket_exists(bucket):
    try:
        S3.head_bucket(Bucket=bucket)
        return True
    except ClientError:
        return False


def file_exists_in_bucket(bucket, key):
    try:
        S3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError:
        return False


def upload_file_to_s3(local_path, bucket, key):
    print(f"Uploading {local_path} to {bucket}/{key}...")
    S3.upload_file(local_path, bucket, key)
    print(f"Uploaded {key}")


def ensure_bucket_exists(bucket):
    if not bucket_exists(bucket):
        print(f"Creating bucket '{bucket}'...")
        S3.create_bucket(Bucket=bucket)
    else:
        print(f"Bucket '{bucket}' already exists.")


def local_files_exist():
    online_path = os.path.join(RAW_DATA_FOLDER, ONLINE_FILE_NAME)
    offline_path = os.path.join(RAW_DATA_FOLDER, OFFLINE_FILE_NAME)
    return os.path.exists(online_path) and os.path.exists(offline_path)


def upload_local_files():
    print("Found both files in 'raw_data'. Uploading to S3...")
    online_path = os.path.join(RAW_DATA_FOLDER, ONLINE_FILE_NAME)
    offline_path = os.path.join(RAW_DATA_FOLDER, OFFLINE_FILE_NAME)
    upload_file_to_s3(online_path, MINIO_RAW, ONLINE_FILE_NAME)
    upload_file_to_s3(offline_path, MINIO_RAW, OFFLINE_FILE_NAME)


def download_and_prepare_kaggle_files():
    print("Missing local files. Downloading from Kaggle...")

    os.makedirs(DOWNLOAD_TEMP, exist_ok=True)
    api = KaggleApi()
    api.authenticate()
    api.dataset_download_files(DATASET, path=DOWNLOAD_TEMP, unzip=True)

    os.makedirs(RAW_DATA_FOLDER, exist_ok=True)

    sort_info = {
        ONLINE_FILE_NAME: TMSTMP,
        OFFLINE_FILE_NAME: DATE
    }

    for file, sort_column in sort_info.items():
        temp_path = os.path.join(DOWNLOAD_TEMP, file)
        raw_path = os.path.join(RAW_DATA_FOLDER, file)

        print(f"Opening {file} and sorting by '{sort_column}'...")

        df = pd.read_csv(temp_path)
        df[sort_column] = pd.to_datetime(df[sort_column], errors="coerce")
        df = df.sort_values(by=sort_column)
        df.to_csv(raw_path, index=False)

        print(f"Sorted and saved '{file}' to '{raw_path}'.")
        upload_file_to_s3(raw_path, MINIO_RAW, file)

    shutil.rmtree(DOWNLOAD_TEMP)
    print("Cleaned up temporary files.")


def download_to_s3_raw():
    ensure_bucket_exists(MINIO_RAW)

    if file_exists_in_bucket(MINIO_RAW, ONLINE_FILE_NAME) and file_exists_in_bucket(MINIO_RAW, OFFLINE_FILE_NAME):
        print("Both files already exist in S3. Nothing to do.")
        return

    if local_files_exist():
        upload_local_files()
    else:
        download_and_prepare_kaggle_files()