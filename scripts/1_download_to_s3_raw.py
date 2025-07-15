import os
import sys
import shutil
import pandas as pd

# Tells the Kaggle API where to find Kaggle credentials (kaggle.json).
os.environ['KAGGLE_CONFIG_DIR'] = os.path.join(os.path.dirname(__file__), '../.kaggle')

from kaggle.api.kaggle_api_extended import KaggleApi
from botocore.exceptions import ClientError

#sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
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

def upload_file_to_S3(local_path, bucket, key):
    print(f"Uploading {local_path} to {bucket}/{key}...")
    S3.upload_file(local_path, bucket, key)
    print(f"Uploaded {key}")

# Step 1: Ensure bucket exists
if not bucket_exists(MINIO_RAW):
    print(f"Creating bucket '{MINIO_RAW}'...")
    S3.create_bucket(Bucket=MINIO_RAW)
else:
    print(f"Bucket '{MINIO_RAW}' already exists.")

# Step 2: If both files already exist in bucket, exit early
if file_exists_in_bucket(MINIO_RAW, ONLINE_FILE_NAME) and file_exists_in_bucket(MINIO_RAW, OFFLINE_FILE_NAME):
    print("Both files already exist in S3. Nothing to do.")
    exit(0)

# Step 3: If raw_data folder exists locally and has both files, upload them
online_path = os.path.join(RAW_DATA_FOLDER, ONLINE_FILE_NAME)
offline_path = os.path.join(RAW_DATA_FOLDER, OFFLINE_FILE_NAME)

if os.path.exists(online_path) and os.path.exists(offline_path):
    print("Found both files in 'raw_data'. Uploading to S3...")
    upload_file_to_S3(online_path, MINIO_RAW, ONLINE_FILE_NAME)
    upload_file_to_S3(offline_path, MINIO_RAW, OFFLINE_FILE_NAME)

else:
    print("Missing local files. Downloading from Kaggle...")

    os.makedirs(DOWNLOAD_TEMP, exist_ok=True)

    api = KaggleApi()
    api.authenticate()
    api.dataset_download_files(DATASET, path=DOWNLOAD_TEMP, unzip=True)

    os.makedirs(RAW_DATA_FOLDER, exist_ok=True)

    # Define which column to sort each file by
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

        upload_file_to_S3(raw_path, MINIO_RAW, file)
    shutil.rmtree(DOWNLOAD_TEMP)
    print("Cleaned up temporary files.")