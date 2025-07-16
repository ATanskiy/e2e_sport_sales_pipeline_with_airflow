from configs.config import S3, MINIO_PROCESSED, MINIO_UNPROCESSED, MINIO_TEMP
from botocore.exceptions import ClientError

def list_all_keys(bucket_name):
    keys = set()
    paginator = S3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket_name):
        for obj in page.get('Contents', []):
            keys.add(obj['Key'])
    return keys

def ensure_bucket_exists(bucket_name):
    try:
        S3.head_bucket(Bucket=bucket_name)
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ['404', 'NoSuchBucket']:
            print(f"Creating bucket '{bucket_name}'...")
            try:
                S3.create_bucket(Bucket=bucket_name)
            except ClientError as ce:
                if ce.response['Error']['Code'] == "BucketAlreadyOwnedByYou":
                    print(f"⚠️ Bucket '{bucket_name}' already exists and is owned by you. Skipping.")
                else:
                    raise
        elif error_code == '403':
            raise PermissionError(f"Access denied to bucket: {bucket_name}")
        elif error_code == '301':
            raise ValueError(f"⚠️ Bucket exists in a different region. Check MinIO config or region settings.")
        else:
            raise

def detect_new_files(**kwargs):
    # Ensure temp bucket exists for intermediate files
    ensure_bucket_exists(MINIO_TEMP)
    ensure_bucket_exists(MINIO_PROCESSED)

    unprocessed = list_all_keys(MINIO_UNPROCESSED)
    processed = list_all_keys(MINIO_PROCESSED)
    new_files = sorted(unprocessed - processed)

    if not new_files:
        raise ValueError("❌ No new files found.")

    kwargs['ti'].xcom_push(key='new_files', value=new_files)