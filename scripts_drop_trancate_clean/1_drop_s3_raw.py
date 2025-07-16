"""
One-Time MinIO Bucket Cleanup Script

This script deletes all objects from the `MINIO_RAW` bucket and then deletes the bucket itself.

Features:
- Lists and deletes all objects in the bucket
- Logs each deletion individually
- Deletes the bucket after clearing its contents
- Handles and logs S3-related client errors
"""

from botocore.exceptions import ClientError
from configs.config import S3, MINIO_RAW

try:
    # List all objects in the bucket
    print(f"📂 Listing objects in '{MINIO_RAW}'...")
    objects = S3.list_objects_v2(Bucket=MINIO_RAW)

    if 'Contents' in objects:
        print("🧹 Deleting files...")
        for obj in objects['Contents']:
            print(f"❌ Deleting {obj['Key']}...")
            S3.delete_object(Bucket=MINIO_RAW, Key=obj['Key'])
    else:
        print("✅ Bucket is already empty.")

    # Delete the bucket
    print(f"💣 Deleting bucket '{MINIO_RAW}'...")
    S3.delete_bucket(Bucket=MINIO_RAW)
    print("✅ Bucket deleted successfully.")

except ClientError as e:
    print(f"❌ Error: {e}")
