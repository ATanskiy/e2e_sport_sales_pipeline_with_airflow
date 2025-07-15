import os
import sys
from dotenv import load_dotenv
from botocore.exceptions import ClientError
from configs.config import BUCKET_LIST, S3

def delete_bucket_completely(bucket):
    try:
        # Check if bucket exists
        S3.head_bucket(Bucket=bucket)
    except ClientError as e:
        if e.response['Error']['Code'] in ("404", "NoSuchBucket"):
            print(f"❌ Bucket '{bucket}' does not exist.")
            return
        else:
            print(f"⚠️ Error checking bucket '{bucket}': {e.response['Error']['Message']}")
            return

    try:
        # Delete all objects
        response = S3.list_objects_v2(Bucket=bucket)
        objects = [{'Key': obj['Key']} for obj in response.get('Contents', [])]
        if objects:
            S3.delete_objects(Bucket=bucket, Delete={'Objects': objects})
            print(f"🗑️ Deleted {len(objects)} objects from bucket '{bucket}'.")
        else:
            print(f"🧺 Bucket '{bucket}' was already empty.")

        # Delete the bucket
        S3.delete_bucket(Bucket=bucket)
        print(f"❌ Deleted bucket '{bucket}'.")
    except ClientError as e:
        print(f"⚠️ Error deleting '{bucket}': {e.response['Error']['Message']}")

if __name__ == "__main__":
    for bucket in BUCKET_LIST:
        delete_bucket_completely(bucket)