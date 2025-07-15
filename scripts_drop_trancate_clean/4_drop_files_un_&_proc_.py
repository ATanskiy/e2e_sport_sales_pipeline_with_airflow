import os
import sys
import botocore
from configs.config import S3, BUCKET_LIST

def delete_all_objects(bucket):
    try:
        response = S3.list_objects_v2(Bucket=bucket)
        objects = [{'Key': obj['Key']} for obj in response.get('Contents', [])]

        if not objects:
            print(f"🧺 Bucket '{bucket}' is already empty.")
            return

        S3.delete_objects(Bucket=bucket, Delete={'Objects': objects})
        print(f"🗑️ Deleted {len(objects)} objects from bucket '{bucket}'.")

    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchBucket':
            print(f"⚠️ Bucket '{bucket}' does not exist. Skipping.")
        else:
            raise

if __name__ == "__main__":
    for bucket in BUCKET_LIST:
        delete_all_objects(bucket)