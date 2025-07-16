from configs.config import S3, MINIO_UNPROCESSED, MINIO_PROCESSED

def move_to_processed_task(**kwargs):
    new_files = kwargs['ti'].xcom_pull(key='new_files')
    
    if not new_files:
        print("⚠️ No files to move.")
        return

    for key in sorted(new_files):
        copy_source = {"Bucket": MINIO_UNPROCESSED, "Key": key}
        
        try:
            S3.copy(copy_source, MINIO_PROCESSED, key)
            # Optional: Delete the original file from unprocessed bucket
            # S3.delete_object(Bucket=MINIO_UNPROCESSED, Key=key)
            print(f"✅ Moved {key} to '{MINIO_PROCESSED}'")
        except Exception as e:
            print(f"❌ Failed to move {key}: {e}")
            raise

    print("🎉 All files moved to processed bucket. Kol Hakavod!")
