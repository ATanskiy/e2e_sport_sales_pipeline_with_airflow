import pandas as pd
from datetime import datetime
from configs.config import S3, MINIO_UNPROCESSED

def download_and_concat(file_list, sort_by=None):
    dfs = []
    for key in file_list:
        print(f"📥 Downloading {key}")
        obj = S3.get_object(Bucket=MINIO_UNPROCESSED, Key=key)
        dfs.append(pd.read_csv(obj['Body']))
    
    df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    if sort_by and sort_by in df.columns:
        df = df.sort_values(by=sort_by)
    
    return df


def save_temp_csv(df, prefix: str, S3, MINIO_TEMP):
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    key = f"temp/{prefix}_{timestamp}.csv"
    print(f"💾 Saving to {key}")
    S3.put_object(Bucket=MINIO_TEMP, Key=key, Body=df.to_csv(index=False))
    return key