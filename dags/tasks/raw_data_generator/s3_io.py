import pandas as pd
from io import StringIO
from configs.config import S3

def read_csv_from_s3(bucket, key, date_column):
    obj = S3.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(obj['Body'])
    df[date_column] = pd.to_datetime(df[date_column], errors='coerce')
    return df.sort_values(by=date_column)

def write_csv_to_s3(df, bucket, key):
    csv_buffer = StringIO()
    
    df.to_csv(csv_buffer, index=False, na_rep="")
    S3.put_object(Bucket=bucket, Key=key, Body=csv_buffer.getvalue())
    print(f"Saved {key}")