"""
S3 Object Cleanup Script (MinIO-Compatible)

Removes all objects from each bucket listed in `BUCKET_LIST` without deleting the buckets themselves.

Features:
- Checks if the bucket exists before attempting deletion
- Deletes all objects if present
- Skips empty buckets with informative logging
- Gracefully handles missing buckets and common S3 errors
"""

from db.connection import get_connection
from configs.config import DROP_TABLE_SCHEMAS_PATH

def run_schema_sql():
    with open(DROP_TABLE_SCHEMAS_PATH, "r") as f:
        sql = f.read()

    conn = get_connection()
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()

    cur.close()
    conn.close()
    print("✅ Schemas and tables dropped successfully.")

if __name__ == "__main__":
    run_schema_sql()