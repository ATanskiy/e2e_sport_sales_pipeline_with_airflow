import sys
import os
from db.connection import get_connection
from configs.config import TRUNCATE_DIM_TABLES, SCHEMAS

def run_truncate_sql():
    with open(TRUNCATE_DIM_TABLES, "r") as f:
        sql_template = f.read()

    conn = get_connection()
    cur = conn.cursor()

    for schema in SCHEMAS:
        sql = sql_template.replace("{{schema}}", schema)
        cur.execute(sql)
        conn.commit()
        
    cur.close()
    conn.close()
    print("🧹 Dim tables truncated using.")

if __name__ == "__main__":
    run_truncate_sql()