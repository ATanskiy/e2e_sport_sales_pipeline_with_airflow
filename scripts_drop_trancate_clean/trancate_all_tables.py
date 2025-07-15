import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from db.connection import get_connection
from configs.config import TRUNCATE_ALL_TABLES_PATH, SCHEMAS

def run_truncate_sql():
    with open(TRUNCATE_ALL_TABLES_PATH, "r") as f:
        sql_template = f.read()

    conn = get_connection()
    cur = conn.cursor()
    for schema in SCHEMAS:
        sql = sql_template.replace("{{schema}}", schema)
        cur.execute(sql)
        conn.commit()
    cur.close()
    conn.close()
    print("🧹 All tables truncated using.")

if __name__ == "__main__":
    run_truncate_sql()
