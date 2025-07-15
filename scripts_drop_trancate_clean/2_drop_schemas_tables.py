# a trick to run scripts from another folder
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from configs.config import DROP_TABLE_SCHEMAS_PATH

# run_sql_init.py
from db.connection import get_connection

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