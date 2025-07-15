from db.connection import get_connection
from configs.config import TRUNCATE_DIM_TABLES_PATH, SCHEMAS
import psycopg2

def run_truncate_sql():
    try:
        with open(TRUNCATE_DIM_TABLES_PATH, "r") as f:
            sql_template = f.read()
    except FileNotFoundError:
        print(f"❌ File not found: {TRUNCATE_DIM_TABLES_PATH}")
        return
    except Exception as e:
        print(f"❌ Failed to read SQL file: {e}")
        return

    conn = None
    cur = None

    try:
        conn = get_connection()
        cur = conn.cursor()

        for schema in SCHEMAS:
            sql = sql_template.replace("{{schema}}", schema)
            try:
                cur.execute(sql)
                conn.commit()
                print(f"✅ Truncated dim tables in schema '{schema}'")
            except Exception as e:
                print(f"❌ Failed to truncate schema '{schema}': {e}")
                conn.rollback()

    except psycopg2.Error as e:
        print(f"❌ Database error: {e}")
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
        print("🔌 Connection closed.")

if __name__ == "__main__":
    run_truncate_sql()