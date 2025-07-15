import sys
import os
import pandas as pd
from datetime import datetime
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from db.connection import get_connection
from configs.config import SCHEMAS, SEEDS, SEEDS_MAPPING

def insert_dataframe_to_table(df, table_name, schema, conn):
    df['inserted_at'] = datetime.now()

    placeholders = ', '.join(['%s'] * len(df.columns))
    columns = ', '.join(df.columns)

    cur = conn.cursor()
    for row in df.itertuples(index=False, name=None):
         # Clean each value: convert NaN or invalid string to None
        cleaned_row = tuple(None if pd.isna(val) else val for val in row) # This is what I fixed to upload None --> null in postgres 06.07.2025
        sql = f"INSERT INTO {schema}.{table_name} ({columns}) VALUES ({placeholders})"
        cur.execute(sql, cleaned_row)
    conn.commit()
    cur.close()
    print(f"Inserted {len(df)} rows into {schema}.{table_name}")

def load_all_files():
    conn = get_connection()
    inserted_any = False  # Track whether anything got inserted
    inserted_count = 0  # New counter

    for file_name, table_name in SEEDS_MAPPING.items():
        file_path = os.path.join(SEEDS, file_name)

        if not os.path.exists(file_path):
            print(f"File not found: {file_path}")
            continue

        df = pd.read_csv(file_path)

        for schema in SCHEMAS:
            with conn.cursor() as cur:
                # 1. Check if table exists
                cur.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = %s AND table_name = %s
                    )
                """, (schema, table_name))
                table_exists = cur.fetchone()[0]

                if not table_exists:
                    print(f"Table '{table_name}' does not exist in schema '{schema}'. Skipping.")
                    continue

                # 2. Check if table is empty
                cur.execute(f"SELECT COUNT(*) FROM {schema}.{table_name}")
                count = cur.fetchone()[0]

            if count > 0:
                print(f"Skipping {schema}.{table_name} — already has {count} rows.")
                continue

            # 3. Insert data
            insert_dataframe_to_table(df.copy(), table_name, schema, conn)
            inserted_any = True
            inserted_count += 1  # Increment counter

    conn.close()
    if inserted_any:
        print(f"Data was inserted into {inserted_count} dimention tables.")
    else:
        print("No data was inserted — all tables either missing or not empty.")

if __name__ == "__main__":
    load_all_files()