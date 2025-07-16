import pandas as pd

def upsert_customers(df, conn, schema):
    if df.empty:
        print(f"No customer records to insert for schema '{schema}'.")
        return

    columns = list(df.columns)
    placeholders = ', '.join(['%s'] * len(columns))
    col_str = ', '.join(columns)

    update_str = ', '.join([
        f"{col} = EXCLUDED.{col}"
        for col in columns
        if col != 'customer_email'
    ])

    newly_inserted_count = 0

    print(columns)

    with conn.cursor() as cur:
        for i, row in enumerate(df.itertuples(index=False, name=None)):
            row = list(row)
            for idx, col in enumerate(columns):
                val = row[idx]
                if col == "customer_age":
                    if isinstance(val, float) and (pd.isna(val) or str(val) == "nan"):
                        row[idx] = None
                    elif isinstance(val, float):
                        row[idx] = int(val)
            row = tuple(row)
            sql = f"""
                INSERT INTO {schema}.customers ({col_str})
                VALUES ({placeholders})
                ON CONFLICT (customer_email) DO UPDATE SET {update_str}
                RETURNING xmax = 0;
            """
            try:
                cur.execute(sql, row)
                if cur.fetchone()[0]:
                    newly_inserted_count += 1
            except Exception as e:
                print(f"\n❌ Error at row {i}")
                print("Row values:", row)
                print("Value types:", [type(val) for val in row])
                print("Exception:", e)
                raise

        conn.commit()

    print(f"Upserted {len(df)} records into {schema}.customers")
    print(f"Newly inserted: {newly_inserted_count}")