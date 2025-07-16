def upsert_sales(df, conn, schema):
    if df.empty:
        print(f"No sales records to insert for schema '{schema}'.")
        return

    columns = list(df.columns)
    placeholders = ', '.join(['%s'] * len(columns))
    col_str = ', '.join(columns)

    # Build UPDATE SET clause, excluding conflict keys
    update_str = ', '.join([
        f"{col} = EXCLUDED.{col}"
        for col in columns
        if col not in ('customer_id', 'tmstmp')  # do not update the conflict keys
    ])

    newly_inserted_count = 0

    with conn.cursor() as cur:
        for row in df.itertuples(index=False, name=None):
            sql = f"""
                INSERT INTO {schema}.sales ({col_str})
                VALUES ({placeholders})
                ON CONFLICT (customer_id, tmstmp) DO UPDATE
                SET {update_str}
                RETURNING xmax = 0;
            """
            cur.execute(sql, row)
            # In PostgreSQL, xmax = 0 means it was inserted, not updated
            is_inserted = cur.fetchone()[0]
            if is_inserted:
                newly_inserted_count += 1

        conn.commit()

    print(f"Upserted {len(df)} records into {schema}.sales")
    print(f"Newly inserted: {newly_inserted_count}")