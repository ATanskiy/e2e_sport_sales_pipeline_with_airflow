def upsert_currency_rates(df, conn, schema):
    from configs.config import CURRENCY_TABLE

    if df.empty:
        print(f"No currency records to insert for schema '{schema}'.")
        return

    df = df[["currency", "to_euro", "to_isl", "date"]]

    newly_inserted_count = 0
    upserted_count = 0

    sql = f"""
        INSERT INTO {schema}.{CURRENCY_TABLE} 
            (currency, to_euro, to_isl, date)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (currency, date) DO UPDATE 
        SET 
            to_euro = EXCLUDED.to_euro,
            to_isl = EXCLUDED.to_isl
        RETURNING xmax;
    """

    with conn.cursor() as cur:
        for row in df.itertuples(index=False, name=None):
            cur.execute(sql, row)
            result = cur.fetchone()
            upserted_count += 1
            if result and result[0] == 0:
                newly_inserted_count += 1

    conn.commit()
    print(f"Upserted: {upserted_count} total records into {schema}.{CURRENCY_TABLE}")
    print(f"Newly inserted: {newly_inserted_count}")
    print(f"Updated: {upserted_count - newly_inserted_count}")