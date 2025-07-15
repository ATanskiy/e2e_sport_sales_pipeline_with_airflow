import pandas as pd
import re
from configs.config import BASE_COLS

def normalize_phone(phone):
    digits = re.sub(r"\D", "", str(phone))
    return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}" if len(digits) == 10 else phone

# Processes 2 dfs online and offline
def transform_customers(online_df: pd.DataFrame, offline_df: pd.DataFrame) -> pd.DataFrame:
    c_online_df = online_df.copy()
    c_offline_df = offline_df.copy()

    # Online
    c_online_df["customer_phone"] = c_online_df["customer_phone"].apply(normalize_phone)
    c_online_df = c_online_df[BASE_COLS]

    # Offline
    c_offline_df = c_offline_df.assign(
        customer_age=None,
        customer_shirtsize=None,
        customer_address=None,
        address_details=None,
        customer_city=c_offline_df["store_city"],
        customer_state=c_offline_df["store_state"]
    )
    c_offline_df["customer_phone"] = c_offline_df["customer_phone"].apply(normalize_phone)
    c_offline_df = c_offline_df[BASE_COLS]

    # Combine and deduplicate
    return (pd.concat([c_online_df, c_offline_df], ignore_index=True)
              .sort_values(by=["customer_email"])
              .drop_duplicates(subset=["customer_email"], keep="last"))

# Upserts new customers to customers table in 2 schemas
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
        if col != 'customer_email'  # Do not update the unique key
    ])

    newly_inserted_count = 0

    with conn.cursor() as cur:
        for row in df.itertuples(index=False, name=None):
            sql = f"""
                INSERT INTO {schema}.customers ({col_str})
                VALUES ({placeholders})
                ON CONFLICT (customer_email) DO UPDATE SET {update_str}
                RETURNING xmax = 0;
            """
            cur.execute(sql, row)
            # In PostgreSQL, xmax = 0 means the row is newly inserted
            is_inserted = cur.fetchone()[0]
            if is_inserted:
                newly_inserted_count += 1

        conn.commit()

    print(f"Upserted {len(df)} records into {schema}.customers")
    print(f"Newly inserted: {newly_inserted_count}")