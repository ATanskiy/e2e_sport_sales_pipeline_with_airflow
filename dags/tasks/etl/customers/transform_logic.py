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

    return  (pd.concat([c_online_df, c_offline_df], ignore_index=True)
              .sort_values(by=["customer_email"])
              .drop_duplicates(subset=["customer_email"], keep="last"))