import pandas as pd
import math

def clean_customers_df(df: pd.DataFrame) -> pd.DataFrame:
    
    df = df.where(pd.notnull(df), None)  # replace NaN/NaT with None
    df = df.replace({'nan': None, 'NaN': None, '': None})  # also for string NaNs

    column_types = {
        "customer_firstname": str,
        "customer_lastname": str,
        "customer_gender": str,
        "customer_shirtsize": str,
        "customer_email": str,
        "customer_phone": str,
        "customer_age": int,
        "customer_address": str,
        "address_details": str,
        "customer_city": str,
        "customer_state": str,
    }

    for col, target_type in column_types.items():
        if col not in df.columns:
            continue

        if target_type == int:
            def safe_cast_age(x):
                if x is None:
                    return None
                if isinstance(x, float) and (pd.isna(x) or math.isnan(x)):
                    return None
                try:
                    return int(x)
                except:
                    return None
            df[col] = df[col].apply(safe_cast_age)
        else:
            df[col] = df[col].apply(lambda x: str(x).strip() if x is not None else None)

    return df
