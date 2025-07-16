import pandas as pd
from configs.config import OFFLINE_COLUMNS_TO_STANDARDISE, ONLINE_COLUMNS_TO_STANDARDISE,\
                    OFFLINE_SALES_CHANNEL, ONLINE_SALES_CHANNEL, TMSTMP, DIM_TABLES,\
                    SALES_COLUMN_ORDER
                    
# Processes 2 dfs online and offline
def transform_sales(online_df: pd.DataFrame, offline_df: pd.DataFrame, conn, schema) -> pd.DataFrame:

    s_online_df = online_df.copy()
    s_offline_df = offline_df.copy()

    # Standardize column names (renaming where necessary for the columns having similar business sense)
    s_offline_df.rename(columns=OFFLINE_COLUMNS_TO_STANDARDISE, inplace=True)
    s_online_df.rename(columns=ONLINE_COLUMNS_TO_STANDARDISE, inplace=True)

    # Add missing columns and fill with None where necessary
    missing_columns = set(s_offline_df.columns) - set(s_offline_df.columns)
    for col in missing_columns:
        s_offline_df[col] = None  # Fill missing columns in offline with None

    missing_columns = set(s_offline_df.columns) - set(s_offline_df.columns)
    for col in missing_columns:
        s_online_df[col] = None  # Fill missing columns in online with None

    # Add a sales channel column
    s_online_df['sales_channel'] = ONLINE_SALES_CHANNEL
    s_offline_df['sales_channel'] = OFFLINE_SALES_CHANNEL

    # Combine and sort
    combined_df = pd.concat([s_online_df, s_offline_df], ignore_index=True)
    combined_df.sort_values(by=TMSTMP, inplace=True)
    combined_df.reset_index(drop=True, inplace=True)

    # Read all dimension tables dynamically
    dim_data = {
        name: pd.read_sql(f"SELECT {', '.join(cfg['columns'])} FROM {schema}.{name}", conn)
        for name, cfg in DIM_TABLES.items()
    }

    # Merge all dimension tables
    for name, cfg in DIM_TABLES.items():
        combined_df = combined_df.merge(
            dim_data[name],
            on=cfg["join_keys"],
            how="left"
        )

    # replace store_id with 8 for Online stores
    combined_df.loc[combined_df['sales_channel'] == 'Online', 'store_id'] = 8

    # final touches
    combined_df['coupon_discount'] = combined_df.apply(
    lambda row: 0 if row['sales_channel'] == OFFLINE_SALES_CHANNEL else float(row['coupon_discount']),
    axis=1)

    combined_df['shipping_method_id'] = combined_df.apply(
    lambda row: 5 if row['sales_channel'] == OFFLINE_SALES_CHANNEL else row['shipping_method_id'],
    axis=1)

    return combined_df[SALES_COLUMN_ORDER].where(pd.notnull(combined_df), None)