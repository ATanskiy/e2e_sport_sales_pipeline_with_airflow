import os
import boto3
from datetime import date
from dotenv import load_dotenv
from pathlib import Path

AIRFLOW_ENV_PATH = Path("/opt/airflow/.env.airflow")
LOCAL_ENV_PATH = Path(__file__).resolve().parent.parent / "envs" / ".env.local"

if AIRFLOW_ENV_PATH.exists():
    load_dotenv(dotenv_path=AIRFLOW_ENV_PATH)
    print("Loaded Airflow environment")
elif LOCAL_ENV_PATH.exists():
    load_dotenv(dotenv_path=LOCAL_ENV_PATH)
    print("Loaded local environment")
else:
    print("No .env file found. Using system environment only.")

# Kaggle dataset
DATASET = "larysa21/retail-data-american-football-gear-sales"

# Settings
ONLINE_FILE_NAME = "AF_online_sales_dataset.csv"
OFFLINE_FILE_NAME = "AF_offline_sales_dataset.csv"
RAW_DATA_FOLDER = "raw_data"
DOWNLOAD_TEMP = "tmp_download"

# Load credentials from .env file
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_RAW = os.getenv("MINIO_RAW")
MINIO_UNPROCESSED=os.getenv("MINIO_UNPROCESSED")
MINIO_PROCESSED=os.getenv("MINIO_PROCESSED")

# List of unprocessed and processed buckets
BUCKET_LIST = [MINIO_UNPROCESSED, MINIO_PROCESSED]

# Initialize S3 client
S3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
)

# Schemas used in the project
SCHEMAS = ["prod", "playground"]

# CSV File names
ONLINE_FILE_NAME = "AF_online_sales_dataset.csv"
OFFLINE_FILE_NAME = "AF_offline_sales_dataset.csv"

# Data folder paths
RAW_DATA_FOLDER = "raw_data"
SEEDS = "seeds"

# Seeds mapping
SEEDS_MAPPING = {
    "employees.csv": "employees",
    "payment_methods.csv": "payment_methods",
    "product_categories.csv": "product_categories",
    "product_subcategories.csv": "product_subcategories",
    "products.csv": "products",
    "shipping_methods.csv": "shipping_methods",
    "stores.csv": "stores",
    "currency_rates.csv": "currency_rates"
}

# Sleep time between ETL batches (in seconds)
TIME_TO_SLEEP = 5
TIME_TO_SLEEP_ETL = 20

# Other shared settings
DATE_FORMAT = "%Y/%m/%d"

# Timestamp columns
TMSTMP =  "tmstmp"
DATE = "date"

# Paths
CREATE_TABLES_SCHEMAS_PATH = "db/ddl/create_schemas_tables.sql"
DROP_TABLE_SCHEMAS_PATH = "db/ddl/drop_schemas_tables.sql"
TRUNCATE_ALL_TABLES_PATH = "db/ddl/trancate_all_tables.sql"
TRUNCATE_DIM_TABLES_PATH = "db/ddl/trancate_dim_tables.sql"

# Scripts to run in order
SCRIPTS = [
    "scripts/1_download_to_s3_raw.py",
    "scripts/2_create_schemas_tables.py",
    "scripts/3_load_dim_tables.py",
    "scripts/4_load_currency_rates.py"
]

SCRIPT_ETL = "scripts/5_run_etl_upsert.py"

#ETL part
# Customers ETL variables
BASE_COLS = [
    "customer_firstname", "customer_lastname", "customer_gender", "customer_shirtsize",
    "customer_email", "customer_phone", "customer_age", "customer_address",
    "address_details", "customer_city", "customer_state"]

# Sales ETL variables
OFFLINE_COLUMNS_TO_STANDARDISE = {
        "product_name": "product",
        "brand": "brand_name",
        "category": "product_category",
        "subcategory": "product_subcategory",
        "date": "tmstmp",
        "price": "product_price",
        "amount_sold": "total_amount",
        "cost_amount": "total_costs"
    }

ONLINE_COLUMNS_TO_STANDARDISE = {
        "payment_type": "payment_method"
    }

OFFLINE_SALES_CHANNEL = "Offline"
ONLINE_SALES_CHANNEL = "Online"

DIM_TABLES = {
    "products": {
        "columns": ["product_id", "product", "brand_name"],
        "join_keys": ["product", "brand_name"]
    },
    "customers": {
        "columns": ["customer_id", "customer_email"],
        "join_keys": ["customer_email"]
    },
    "stores": {
        "columns": ["store_id", "store_type", "store_street", "store_city", "store_state"],
        "join_keys": ["store_type", "store_street", "store_city", "store_state"]
    },
    "employees": {
        "columns": ["employee_id", "employee_firstname", "employee_lastname", "employee_email", "employee_skill", "employee_education"],
        "join_keys": ["employee_firstname", "employee_lastname", "employee_email", "employee_skill", "employee_education"]
    },
    "payment_methods": {
        "columns": ["payment_method_id", "payment_method"],
        "join_keys": ["payment_method"]
    },
    "shipping_methods": {
        "columns": ["shipping_method_id", "shipping_method"],
        "join_keys": ["shipping_method"]
    }
}

# Final reorder
SALES_COLUMN_ORDER = [
    'tmstmp', 'product_id', 'customer_id', 'store_id', 'employee_id',
    'payment_method_id', 'shipping_method_id', 'product_price', 'coupon_discount',
    'quantity_sold', 'total_amount', 'total_costs', 'sales_channel', 'store_website', 'supplier'
]

# Configs for working with currencies
START_DATE = "2022-01-01"
END_DATE = date.today().strftime("%Y-%m-%d")

FRANKFURTER_API_URL = "https://api.frankfurter.app"

# Base currency in the project is USD, it is nice to have EUR and ILS
BASE_CURRENCY = "USD"
TARGET_CURRENCIES = ["EUR", "ILS"]
CURRENCY_TABLE = "currency_rates"