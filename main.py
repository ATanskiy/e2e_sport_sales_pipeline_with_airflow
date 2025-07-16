"""
Local Bootstrap Script for Project Initialization

This script automates the setup of a local development environment by:
1. Starting required Docker containers (e.g., Postgres, MinIO)
2. Waiting for services to be ready
3. Executing a sequence of ETL and setup scripts:
   - Downloads raw data from Kaggle to MinIO
   - Creates schemas and tables in Postgres
   - Seeds dimension tables
   - Loads currency exchange rates

Features:
- Sets `ENV=local` for local config loading
- Uses a configurable sleep delay (`TIME_TO_SLEEP`) to wait for containers
- Handles and logs script failures gracefully
- Stops execution on the first failure

Typical Use:
- Local development or testing setup
- One-click environment preparation for notebooks, Airflow, or API apps

"""

import subprocess
import time
import os
import sys
from scripts.download_to_s3_raw import download_to_s3_raw
from scripts.create_schemas_tables import create_schemas_tables
from scripts.load_dim_tables import load_dim_tables
from dags.tasks.currencies.pipeline import run_currency_pipeline
from configs.config import TIME_TO_SLEEP

# Explicitly set ENV for local execution
os.environ["ENV"] = "local"

# List of scripts to run
SCRIPTS_LIST = [
    ("download_to_s3_raw", download_to_s3_raw),
    ("create_schemas_tables", create_schemas_tables),
    ("load_dim_tables", load_dim_tables),
    ("load_currency_rates", run_currency_pipeline)
]

python_exec = sys.executable

# 1. Start Docker containers
print("Starting Docker containers...")
subprocess.run(["docker-compose", "up", "-d"], check=True)

print("Waiting for services to become available...")
time.sleep(TIME_TO_SLEEP)

# 2. Run each script as a Python function
for name, func in SCRIPTS_LIST:
    print(f"\nRunning {name}...\n{'-'*50}")
    try:
        func()  # Just call the function
        print(f"\n✅ Finished {name}\n{'='*50}")
    except Exception as e:
        print(f"\nScript {name} failed with error:\n{e}")
        break

print("\nAll scripts finished (or stopped on error).")
