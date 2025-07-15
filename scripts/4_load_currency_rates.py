import sys
import os

# Add project root to Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from etl.etl_currencies import run_currency_pipeline

if __name__ == "__main__":
    run_currency_pipeline()