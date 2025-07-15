import subprocess
import time
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from configs.config import TIME_TO_SLEEP_ETL, SCRIPT_ETL

# 1. Use the same Python interpreter that runs this script
python_exec = sys.executable

# 2. Infinite loop to run the ETL script repeatedly
while True:
    print(f"\nRunning {SCRIPT_ETL}...\n{'-'*50}")
    process = subprocess.Popen(
        [python_exec, "-u", SCRIPT_ETL],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True
    )

    for line in process.stdout:
        print(line, end="")

    process.wait()
    if process.returncode != 0:
        print(f"\nScript failed: {SCRIPT_ETL} — retrying after {TIME_TO_SLEEP_ETL} seconds...\n")
    else:
        print(f"\nFinished {SCRIPT_ETL}\n{'='*50}")

    print(f"Sleeping for {TIME_TO_SLEEP_ETL} seconds...\n")
    time.sleep(TIME_TO_SLEEP_ETL)