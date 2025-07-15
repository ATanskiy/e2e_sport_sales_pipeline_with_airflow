import subprocess
import time
import os
import sys

# Explicitly set ENV for local execution
os.environ["ENV"] = "local"

# Ensure the root path is available for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Load configs
from configs.config import SCRIPTS, TIME_TO_SLEEP

python_exec = sys.executable

def run_bootstrap():
    print("🟡 Starting infrastructure containers (Postgres, MinIO, Airflow DB)...")
    subprocess.run(["docker-compose", "up", "-d", "postgres", "minio", "airflow-db"], check=True)

    print(f"⏳ Waiting {TIME_TO_SLEEP} seconds for services to become available...")
    time.sleep(TIME_TO_SLEEP)

    all_success = True
    for script in SCRIPTS:
        print(f"\n🚀 Running {script}...\n{'-' * 50}")
        process = subprocess.Popen([python_exec, "-u", script], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

        for line in process.stdout:
            print(line, end="")

        process.wait()
        if process.returncode != 0:
            print(f"\n❌ Script failed: {script}")
            all_success = False
            break

        print(f"\n✅ Finished {script}\n{'=' * 50}")

    return all_success

if __name__ == "__main__":
    try:
        success = run_bootstrap()

        if success:
            print("\n🟢 All scripts succeeded. Starting Airflow scheduler and webserver...")
            subprocess.run(["docker-compose", "up", "-d", "airflow-scheduler", "airflow-webserver"], check=True)
        else:
            print("\n🔴 One or more scripts failed. Cleaning up containers...")
            subprocess.run(["docker-compose", "down"], check=True)
            print("🧹 All containers were removed due to failure.")

    except Exception as e:
        print(f"\n🔥 Unexpected error: {e}")
        subprocess.run(["docker-compose", "down"], check=True)
        print("🧹 All containers were removed due to unexpected failure.")
