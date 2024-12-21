import subprocess
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv()
pythonpath = os.getenv("PYTHONPATH")

def run_script(script_name):
    """Run a Python script and wait for it to complete."""
    print(f"[{datetime.now()}] Starting {script_name}...")
    result = subprocess.run(["python3", script_name], capture_output=True, text=True)
    if result.returncode == 0:
        print(f"[{datetime.now()}] {script_name} completed successfully!")
        print(result.stdout)  # Optional: Print script output
    else:
        print(f"[{datetime.now()}] {script_name} failed!")
        print(result.stderr)  # Print error details
        raise RuntimeError(f"{script_name} failed with exit code {result.returncode}")

def main():
    try:
        print("\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")

        # Run Workly ETL
        run_script("etl/etl_workly_main.py")
        
        # Run MoySklad ETL
        run_script("etl/etl_moysklad_main.py")

        # Run Moysklad currency ETL
        run_script("etl/etl_currency_main.py")
    except RuntimeError as e:
        print(f"[{datetime.now()}] ETL process failed: {e}")
    else:
        print(f"[{datetime.now()}] All ETL processes completed successfully.")

if __name__ == "__main__":
    main()