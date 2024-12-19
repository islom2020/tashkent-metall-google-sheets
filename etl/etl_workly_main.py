from clients.workly_client import WorklyClient
from sheets.sheets_handler import GoogleSheetsHandler
from datetime import datetime

# Google Sheets Constants
GOOGLE_CREDENTIALS_PATH = "/home/upsoft/projects/tashkent-metall-google-sheets/sheets/fair-splice-443801-q3-42d1e9b89145.json"
GOOGLE_SHEETS_ID = "1BXSHsfJH0A7A8rjztrwm0IJ2-uiBIqDRD7XxcEeNuRo"

# Workly Credentials
WORKLY_CLIENT_ID = "2b718e31a7f98045232ac44e6beaaf4e662b215c28957"
WORKLY_CLIENT_SECRET = "babfef0a5cf7ed16f617542cbf8cbb5a662b215c2895a"
WORKLY_USERNAME = "umidismoilov0329@gmail.com"
WORKLY_PASSWORD = "1222211998"


def main():
    """ETL Process for Workly."""
    # Initialize Google Sheets Handler
    sheets_handler = GoogleSheetsHandler(GOOGLE_CREDENTIALS_PATH, GOOGLE_SHEETS_ID)

    # Fetch Workly Data
    print("---------------------------------")
    print(f"[{datetime.now()}] Fetching Workly data...")
    workly_client = WorklyClient(WORKLY_CLIENT_ID, WORKLY_CLIENT_SECRET, WORKLY_USERNAME, WORKLY_PASSWORD)
    workly_data = workly_client.fetch_inouts(start_date="2024-11-01")

    # Prepare Data for Google Sheets
    workly_headers = ["Employee ID", "Full Name", "Department", "Event Date", "Event Full Date", "Event Time", "Event Name"]
    workly_rows = [
        [item["employee_id"], item["full_name"], item["department_title"], item["event_date"],
         item["event_full_date"], item["event_time"], item["event_name"]]
        for item in workly_data
    ]

    # Write Data to Google Sheets
    sheets_handler.write_data("Workly", workly_rows, workly_headers)

    print(f"[{datetime.now()}] Workly ETL completed successfully!")


if __name__ == "__main__":
    main()