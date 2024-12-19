import requests
import gspread
import time
from datetime import datetime
from google.oauth2.service_account import Credentials

# Constants
WORKLY_AUTH_URL = "https://api.workly.uz/v1/oauth/token"
WORKLY_INOUTS_URL = "https://api.workly.uz/v1/reports/inouts"
CLIENT_ID = "2b718e31a7f98045232ac44e6beaaf4e662b215c28957"
CLIENT_SECRET = "babfef0a5cf7ed16f617542cbf8cbb5a662b215c2895a"
USERNAME = "umidismoilov0329@gmail.com"
PASSWORD = "1222211998"
GOOGLE_SHEETS_ID = "1BXSHsfJH0A7A8rjztrwm0IJ2-uiBIqDRD7XxcEeNuRo"
SHEET_NAME = "Workly"
GOOGLE_CREDENTIALS_PATH = "/home/upsoft/projects/tashkent-metall-google-sheets/fair-splice-443801-q3-42d1e9b89145.json"

# Global Token
access_token = None
refresh_token = None

# Authenticate and get tokens
def authenticate():
    global refresh_token
    payload = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "grant_type": "password",
        "username": USERNAME,
        "password": PASSWORD,
    }
    response = requests.post(WORKLY_AUTH_URL, json=payload)
    if response.status_code == 401:
        raise Exception("Authorization Error: Incorrect username or password.")
    response.raise_for_status()
    auth_data = response.json()
    refresh_token = auth_data["refresh_token"]
    return auth_data["access_token"]

# Refresh Access Token
def refresh_access_token():
    global refresh_token
    payload = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
    }
    response = requests.post(WORKLY_AUTH_URL, json=payload)
    response.raise_for_status()
    auth_data = response.json()
    refresh_token = auth_data["refresh_token"]
    return auth_data["access_token"]

# Error Handler
def handle_api_errors(response):
    if response.status_code == 400:
        raise Exception("400: Bad Request - Syntax error in the request.")
    elif response.status_code == 401:
        raise Exception("401: Unauthorized - Invalid access key.")
    elif response.status_code == 403:
        raise Exception("403: Forbidden - Access is restricted.")
    elif response.status_code == 404:
        raise Exception("404: Not Found - Invalid URL.")
    elif response.status_code == 426:
        raise Exception("426: Upgrade Required - API version update required.")
    elif response.status_code == 429:
        print("429: Too Many Requests - Retrying after backoff...")
        time.sleep(10)  # Exponential backoff
        return True
    elif response.status_code == 500:
        raise Exception("500: Internal Server Error - Please try again later.")
    elif response.status_code == 503:
        raise Exception("503: Service Unavailable - Server is down for maintenance.")
    else:
        response.raise_for_status()
    return False

# Fetch All Pages of Data
def fetch_all_data(start_date):
    global access_token
    all_data = []
    headers = {"Authorization": f"Bearer {access_token}"}
    page = 1

    while True:
        params = {"start_date": start_date, "page": page, "per-page": 50}
        response = requests.get(WORKLY_INOUTS_URL, headers=headers, params=params)
        
        # Handle errors
        if response.status_code in [7008, 7009]:  # Invalid or Expired Token
            print("Access token expired. Refreshing...")
            access_token = refresh_access_token()
            headers["Authorization"] = f"Bearer {access_token}"
            continue
        
        if handle_api_errors(response):
            continue
        
        # Process data
        data = response.json()
        all_data.extend(data["items"])
        
        # Check for next page
        if "next" not in data["_links"]:
            break
        page += 1
    return all_data

# Write to Google Sheets
def write_to_google_sheets(data):
    creds = Credentials.from_service_account_file(GOOGLE_CREDENTIALS_PATH, scopes=[
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ])
    client = gspread.authorize(creds)
    sheet = client.open_by_key(GOOGLE_SHEETS_ID).worksheet(SHEET_NAME)
    sheet.clear()

    header = ["Employee ID", "Full Name", "Department", "Event Date", "Event Full Date", "Event Time", "Event Name"]
    rows = [header] + [[
        item["employee_id"],
        item["full_name"],
        item["department_title"],
        item["event_date"],
        item["event_full_date"],
        item["event_time"],
        item["event_name"],
    ] for item in data]
    
    # Batch write data
    try:
        sheet.update(rows, "A1")
    except gspread.exceptions.APIError as e:
        print(f"Error writing to Google Sheets: {e}")
        raise

# Main ETL Function
def etl_workly_to_google_sheets():
    global access_token
    try:
        # Authenticate
        access_token = authenticate()
        
        # Fetch Data
        start_date = "2024-11-01"  # Start from the beginning of November
        print("------------------------------")
        print(datetime.now())
        print("Fetching data...")
        all_data = fetch_all_data(start_date)
        
        # Write to Google Sheets
        print("Writing to Google Sheets...")
        write_to_google_sheets(all_data)
        print("ETL process completed successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")

# Run ETL Process
if __name__ == "__main__":
    etl_workly_to_google_sheets()