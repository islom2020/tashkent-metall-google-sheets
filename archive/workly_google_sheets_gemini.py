import requests
import json
from googleapiclient.discovery import build
from google.oauth2 import service_account

# Workly API credentials
WORKLY_API_URL = "https://api.workly.uz/v1"
WORKLY_AUTH_URL = f"{WORKLY_API_URL}/oauth/token"
WORKLY_INOUTS_URL = f"{WORKLY_API_URL}/reports/inouts"
CLIENT_ID = "2b718e31a7f98045232ac44e6beaaf4e662b215c28957"
CLIENT_SECRET = "babfef0a5cf7ed16f617542cbf8cbb5a662b215c2895a"
USERNAME = "umidismoilov0329@gmail.com"
PASSWORD = "1222211998"

# Google Sheets credentials
SERVICE_ACCOUNT_FILE = '/home/upsoft/projects/tashkent-metall-google-sheets/fair-splice-443801-q3-42d1e9b89145.json'  # Replace with your service account file
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SPREADSHEET_ID = '1BXSHsfJH0A7A8rjztrwm0IJ2-uiBIqDRD7XxcEeNuRo'  # Replace with your spreadsheet ID
SHEET_NAME = 'workly_gemini'  # Replace with your sheet name


def get_workly_access_token():
    """
    Authenticates with the Workly API and returns an access token.
    Handles potential authentication errors.
    """
    try:
        data = {
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "grant_type": "password",
            "username": USERNAME,
            "password": PASSWORD
        }
        response = requests.post(WORKLY_AUTH_URL, json=data)
        response.raise_for_status()  # Raise an exception for bad status codes

        # Check for specific Workly error codes
        if response.status_code == 401:
            if response.json().get('code') == 7002:
                raise ValueError("Workly API Error: Incorrect username or password (7002)")
            else:
                raise ValueError("Workly API Error: Unauthorized (401)")
        elif response.status_code == 403:
            raise PermissionError("Workly API Error: Access restricted (403)")
        elif response.status_code == 426:
            raise ValueError("Workly API Error: API version needs to be updated (426)")

        return response.json()['access_token']

    except requests.exceptions.RequestException as e:
        print(f"An error occurred while authenticating with Workly API: {e}")
        raise  # Re-raise the exception after printing the error


def refresh_workly_access_token(refresh_token):
    """
    Refreshes the Workly API access token using the refresh token.
    Handles potential token refresh errors.
    """
    try:
        data = {
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "grant_type": "refresh_token",
            "refresh_token": refresh_token
        }
        response = requests.post(WORKLY_AUTH_URL, json=data)
        response.raise_for_status()

        # Check for specific Workly error codes
        if response.status_code == 401:
            if response.json().get('code') == 7008:
                raise ValueError("Workly API Error: Invalid access token (7008)")
            else:
                raise ValueError("Workly API Error: Unauthorized (401)")

        return response.json()['access_token']

    except requests.exceptions.RequestException as e:
        print(f"An error occurred while refreshing the access token: {e}")
        raise


def get_employee_inouts(access_token, start_date):  # Removed end_date parameter
    """
    Fetches employee in/out data from the Workly API, handling pagination.
    Includes error handling for API requests.
    """
    try:
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Limit': '50'
        }
        params = {
            'start_date': start_date,
            # 'end_date': end_date,  # Removed end_date parameter
            'page': 1
        }
        all_items = []
        while True:
            response = requests.get(WORKLY_INOUTS_URL, headers=headers, params=params)
            response.raise_for_status()

            # Check for specific Workly error codes
            if response.status_code == 429:
                raise ValueError("Workly API Error: Too many requests (429)")

            data = response.json()
            all_items.extend(data['items'])
            if params['page'] == data['_meta']['pageCount']:
                break
            params['page'] += 1
        return all_items

    except requests.exceptions.RequestException as e:
        print(f"An error occurred while fetching employee in/outs: {e}")
        raise


def transform_data(inouts_data):
    """
    Extracts the required fields from the in/out data.
    """
    transformed_data = []
    for item in inouts_data:
        transformed_data.append([
            item['employee_id'],
            item['full_name'],
            item['department_title'],
            item['event_date'],
            item['event_full_date'],
            item['event_time'],
            item['event_name']
        ])
    return transformed_data


def write_to_google_sheets(data, spreadsheet_id, sheet_name):
    """
    Writes the data to the specified Google Sheet.
    """
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    service = build('sheets', 'v4', credentials=creds)

    # Clear the sheet
    service.spreadsheets().values().clear(spreadsheetId=spreadsheet_id, range=f'{sheet_name}!A:Z').execute()

    # Write the data with header
    header = ['employee_id', 'full_name', 'department_title', 'event_date', 'event_full_date', 'event_time', 'event_name']
    data.insert(0, header)
    body = {
        'values': data
    }
    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id, range=f'{sheet_name}!A1',
        valueInputOption='RAW', body=body).execute()


if __name__ == "__main__":
    try:
        access_token = get_workly_access_token()
        start_date = "2024-11-01"  # Replace with your desired start date
        # end_date = "2024-11-30"  # Removed end_date
        inouts_data = get_employee_inouts(access_token, start_date)  # Updated function call
        transformed_data = transform_data(inouts_data)
        write_to_google_sheets(transformed_data, SPREADSHEET_ID, SHEET_NAME)
        print("Data successfully extracted from Workly API and loaded to Google Sheets.")

    except Exception as e:
        print(f"ETL process failed: {e}")