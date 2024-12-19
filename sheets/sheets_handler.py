import gspread
from google.oauth2.service_account import Credentials


class GoogleSheetsHandler:
    def __init__(self, credentials_path, sheet_id):
        creds = Credentials.from_service_account_file(credentials_path, scopes=[
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ])
        self.client = gspread.authorize(creds)
        self.sheet_id = sheet_id

    def write_data(self, sheet_name, data, headers):
        sheet = self.client.open_by_key(self.sheet_id).worksheet(sheet_name)
        sheet.clear()
        rows = [headers] + data
        sheet.update("A1", rows)
        print(f"Data written to sheet: {sheet_name}")