import gspread
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials


class GoogleSheetsHandler:
    def __init__(self, credentials_path, sheet_id):
        # Authenticate using service account credentials
        self.credentials = Credentials.from_service_account_file(
            credentials_path,
            scopes=[
                "https://spreadsheets.google.com/feeds",
                "https://www.googleapis.com/auth/drive",
                "https://www.googleapis.com/auth/spreadsheets"
            ]
        )
        self.client = gspread.authorize(self.credentials)
        self.sheet_id = sheet_id
        self.service = build('sheets', 'v4', credentials=self.credentials)

    def write_data(self, sheet_name, data, headers):
        """Writes data to a Google Sheets tab."""
        sheet = self.client.open_by_key(self.sheet_id).worksheet(sheet_name)
        sheet.clear()
        rows = [headers] + data
        sheet.update("A1", rows)
        print(f"Data written to sheet: {sheet_name}")

    def set_column_format(self, sheet_name, column_index, format_type):
        """
        Set the format of a specific column in a Google Sheets tab.

        :param sheet_name: The name of the sheet/tab.
        :param column_index: The column index (0-based) to format.
        :param format_type: The format type to apply (e.g., 'datetime', 'date').
        """
        # Fetch the sheet ID by name
        sheet = self.client.open_by_key(self.sheet_id).worksheet(sheet_name)
        sheet_id = sheet.id

        # Determine the number format based on the type
        if format_type == "datetime":
            number_format = {
                "type": "DATE_TIME",  # Google Sheets datetime format
                "pattern": "yyyy-MM-dd HH:mm:ss"
            }
        elif format_type == "date":
            number_format = {
                "type": "DATE",  # Google Sheets date format
                "pattern": "yyyy-MM-dd"
            }
        else:
            raise ValueError("Unsupported format type. Use 'datetime' or 'date'.")

        # Define the request to update the column format
        requests = [
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startColumnIndex": column_index,
                        "endColumnIndex": column_index + 1
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "numberFormat": number_format
                        }
                    },
                    "fields": "userEnteredFormat.numberFormat"
                }
            }
        ]

        # Execute the batch update
        body = {"requests": requests}
        response = self.service.spreadsheets().batchUpdate(
            spreadsheetId=self.sheet_id,
            body=body
        ).execute()

        print(f"Request Body Sent: {body}")
        print(f"Column format updated. Response: {response}")


# Example usage
if __name__ == "__main__":
    # Define credentials path and spreadsheet ID
    CREDENTIALS_PATH = "path/to/credentials.json"
    SPREADSHEET_ID = "your-spreadsheet-id"

    # Initialize the handler
    handler = GoogleSheetsHandler(CREDENTIALS_PATH, SPREADSHEET_ID)

    # Write some sample data
    sheet_name = "Sheet1"
    data = [
        ["Apples", 10, "2024-12-21 14:14:00"],
        ["Oranges", 5, "2024-12-21 15:15:00"],
        ["Bananas", 20, "2024-12-21 16:16:00"],
    ]
    headers = ["Item", "Quantity", "Timestamp"]
    handler.write_data(sheet_name, data, headers)

    # Set column format for "Timestamp" (column index 2)
    handler.set_column_format(sheet_name, column_index=2, format_type="datetime")