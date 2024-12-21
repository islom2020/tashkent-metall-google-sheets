import sqlite3
from clients.moysklad_client import MoyskladClient
from sheets.sheets_handler import GoogleSheetsHandler
from datetime import datetime

# Database setup
DB_PATH = "database/moysklad.db"

# Google Sheets setup
GOOGLE_CREDENTIALS_PATH = "sheets/fair-splice-443801-q3-42d1e9b89145.json"
GOOGLE_SHEETS_ID = "1ucns79cbkgtybQl2RkvVe1ppfxRRpdhNLXnOqJtt67A"

# Currency ID for USD
USD_CURRENCY_ID = "b6b924be-3c50-11ef-0a80-09d80028874c"

def create_currency_table():
    """Create the currency table if it doesn't exist."""
    connection = sqlite3.connect(DB_PATH)
    cursor = connection.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS currency (
        date TEXT UNIQUE NOT NULL, -- Date column, must be unique
        rate REAL NOT NULL         -- Decimal number column
    )
    ''')
    connection.commit()
    connection.close()

def insert_currency_data(date, rate):
    """Insert or update currency data in the database."""
    connection = sqlite3.connect(DB_PATH)
    cursor = connection.cursor()
    cursor.execute('''
    INSERT INTO currency (date, rate)
    VALUES (?, ?)
    ON CONFLICT(date) DO UPDATE SET rate = excluded.rate
    ''', (date, rate))
    connection.commit()
    connection.close()

def fetch_currency_from_db():
    """Retrieve all currency data from the database."""
    connection = sqlite3.connect(DB_PATH)
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM currency")
    rows = cursor.fetchall()
    connection.close()
    return rows

def main():
    """ETL Process for Currency Data."""
    print(f"[{datetime.now()}] Starting Currency ETL...")

    # Initialize MoyskladClient and GoogleSheetsHandler
    client = MoyskladClient()
    sheets_handler = GoogleSheetsHandler(GOOGLE_CREDENTIALS_PATH, GOOGLE_SHEETS_ID)

    # Step 1: Create table if not exists
    create_currency_table()

    # Step 2: Fetch currency data from Moysklad
    currency_data = client.fetch_currency_data()
    usd_currency = next((item for item in currency_data if item["id"] == USD_CURRENCY_ID), None)

    if not usd_currency:
        print(f"[{datetime.now()}] USD currency data not found.")
        return

    usd_rate = usd_currency["rate"]
    today_date = datetime.now().strftime("%Y-%m-%d")

    # Step 3: Update database
    insert_currency_data(today_date, usd_rate)

    # Step 4: Fetch all data from the database
    all_currency_data = fetch_currency_from_db()

    # Step 5: Write data to Google Sheets
    headers = ["Date", "Currency Rate"]
    sheets_handler.write_data("Daily Currency", all_currency_data, headers)

    print(f"[{datetime.now()}] Currency ETL completed successfully!")

if __name__ == "__main__":
    main()