from clients.moysklad_client import MoyskladClient
from etl.data_preparation import transform_supply, transform_customer_order, transform_purchase_return
from sheets.sheets_handler import GoogleSheetsHandler
from datetime import datetime

GOOGLE_CREDENTIALS_PATH = "/home/upsoft/projects/tashkent-metall-google-sheets/sheets/fair-splice-443801-q3-42d1e9b89145.json"
GOOGLE_SHEETS_ID = "1ucns79cbkgtybQl2RkvVe1ppfxRRpdhNLXnOqJtt67A"


def fetch_references(client: MoyskladClient):
    """Fetch reference data like stores and products."""
    return {
        "stores": {item["meta"]["href"]: item["name"] for item in client.fetch_paginated_data("/entity/store")},
        "agents": {item["meta"]["href"]: item["name"] for item in client.fetch_paginated_data("/entity/counterparty")},
        "uoms": {item["meta"]["href"]: item["name"] for item in client.fetch_paginated_data("/entity/uom")},
        "products": {
            item["meta"]["href"]: {"name": item["name"],
                                   "weight": item.get("weight", 0),
                                   "uomMetaHref": item.get("uom", {}).get("meta", {}).get("href")}
            for item in client.fetch_paginated_data("/entity/product")
        },
        "currencies": {item["meta"]["href"]: item["name"] for item in client.fetch_paginated_data("/entity/currency")},
        "supplies": {item["meta"]["href"]: item["name"] for item in client.fetch_paginated_data("/entity/supply")},
    }


def main():
    """ETL Process for MoySklad."""
    print(f"[{datetime.now()}] Starting MoySklad ETL...")
    sheets_handler = GoogleSheetsHandler(
        GOOGLE_CREDENTIALS_PATH, GOOGLE_SHEETS_ID)
    moysklad_client = MoyskladClient()

    # Fetch Reference Data
    refs = fetch_references(moysklad_client)

    # Tasks: Define endpoints, transformations, and headers
    tasks = [
        {
            "name": "Приход товар",
            "endpoint": "/entity/supply",
            "transform_function": transform_supply,
            "headers": ["Дата", "№ документа", "Контрагент", "Склад", "Наименование товара", "Ед.изм", "Количество", "Валюта", "Цена","Сумма", "Вес"]
        },
        {
            "name": "Возврат приход товар",
            "endpoint": "/entity/purchasereturn",
            "transform_function": transform_purchase_return,
            "headers": ["Дата", "№ документа", "№ документа приход товар","Контрагент", "Склад", "Наименование товара", "Ед.изм", "Количество", "Валюта", "Цена","Сумма", "Вес"]
        },
        {
            "name": "Продажа товар",
            "endpoint": "/entity/customerorder",
            "transform_function": transform_customer_order,
            "headers": ["Дата", "№ документа", "Склад", "Наименование товара", "Количество"]
        }
    ]

    # Process Each Task
    for task in tasks:
        try:
            print(f"[{datetime.now()}] Fetching data for {task['name']}...")
            raw_data = moysklad_client.fetch_paginated_data(task["endpoint"])
            print(f"[{datetime.now()}] Transforming data for {task['name']}...")
            transformed_data = task["transform_function"](
                raw_data, refs, moysklad_client)
            print(f"[{datetime.now()}] Writing data for {task['name']}...")
            sheets_handler.write_data(
                task["name"], transformed_data, task["headers"])
            print(f"[{datetime.now()}] Task {task['name']} completed successfully!")
        except Exception as e:
            print(f"[{datetime.now()}] Error processing {task['name']}: {str(e)}")

    print(f"[{datetime.now()}] MoySklad ETL completed successfully!")


if __name__ == "__main__":
    main()
