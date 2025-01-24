from clients.moysklad_client import MoyskladClient
from etl.data_preparation import transform_supply, transform_customer_order, transform_purchase_return,transform_move, transform_sales_return,transform_loss, transform_payment
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
        "accounts": {str(item["accountId"]): item["name"] for item in client.fetch_all_organization_accounts()}
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
            "endpoint": "/entity/supply?expand=positions&limit=100&offset=0",
            "transform_function": transform_supply,
            "headers": ["Дата", "№ документа", "Контрагент", "Склад", "Наименование товара", "Ед.изм", "Количество", "Валюта", "Цена","Сумма", "Вес"]
        },
        {
            "name": "Возврат приход товар",
            "endpoint": "/entity/purchasereturn?expand=supply,positions&limit=100&offset=0",
            "transform_function": transform_purchase_return,
            "headers": ["Дата", "№ документа", "№ документа приход товар","Контрагент", "Склад", "Наименование товара", "Ед.изм", "Количество", "Валюта", "Цена","Сумма", "Вес"]
        },
        {
            "name": "Продажа товар",
            "endpoint": "/entity/demand?expand=positions&limit=100&offset=0",
            "transform_function": transform_customer_order,
            "headers": ["Дата", "№ документа", "Клиент", "Склад", "Наименование товара", "Ед.изм", "Валюта", "Цена", "Сумма", "Количество", "Вес", "Грузчик бригада", "Кто отгрузил"]
        },
        {
            "name": "Перемещение",
            "endpoint": "/entity/move?expand=positions&limit=100&offset=0",
            "transform_function": transform_move,
            "headers": ["Дата", "№ документа", "Склад отгрузка", "Склад приход", "Наименование товара", "Ед.изм", "Количество", "Вес"]
        },
        {
            "name": "Возврат продажа товар",
            "endpoint": "/entity/salesreturn?expand=demand,positions&limit=100&offset=0",
            "transform_function": transform_sales_return,
            "headers": ["Дата", "№ документа", "№ документа приход товар","Контрагент", "Склад", "Наименование товара", "Ед.изм", "Количество", "Валюта", "Цена","Сумма", "Вес"]
        },
        {
            "name": "Списание",
            "endpoint": "/entity/loss?expand=positions&limit=100&offset=0",
            "transform_function": transform_loss,
            "headers": ["Дата", "№ документа", "Склад", "Наименование товара", "Количество", "Ед.изм", "Вес"]
        },
        {
            "name": "Касса приход",
            "endpoint": "/entity/paymentin",
            "transform_function": transform_payment,
            "headers": ["Дата", "Кошелок", "Валюта", "Категория", "Контрагент", "Назначение", "Сумма"]
        },
        {
            "name": "Касса расход",
            "endpoint": "/entity/paymentout?expand=expenseItem&limit=100&offset=0",
            "transform_function": transform_payment,
            "headers": ["Дата", "Кошелок", "Валюта", "Категория", "Контрагент", "Назначение", "Сумма"]
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
            
            # print(f"[{datetime.now()}] Setting first column format datetime for {task['name']}...")
            # sheets_handler.set_column_format(
            #     task["name"], column_index=0, format_type="datetime")

            print(f"[{datetime.now()}] Task {task['name']} completed successfully!")
        except Exception as e:
            print(f"[{datetime.now()}] Error processing {task['name']}: {str(e)}")

    print(f"[{datetime.now()}] MoySklad ETL completed successfully!")


if __name__ == "__main__":
    main()
