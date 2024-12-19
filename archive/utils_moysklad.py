import requests
import time
import base64

BASE_URL = 'https://api.moysklad.ru/api/remap/1.2'

# MoySklad credentials
USERNAME = 'admin@chainmetall'
PASSWORD = 'Tashkent77'
AUTH_TOKEN = base64.b64encode(f"{USERNAME}:{PASSWORD}".encode()).decode()


# MoySklad field ids
PRICE_USD_ID = 'b81a34a9-3c4d-11ef-0a80-0e9b00276b5f'
PRICE_UZS_ID = 'c9fd23f7-5b96-11ef-0a80-13a900052a22'
PRICE_TRANSFER_ID = 'e07a6bdb-5df8-11ef-0a80-0cd400348e90'
PRICE_USD_UZS = '7b20da7a-b267-11ef-0a80-18530000c419'

CURRENCY_UZS_ID = 'b6b924be-3c50-11ef-0a80-09d80028874c'
CURRENCY_USD_ID = 'b81a0f17-3c4d-11ef-0a80-0e9b00276b5e'

# Attribute ID for USD Price
ATTRIBUTE_USD_ID = "3c2d9b05-b2ba-11ef-0a80-113c000345b9"
PATH_NAME_ARMATURA = 'АРМАТУРА'

# API Headers
HEADERS = {
    "Authorization": f"Basic {AUTH_TOKEN}",
    "Content-type": "application/json",
    "Accept-Encoding": "gzip"
}

def get_all_products_from_moysklad():
    url = f"{BASE_URL}/entity/product"
    products = []
    offset = 0
    limit = 100  # Respect API limitations: retrieve data in chunks

    while True:
        try:
            # Request a batch of products with offset and limit
            response = requests.get(url, headers=HEADERS, params={
                                    "limit": limit, "offset": offset})
            response.raise_for_status()
            data = response.json().get("rows", [])

            # Process each product in the batch
            for product in data:
                product_id = product.get("id", None)
                if product_id is None:
                    continue
                product_path_name = product.get("pathName", "Unknown").strip()

                # Get USD Price attribute, default to 0 if not present
                usd_price = 0
                if "attributes" in product:
                    for attribute in product["attributes"]:
                        if attribute.get("id") == ATTRIBUTE_USD_ID:
                            usd_price = attribute.get("value", 0)
                            break

                # Append product details to the result list
                products.append({
                    "ID": product_id,
                    "Name": product_path_name,
                    "USD Price": usd_price
                })

            # Break loop if no more data to fetch
            if len(data) < limit:
                break

            # Increment offset for the next batch
            offset += limit

            # Respect API rate limits
            time.sleep(1)

        except requests.exceptions.RequestException as e:
            print(f"Error fetching data: {e}")
            break

    return products
