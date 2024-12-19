import logging
from datetime import datetime, timedelta

logging.basicConfig(filename="etl_errors.log", level=logging.ERROR,
                    format="%(asctime)s - %(levelname)s - %(message)s")


def transform_supply(data, refs, client):
    """Transform supply data for Google Sheets."""
    results = []
    for item in data:
        try:
            store_name = refs["stores"].get(item.get("store", {}).get("meta", {}).get("href"), "Unknown store")
            adjusted_moment = adjust_datetime(item['moment'])
            positions = client.fetch_paginated_data(item["positions"]["meta"]["href"])
            agentName = refs["agents"].get(item.get("agent", {}).get("meta", {}).get("href"), "Unknown agent")
            currencyName = refs["currencies"].get(item.get("rate", {}).get("currency", {}).get("meta", {}).get("href"), "Unknown currency")
            for position in positions:
                product = refs["products"].get(position.get("assortment", {}).get("meta", {}).get("href"), {})
                productUomName = refs["uoms"].get(product.get("uomMetaHref", {}), "Unknown uom")
                results.append([
                    adjusted_moment,  # Adjusted Date
                    item["name"],  # Document Number
                    agentName,
                    store_name,  # Store
                    product.get("name", "Unknown"),  # Product Name
                    productUomName,
                    position.get("quantity", "-"),  # Quantity
                    currencyName,
                    position.get("price", "-"),
                    item['sum'],
                    product.get("weight", "-")  # Weight
                ])
        except Exception as e:
            logging.error(f"Error processing supply entry {item.get('name', 'Unknown')}: {str(e)}")
    return results

def transform_purchase_return(data, refs, client):
    """Transform supply data for Google Sheets."""
    results = []
    for item in data:
        try:
            store_name = refs["stores"].get(item.get("store", {}).get("meta", {}).get("href"), "Unknown store")
            adjusted_moment = adjust_datetime(item['moment'])
            positions = client.fetch_paginated_data(item["positions"]["meta"]["href"])
            agentName = refs["agents"].get(item.get("agent", {}).get("meta", {}).get("href"), "Unknown agent")
            currencyName = refs["currencies"].get(item.get("rate", {}).get("currency", {}).get("meta", {}).get("href"), "Unknown currency")
            supplyName = refs["supplies"].get(item.get("supply",{}).get("meta",{}).get("href"),"-")
            for position in positions:
                product = refs["products"].get(position.get("assortment", {}).get("meta", {}).get("href"), {})
                productUomName = refs["uoms"].get(product.get("uomMetaHref", {}), "Unknown uom")
                results.append([
                    adjusted_moment,  # Adjusted Date
                    item["name"],  # Document Number
                    supplyName,
                    agentName,
                    store_name,  # Store
                    product.get("name", "Unknown"),  # Product Name
                    productUomName,
                    position.get("quantity", "-"),  # Quantity
                    currencyName,
                    position.get("price", "-"),
                    item['sum'],
                    product.get("weight", "-")  # Weight
                ])
        except Exception as e:
            logging.error(f"Error processing supply entry {item.get('name', 'Unknown')}: {str(e)}")
    return results


def transform_customer_order(data, refs, client):
    """Transform customer order data for Google Sheets."""
    results = []
    for item in data:
        try:
            store_name = refs["stores"].get(item.get("store", {}).get("meta", {}).get("href"), "Unknown")
            adjusted_moment = adjust_datetime(item['moment'])
            positions = client.fetch_paginated_data(item["positions"]["meta"]["href"])
            for position in positions:
                product = refs["products"].get(position.get("assortment", {}).get("meta", {}).get("href"), {})
                results.append([
                    adjusted_moment,  # Adjusted Date
                    item["name"],  # Document Number
                    store_name,  # Store
                    product.get("name", "Unknown"),  # Product Name
                    position.get("quantity", 0)  # Quantity
                ])
        except Exception as e:
            logging.error(f"Error processing customer order {item.get('name', 'Unknown')}: {str(e)}")
    return results

def adjust_datetime(datetime_str):
    """Adjust datetime string by adding 2 hours."""
    try:
        dt = datetime.fromisoformat(datetime_str.replace("Z", "+00:00"))  # Handle ISO format with UTC offset
        dt_adjusted = dt + timedelta(hours=2)
        return dt_adjusted.isoformat()  # Return adjusted datetime in ISO format
    except Exception as e:
        logging.error(f"Failed to adjust datetime: {datetime_str} - {str(e)}")
        return datetime_str  # Return original if adjustment fails