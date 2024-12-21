import logging
from datetime import datetime, timedelta

logging.basicConfig(filename="etl/data_preparation.log", level=logging.ERROR,
                    format="%(asctime)s - %(levelname)s - %(message)s")

demand_attribute_kto_otgruzil = "fd17e1fe-ab2d-11ef-0a80-02cc003a75b0"
demand_attribute_brigada = "0a025ac2-ab2e-11ef-0a80-163f003cca55"


def transform_supply(data, refs, client):
    """Transform supply data for Google Sheets."""
    results = []
    for item in data:
        try:
            store_name = refs["stores"].get(item.get("store", {}).get("meta", {}).get("href"), "Unknown store")
            adjusted_moment = adjust_datetime(item['moment'])
            # positions = client.fetch_paginated_data(item["positions"]["meta"]["href"])
            positions = item["positions"].get("rows",[])
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
            # positions = client.fetch_paginated_data(item["positions"]["meta"]["href"])
            positions = item["positions"].get("rows", [])
            agentName = refs["agents"].get(item.get("agent", {}).get("meta", {}).get("href"), "Unknown agent")
            currencyName = refs["currencies"].get(item.get("rate", {}).get("currency", {}).get("meta", {}).get("href"), "Unknown currency")
            for position in positions:
                product = refs["products"].get(position.get("assortment", {}).get("meta", {}).get("href"), {})
                productUomName = refs["uoms"].get(product.get("uomMetaHref", {}), "Unknown uom")
                results.append([
                    adjusted_moment,  # Adjusted Date
                    item["name"],  # Document Number
                    item.get("supply",{}).get("name","-"),
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
            logging.error(f"Error processing purchase return {item.get('name', 'Unknown')}: {str(e)}")
    return results

def transform_sales_return(data, refs, client):
    """Transform supply data for Google Sheets."""
    results = []
    for item in data:
        try:
            store_name = refs["stores"].get(item.get("store", {}).get("meta", {}).get("href"), "Unknown store")
            adjusted_moment = adjust_datetime(item['moment'])
            # positions = client.fetch_paginated_data(item["positions"]["meta"]["href"])
            positions = item["positions"].get("rows", [])
            agentName = refs["agents"].get(item.get("agent", {}).get("meta", {}).get("href"), "Unknown agent")
            currencyName = refs["currencies"].get(item.get("rate", {}).get("currency", {}).get("meta", {}).get("href"), "Unknown currency")
            for position in positions:
                product = refs["products"].get(position.get("assortment", {}).get("meta", {}).get("href"), {})
                productUomName = refs["uoms"].get(product.get("uomMetaHref", {}), "Unknown uom")
                results.append([
                    adjusted_moment,  # Adjusted Date
                    item["name"],  # Document Number
                    item.get("demand",{}).get("name","-"),
                    agentName,
                    store_name,  # Store
                    product.get("name", "Unknown"),  # Product Name
                    productUomName,
                    position.get("quantity", "-"),  # Quantity
                    currencyName,
                    position.get("price", "-"),
                    product.get("weight", "-")  # Weight
                ])
        except Exception as e:
            logging.error(f"Error processing sales return {item.get('name', 'Unknown')}: {str(e)}")
    return results

def transform_move(data, refs, client):
    """Transform supply data for Google Sheets."""
    results = []
    for item in data:
        try:
            source_store_name = refs["stores"].get(item.get("sourceStore", {}).get("meta", {}).get("href"), "Unknown store")
            target_store_name = refs["stores"].get(item.get("targetStore", {}).get("meta", {}).get("href"), "Unknown store")
            adjusted_moment = adjust_datetime(item['moment'])
            # positions = client.fetch_paginated_data(item["positions"]["meta"]["href"])
            positions = item["positions"].get("rows", [])
            for position in positions:
                product = refs["products"].get(position.get("assortment", {}).get("meta", {}).get("href"), {})
                productUomName = refs["uoms"].get(product.get("uomMetaHref", {}), "Unknown uom")
                results.append([
                    adjusted_moment,  # Adjusted Date
                    item["name"],  # Document Number
                    source_store_name,
                    target_store_name,
                    product.get("name", "Unknown"),  # Product Name
                    productUomName,
                    position.get("quantity", "-"),  # Quantity
                    product.get("weight", "-")  # Weight
                ])
        except Exception as e:
            logging.error(f"Error processing move entry {item.get('name', 'Unknown')}: {str(e)}")
    return results


def transform_customer_order(data, refs, client):
    """Transform customer order data for Google Sheets."""
    results = []
    for item in data:
        try:
            store_name = refs["stores"].get(item.get("store", {}).get("meta", {}).get("href"), "Unknown")
            adjusted_moment = adjust_datetime(item['moment'])
            # positions = client.fetch_paginated_data(item["positions"]["meta"]["href"])
            positions = item["positions"].get("rows", [])
            attributes = item.get("attributes", {})

            for position in positions:
                product = refs["products"].get(position.get("assortment", {}).get("meta", {}).get("href"), {})
                results.append([
                    adjusted_moment,  # Adjusted Date
                    item["name"],  # Document Number
                    store_name,  # Store
                    product.get("name", "Unknown"),  # Product Name
                    position.get("quantity", 0),  # Quantity
                    get_attribute_value(attributes, demand_attribute_brigada),
                    get_attribute_value(attributes, demand_attribute_kto_otgruzil)
                ])
        except Exception as e:
            logging.error(f"Error processing customer order {item.get('name', 'Unknown')}: {str(e)}")
    return results

def transform_loss(data, refs, client):
    """Transform customer order data for Google Sheets."""
    results = []
    for item in data:
        try:
            store_name = refs["stores"].get(item.get("store", {}).get("meta", {}).get("href"), "Unknown")
            adjusted_moment = adjust_datetime(item['moment'])
            # positions = client.fetch_paginated_data(item["positions"]["meta"]["href"])
            positions = item.get("positions",{}).get("rows", [])

            for position in positions:
                product = refs["products"].get(position.get("assortment", {}).get("meta", {}).get("href"), {})
                productUomName = refs["uoms"].get(product.get("uomMetaHref", {}), "Unknown uom")

                results.append([
                    adjusted_moment,  # Adjusted Date
                    item["name"],  # Document Number
                    store_name,  # Store
                    product.get("name", "Unknown"),  # Product Name
                    position.get("quantity", 0),  # Quantity
                    productUomName,
                    product.get("weight", "-")  # Weight
                ])
        except Exception as e:
            logging.error(f"Error processing loss entry {item.get('name', 'Unknown')}: {str(e)}")
    return results

def transform_payment(data, refs, client):
    """Transform customer order data for Google Sheets."""
    results = []
    for item in data:
        try:
            accountName = refs["accounts"].get(item["accountId"], "Unknown account")
            adjusted_moment = adjust_datetime(item['moment'])
            currencyName = refs["currencies"].get(item.get("rate", {}).get("currency", {}).get("meta", {}).get("href"), "Unknown currency")
            agentName = refs["agents"].get(item.get("agent", {}).get("meta", {}).get("href"), "Unknown agent")

            results.append([
                adjusted_moment,  # Adjusted Date
                accountName,
                currencyName,
                item.get("expenseItem", {}).get("name","-"),
                agentName,
                item.get("paymentPurpose","-"),
                item["sum"]
            ])
        except Exception as e:
            logging.error(f"Error processing payment entry {item.get('name', 'Unknown')}: {str(e)}")
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
    
def get_attribute_value(attributes, target_id):
    """
    Retrieve the value.name of an attribute based on `id`.

    :param attributes: List of attribute dictionaries.
    :param target_id: The target id to match.
    :return: The name of the value if found, otherwise "-".
    """
    try:
        for attr in attributes:
            attr_id = attr.get("id")
            
            if (attr_id == target_id):
                return attr.get("value", {}).get("name")
        return "-"  # Return "-" if no match is found
    except Exception as e:
        logging.error(f"Error retrieving attribute value: {str(e)}")
        return "-"