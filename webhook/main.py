from flask import Flask, jsonify, request
import redis
import json
from threading import Timer
from functools import partial
from datetime import datetime
from clients.moysklad_client import MoyskladClient
from etl.data_preparation import transform_supply, transform_customer_order, transform_purchase_return, transform_move, transform_sales_return, transform_loss, transform_payment
import logging
import os
from etl.etl_moysklad_main import fetch_references

# Configure logging
logging.basicConfig(level=logging.INFO, filename='webhook.log', format='%(asctime)s - %(levelname)s - %(message)s')

app = Flask(__name__)

# Configure Redis clientto connect to the Redis server
redis_client = redis.StrictRedis(
    host='localhost', port=6379, db=0, decode_responses=True)

# MoyskladClient instance
moysklad_client = MoyskladClient()
refs = fetch_references(moysklad_client)

# Task configuration for different data types
tasks = [
    {
        "name": "Приход товар",
        "slug": "supply",
        "endpoint": "/entity/supply?expand=positions&limit=100&offset=0",
        "transform_function": transform_supply,
        "headers": ["Дата", "№ документа", "Контрагент", "Склад", "Наименование товара", "Ед.изм", "Количество", "Валюта", "Цена", "Сумма", "Вес"]
    },
    {
        "name": "Возврат приход товар",
        "slug": "purchaseReturn",
        "endpoint": "/entity/purchasereturn?expand=supply,positions&limit=100&offset=0",
        "transform_function": transform_purchase_return,
        "headers": ["Дата", "№ документа", "№ документа приход товар", "Контрагент", "Склад", "Наименование товара", "Ед.изм", "Количество", "Валюта", "Цена", "Сумма", "Вес"]
    },
    {
        "name": "Продажа товар",
        "slug": "customerOrder",
        "endpoint": "/entity/demand?expand=positions&limit=100&offset=0",
        "transform_function": transform_customer_order,
        "headers": ["Дата", "№ документа", "Клиент", "Склад", "Наименование товара", "Ед.изм", "Валюта", "Цена", "Сумма", "Количество", "Вес", "Грузчик бригада", "Кто отгрузил"]
    },
    {
        "name": "Перемещение",
        "slug": "move",
        "endpoint": "/entity/move?expand=positions&limit=100&offset=0",
        "transform_function": transform_move,
        "headers": ["Дата", "№ документа", "Склад отгрузка", "Склад приход", "Наименование товара", "Ед.изм", "Количество", "Вес"]
    },
    {
        "name": "Возврат продажа товар",
        "slug": "salesReturn",
        "endpoint": "/entity/salesreturn?expand=demand,positions&limit=100&offset=0",
        "transform_function": transform_sales_return,
        "headers": ["Дата", "№ документа", "№ документа приход товар", "Контрагент", "Склад", "Наименование товара", "Ед.изм", "Количество", "Валюта", "Цена", "Сумма", "Вес"]
    },
    {
        "name": "Списание",
        "slug": "loss",
        "endpoint": "/entity/loss?expand=positions&limit=100&offset=0",
        "transform_function": transform_loss,
        "headers": ["Дата", "№ документа", "Склад", "Наименование товара", "Количество", "Ед.изм", "Вес"]
    },
    {
        "name": "Касса приход",
        "slug": "paymentIn",
        "endpoint": "/entity/paymentin",
        "transform_function": transform_payment,
        "headers": ["Дата", "Кошелок", "Валюта", "Категория", "Контрагент", "Назначение", "Сумма"]
    },
    {
        "name": "Касса расход",
        "slug": "paymentOut",
        "endpoint": "/entity/paymentout?expand=expenseItem&limit=100&offset=0",
        "transform_function": transform_payment,
        "headers": ["Дата", "Кошелок", "Валюта", "Категория", "Контрагент", "Назначение", "Сумма"]
    }
]

# Function to fetch and transform data for a given task
def fetch_and_transform_data(task, moysklad_client, refs):
    logging.info(f"Fetching data for {task['name']}...")
    try:
        raw_data = moysklad_client.fetch_paginated_data(task["endpoint"])
    except Exception as e:
        logging.error(f"Error fetching data for {task['name']}: {e}")
        return []

    logging.info(f"Transforming data for {task['name']}...")
    try:
        transformed_data = task["transform_function"](
            data=raw_data, refs=refs, client=moysklad_client)
    except Exception as e:
        logging.error(f"Error transforming data for {task['name']}: {e}")
        return []
    return transformed_data

# Function to periodically update the cached data in Redis
def update_cache(refs):
    logging.info("Starting cache update")
    for task in tasks:
        transformed_data = fetch_and_transform_data(task, moysklad_client, refs)
        try:
            redis_client.set(task["slug"], json.dumps(transformed_data))
            logging.info(f"Updated cache for {task['name']} at {datetime.now()}")
        except Exception as e:
            logging.error(f"Error setting cache for {task['name']}: {e}")
    # Schedule this function to run again after 1 hour (3600 seconds)
    Timer(3600, partial(update_cache, refs)).start()

# Add basic authentication
def check_auth(username, password):
    return username == os.getenv('BASIC_AUTH_USERNAME', 'admin') and password == os.getenv('BASIC_AUTH_PASSWORD', 'metall_123')

def authenticate():
    return jsonify({"error": "Authentication required"}), 401

@app.before_request
def require_basic_auth():
    auth = request.authorization
    if not auth or not check_auth(auth.username, auth.password):
        return authenticate()

# Dynamic endpoint generation based on "slug"
@app.route('/<slug>', methods=['GET'])
def dynamic_endpoint(slug):
    task = next((task for task in tasks if task["slug"] == slug), None)
    if not task:
        return jsonify({"error": "Invalid endpoint"}), 404

    try:
        cached_data = redis_client.get(task["slug"])
    except Exception as e:
        logging.error(f"Error getting cache for {task['name']}: {e}")
        cached_data = None

    if cached_data:
        return jsonify(json.loads(cached_data))
    else:
        transformed_data = fetch_and_transform_data(task)
        try:
            redis_client.set(task["slug"], json.dumps(transformed_data))
        except Exception as e:
            logging.error(f"Error setting cache for {task['name']}: {e}")
        return jsonify(transformed_data)

@app.route('/data/<task_slug>', methods=['GET'])
def get_data(task_slug):
    logging.info(f"Fetching data for task: {task_slug}")
    data = redis_client.get(task_slug)
    if data:
        logging.info(f"Data found for task: {task_slug}")
        return jsonify(json.loads(data))
    else:
        logging.warning(f"No data found for task: {task_slug}")
        return jsonify([]), 404

if __name__ == '__main__':
    # Start the periodic cache update process before running the server
    update_cache(refs)
    # Run the Flask application on port 3508
    app.run(port=3508)