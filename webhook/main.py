from flask import Flask, jsonify
import redis
import json
from threading import Timer

app = Flask(__name__)

# Configure Redis client to connect to the Redis server
redis_client = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses=True)

# Function to simulate fetching customer orders from a database or external service
def get_customer_orders():
    return [
        {"id": 1, "customer_name": "John Doe", "order_amount": 99.99},
        {"id": 2, "customer_name": "Jane Smith", "order_amount": 149.49},
        {"id": 3, "customer_name": "Sam Wilson", "order_amount": 79.89}
    ]

# Function to periodically update the cached data in Redis
def update_cache():
    # Fetch fresh data using the ready-made function
    orders = get_customer_orders()
    # Store the data in Redis as a JSON string
    redis_client.set('customer_orders', json.dumps(orders))
    # Schedule this function to run again after 1 hour (3600 seconds)
    Timer(3600, update_cache).start()

# Define a route to handle GET requests at /customerOrder
@app.route('/customerOrder', methods=['GET'])
def customer_order_endpoint():
    # Attempt to fetch cached data from Redis
    cached_orders = redis_client.get('customer_orders')
    if cached_orders:
        # If data is found in cache, return it as a JSON response
        return jsonify(json.loads(cached_orders))
    else:
        # If cache is empty, fetch data, update the cache, and return the response
        orders = get_customer_orders()
        redis_client.set('customer_orders', json.dumps(orders))
        return jsonify(orders)

if __name__ == '__main__':
    # Start the periodic cache update process before running the server
    update_cache()
    # Run the Flask application on port 3508
    app.run(port=3508)
