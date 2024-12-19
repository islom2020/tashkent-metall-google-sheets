import requests
import time
import logging

logging.basicConfig(filename="etl_errors.log", level=logging.ERROR,
                    format="%(asctime)s - %(levelname)s - %(message)s")


class MoyskladClient:
    BASE_URL = "https://api.moysklad.ru/api/remap/1.2"
    HEADERS = {
        "Authorization": "Basic YWRtaW5AY2hhaW5tZXRhbGw6VGFzaGtlbnQ3Nw==",
        "Accept-Encoding": "gzip",
        "Content-Type": "application/json"
    }
    RATE_LIMIT = 45  # Max 45 requests in 3 seconds
    WINDOW = 3
    MAX_RETRIES = 3
    TIMEOUT = 30

    def __init__(self):
        self.request_count = 0
        self.window_start = time.time()

    def wait_for_rate_limit(self):
        """Ensure compliance with API rate limits."""
        if self.request_count >= self.RATE_LIMIT:
            elapsed = time.time() - self.window_start
            if elapsed < self.WINDOW:
                time.sleep(self.WINDOW - elapsed)
            self.request_count = 0
            self.window_start = time.time()
    
    def fetch_paginated_data(self, endpoint, params=None):
        """Fetch data from the API with pagination support."""
        # Check if the endpoint is a full URL
        if endpoint.startswith("http://") or endpoint.startswith("https://"):
            url = endpoint
        else:
            url = f"{self.BASE_URL}{endpoint}"
        
        params = params or {}
        all_data = []

        while url:
            self.wait_for_rate_limit()
            try:
                response = self._make_request("GET", url, params=params)
                data = response.json()
                all_data.extend(data.get("rows", []))
                url = data.get("meta", {}).get("nextHref")  # Assume nextHref is a full URL
                self.request_count += 1
            except Exception as e:
                logging.error(f"Failed to fetch data from {url}: {str(e)}")
                break
        return all_data


    def _make_request(self, method, url, params=None, json=None):
        """Handle individual API requests with retries."""
        for attempt in range(self.MAX_RETRIES):
            try:
                response = requests.request(
                    method, url, headers=self.HEADERS, params=params, json=json, timeout=self.TIMEOUT
                )
                if response.status_code == 200:
                    return response
                elif response.status_code in {429, 503}:  # Rate-limit or service unavailable
                    time.sleep(self.WINDOW)
                else:
                    response.raise_for_status()
            except requests.exceptions.RequestException as e:
                if attempt == self.MAX_RETRIES - 1:
                    logging.error(f"Request failed: {url} - {str(e)}")
                    raise
        raise Exception(f"Failed to fetch data from {url} after {self.MAX_RETRIES} retries.")