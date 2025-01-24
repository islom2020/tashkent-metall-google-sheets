import requests
import time
import logging

# Configure logging
logging.basicConfig(
    filename="clients/moysklad_client.log",
    level=logging.ERROR,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


class MoyskladClient:
    BASE_URL = "https://api.moysklad.ru/api/remap/1.2"
    HEADERS = {
        "Authorization": "Basic YWRtaW5AY2hhaW5tZXRhbGw6VGFzaGtlbnQ3Nw==",
        "Accept-Encoding": "gzip",
        "Content-Type": "application/json"
    }
    RATE_LIMIT = 45  # Max 45 requests per 3 seconds
    WINDOW = 3
    TIMEOUT = 30  # Timeout for requests (seconds)

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
        # Build URL
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
                url = data.get("meta", {}).get("nextHref")  # Use pagination
                self.request_count += 1
            except Exception as e:
                logging.error(f"Error fetching paginated data from {url}: {str(e)}")
                break

        return all_data

    def fetch_all_organization_accounts(self):
        """
        Fetch all organizations and extract their accounts.

        :return: List of all accounts from all organizations.
        """
        all_accounts = []
        try:
            # Fetch all organizations with expanded accounts
            organizations = self.fetch_paginated_data("/entity/organization?expand=accounts&limit=100&offset=0")
            
            # Extract accounts from each organization
            for organization in organizations:
                accounts = organization.get("rows", [])
                all_accounts.extend(accounts)
        except Exception as e:
            logging.error(f"Error fetching or extracting accounts from organizations: {str(e)}")

        return all_accounts

    def fetch_currency_data(self):
        """Fetch currency data from the `/entity/currency` endpoint."""
        try:
            return self.fetch_paginated_data("/entity/currency")
        except Exception as e:
            logging.error(f"Error fetching currency data: {str(e)}")
            return []

    def _make_request(self, method, url, params=None, json=None):
        """Make an HTTP request to the API with enhanced error handling."""
        try:
            response = requests.request(
                method, url, headers=self.HEADERS, params=params, json=json, timeout=self.TIMEOUT
            )
            if response.status_code == 200:
                return response

            # Handle specific HTTP status codes
            if response.status_code == 429:  # Too Many Requests
                retry_after = int(response.headers.get("Retry-After", self.WINDOW))
                logging.warning(f"Rate limit hit. Retrying after {retry_after} seconds.")
                time.sleep(retry_after)
            elif response.status_code == 401:  # Unauthorized
                logging.error("Unauthorized. Please check your credentials.")
            elif response.status_code == 503:  # Service Unavailable
                logging.warning("Service temporarily unavailable. Retrying after a delay.")
                time.sleep(self.WINDOW)
            else:
                logging.error(f"Unexpected response: {response.status_code} - {response.text}")

            response.raise_for_status()

        except requests.exceptions.Timeout as e:
            logging.error(f"Request timed out: {url} - {str(e)}")
            raise
        except requests.exceptions.ConnectionError as e:
            logging.error(f"Connection error: {url} - {str(e)}")
            raise
        except requests.exceptions.RequestException as e:
            logging.error(f"Request exception: {url} - {str(e)}")
            raise