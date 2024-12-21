import requests
import logging

# Configure logging
logging.basicConfig(
    filename="clients/workly_client.log",
    level=logging.ERROR,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

WORKLY_AUTH_URL = "https://api.workly.uz/v1/oauth/token"
WORKLY_INOUTS_URL = "https://api.workly.uz/v1/reports/inouts"


class WorklyClient:
    def __init__(self, client_id, client_secret, username, password):
        self.client_id = client_id
        self.client_secret = client_secret
        self.username = username
        self.password = password
        self.access_token = None
        self.refresh_token = None
        self.authenticate()

    def authenticate(self):
        """Authenticate with the Workly API and obtain tokens."""
        payload = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "password",
            "username": self.username,
            "password": self.password,
        }
        try:
            response = requests.post(WORKLY_AUTH_URL, json=payload, timeout=30)
            response.raise_for_status()
            auth_data = response.json()
            self.access_token = auth_data["access_token"]
            self.refresh_token = auth_data["refresh_token"]
        except requests.exceptions.Timeout:
            logging.error("Authentication request timed out.")
            raise Exception("Authentication request timed out. Please check your connection.")
        except requests.exceptions.RequestException as e:
            logging.error(f"Authentication request failed: {str(e)}")
            raise Exception(f"Authentication failed: {str(e)}")

    def refresh_access_token(self):
        """Refresh the access token using the refresh token."""
        payload = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "refresh_token",
            "refresh_token": self.refresh_token,
        }
        try:
            response = requests.post(WORKLY_AUTH_URL, json=payload, timeout=30)
            response.raise_for_status()
            auth_data = response.json()
            self.access_token = auth_data["access_token"]
            self.refresh_token = auth_data["refresh_token"]
        except requests.exceptions.Timeout:
            logging.error("Token refresh request timed out.")
            raise Exception("Token refresh request timed out. Please check your connection.")
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to refresh access token: {str(e)}")
            raise Exception(f"Failed to refresh access token: {str(e)}")

    def fetch_inouts(self, start_date):
        """Fetch inouts data starting from the given date."""
        headers = {"Authorization": f"Bearer {self.access_token}"}
        all_data = []
        page = 1

        while True:
            params = {"start_date": start_date, "page": page, "per-page": 50}
            try:
                response = requests.get(
                    WORKLY_INOUTS_URL, headers=headers, params=params, timeout=30
                )

                # Handle expired tokens
                if response.status_code == 401:
                    logging.warning("Token expired. Refreshing access token...")
                    self.refresh_access_token()
                    headers["Authorization"] = f"Bearer {self.access_token}"
                    continue

                response.raise_for_status()
                data = response.json()
                all_data.extend(data.get("items", []))

                # Check for next page
                if "next" not in data["_links"]:
                    break

                page += 1
            except requests.exceptions.Timeout:
                logging.error(f"Request timed out for page {page}.")
                raise Exception(f"Request timed out while fetching inouts for page {page}.")
            except requests.exceptions.RequestException as e:
                logging.error(f"Request failed for page {page}: {str(e)}")
                raise Exception(f"Failed to fetch inouts for page {page}: {str(e)}")
            except Exception as e:
                logging.error(f"Unexpected error on page {page}: {str(e)}")
                raise Exception(f"Unexpected error occurred: {str(e)}")

        return all_data