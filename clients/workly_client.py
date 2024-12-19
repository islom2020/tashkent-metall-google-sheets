import requests
import time

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
        payload = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "password",
            "username": self.username,
            "password": self.password,
        }
        response = requests.post(WORKLY_AUTH_URL, json=payload)
        response.raise_for_status()
        auth_data = response.json()
        self.access_token = auth_data["access_token"]
        self.refresh_token = auth_data["refresh_token"]

    def refresh_access_token(self):
        payload = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "refresh_token",
            "refresh_token": self.refresh_token,
        }
        response = requests.post(WORKLY_AUTH_URL, json=payload)
        response.raise_for_status()
        auth_data = response.json()
        self.access_token = auth_data["access_token"]
        self.refresh_token = auth_data["refresh_token"]

    def fetch_inouts(self, start_date):
        headers = {"Authorization": f"Bearer {self.access_token}"}
        all_data = []
        page = 1

        while True:
            params = {"start_date": start_date, "page": page, "per-page": 50}
            response = requests.get(WORKLY_INOUTS_URL, headers=headers, params=params)

            # Handle expired tokens
            if response.status_code == 401:
                print("Token expired. Refreshing...")
                self.refresh_access_token()
                headers["Authorization"] = f"Bearer {self.access_token}"
                continue

            response.raise_for_status()
            data = response.json()
            all_data.extend(data.get("items", []))

            if "next" not in data["_links"]:
                break
            page += 1

            time.sleep(1)  # Avoid rate-limiting
        return all_data