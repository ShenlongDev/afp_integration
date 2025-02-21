import requests
import logging

logger = logging.getLogger(__name__)

class ToastAuthService:
    """
    Handles authentication with the Toast API.
    """
    def __init__(self, hostname, client_id, client_secret):
        self.hostname = hostname.rstrip('/')
        self.client_id = client_id
        self.client_secret = client_secret
        self.token = None

    def login(self):
        """
        Authenticate with Toast and return an access token.
        """
        url = f"{self.hostname}/authentication/v1/authentication/login"
        payload = {
            "clientId": self.client_id,
            "clientSecret": self.client_secret,
            "hostname": self.hostname
        }
        try:
            response = requests.post(url, json=payload)
            response.raise_for_status()
            data = response.json()
            self.token = data.get("accessToken")
            if not self.token:
                logger.error("No access token received from Toast.")
                raise Exception("Toast authentication failed: No token")
            return self.token
        except requests.RequestException as e:
            logger.error("Toast authentication failed: %s", e)
            raise e
