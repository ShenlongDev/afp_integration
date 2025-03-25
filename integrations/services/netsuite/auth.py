import requests
import jwt 
from django.utils import timezone
from datetime import timedelta
from django.conf import settings

from integrations.models.models import Integration, IntegrationAccessToken

INTEGRATION_TYPE_NETSUITE = "NETSUITE"

class NetSuiteAuthService:
    """
    Handles NetSuite OAuth2 (Machine-to-Machine) authentication for a given NetSuite account.
    Uses the Integration model for client creds (including the RSA private key) and the
    IntegrationAccessToken for storing the access token.
    
    In this M2M implementation, the access token is obtained once via a JWT assertion and then
    stored. We do not attempt to fetch a new token automatically.
    """

    def __init__(self, integration: Integration):
        """
        :param integration: The Integration record that has netsuite_account_id,
                            netsuite_client_id, and netsuite_private_key.
        """
        self.integration = integration

        account_id = integration.netsuite_account_id
        if not account_id:
            raise ValueError("Integration missing netsuite_account_id.")

        # Token endpoint:
        # "https://{account_id}.suitetalk.api.netsuite.com/services/rest/auth/oauth2/v1/token"
        self.token_url = f"https://{account_id}.suitetalk.api.netsuite.com/services/rest/auth/oauth2/v1/token"
        self.scope = ['restlets','rest_webservices']

    def _generate_jwt_assertion(self) -> str:
        """
        Generates a JWT assertion using the integration's client ID, integration user ID,
        and private key. The JWT contains the required claims and is signed with PS256.
        """
        now = int(timezone.now().timestamp())
        
        # Add JWT header with kid (certificate ID) that matches your NetSuite integration
        headers = {
            "alg": "PS256",
            "typ": "JWT",
            "kid": self.integration.netsuite_certificate_id  # You need to store this in your integration model
        }
        
        payload = {
            "iss": self.integration.netsuite_consumer_key,  # Consumer key from integration record
            "aud": self.token_url,                          # Token endpoint URL
            "iat": now,
            "exp": now + 3600,                              # Valid for 1 hour
            "scope": self.scope                             # Already set to ['restlets','rest_webservices'] in __init__
        }
        
        # Sign the JWT using the RSA private key stored in the integration record
        # Pass the headers parameter to include the kid
        client_assertion = jwt.encode(payload, self.integration.netsuite_private_key, algorithm="PS256", headers=headers)
        return client_assertion


    def obtain_access_token(self) -> str:
        """
        Requests a new access token using the M2M (client credentials) flow.
        This method is intended to be called manually when you need a new token.
        """
        client_assertion = self._generate_jwt_assertion()
        data = {
            "grant_type": "client_credentials",
            "client_assertion_type": "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
            "client_assertion": client_assertion
        }
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        resp = requests.post(self.token_url, data=data, headers=headers)
        if resp.status_code != 200:
            raise Exception(f"NetSuite M2M token request failed: {resp.status_code} {resp.text}")
        
        tokens = resp.json()
        access_token = tokens.get("access_token")
        expires_in = tokens.get("expires_in", 3600)
        if not access_token:
            raise Exception("No access_token returned in M2M token request.")

        # Save the token. Since there is no refresh token in M2M, we store only the access token.
        self.save_tokens(access_token, None, expires_in)
        return access_token


    def save_tokens(self, access_token: str, refresh_token: str | None, expires_in: int):
        """
        Persists the access token to IntegrationAccessToken with integration_type="NETSUITE".
        For M2M, refresh_token is not applicable.
        """
        expire_time = timezone.now() + timedelta(seconds=int(expires_in))
        defaults = {
            "token": access_token,
            "expires_at": expire_time,
        }
        IntegrationAccessToken.objects.update_or_create(
            integration=self.integration,
            integration_type=INTEGRATION_TYPE_NETSUITE,
            defaults=defaults
        )

    def get_access_token(self) -> str:
        """
        Returns the saved NetSuite access_token. If it is not present,
        the user must manually obtain a token (via obtain_access_token).
        """
        try:
            token_obj = IntegrationAccessToken.objects.get(
                integration=self.integration,
                integration_type=INTEGRATION_TYPE_NETSUITE
            )
        except IntegrationAccessToken.DoesNotExist:
            raise Exception("No NetSuite token found. Please obtain a token first via M2M authentication.")

        return token_obj.token