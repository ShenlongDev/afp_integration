import requests
from django.utils import timezone
from datetime import timedelta
from urllib.parse import urlencode

from integrations.models.models import Integration, IntegrationAccessToken

INTEGRATION_TYPE_NETSUITE = "NETSUITE"

class NetSuiteAuthService:
    """
    Handles NetSuite OAuth2 (Authorization Code flow) for a given NetSuite account.
    Uses the Integration model for client creds and the IntegrationAccessToken for stored tokens.
    """

    def __init__(self, integration: Integration):
        """
        :param integration: The Integration record that has netsuite_account_id,
                            netsuite_client_id, netsuite_client_secret
        """
        self.integration = integration

        # The NetSuite account ID, e.g. "1234567_SB2"
        # This is stored in integration.netsuite_account_id
        account_id = integration.netsuite_account_id

        if not account_id:
            raise ValueError("Integration missing netsuite_account_id.")

        # 1) Authorize endpoint:
        #    "https://{account_id}.app.netsuite.com/app/login/oauth2/authorize.nl"
        self.authorize_url = f"https://{account_id}.app.netsuite.com/app/login/oauth2/authorize.nl"

        # 2) Token endpoint:
        #    "https://{account_id}.suitetalk.api.netsuite.com/services/rest/auth/oauth2/v1/token"
        self.token_url = f"https://{account_id}.suitetalk.api.netsuite.com/services/rest/auth/oauth2/v1/token"

        # Must match exactly what is configured in NetSuite’s OAuth2 application settings.
        self.redirect_uri = "http://localhost:8000/integrations/auth/callback/"


        # Usually "rest_webservices" for NetSuite REST APIs
        self.scope = "rest_webservices"

    def get_authorization_url(self, state: str = "my-random-state") -> str:
        """
        Returns the NetSuite authorization URL to which you redirect the user.
        NetSuite will then ask them to log in and give you an authorization code.

        :param state: A random or unique string used to maintain state (CSRF protection).
        """
        if not (self.integration.netsuite_client_id and self.integration.netsuite_client_secret):
            raise ValueError("NetSuite client credentials not set on the Integration record.")

        params = {
            "response_type": "code",
            "client_id": self.integration.netsuite_client_id,
            "redirect_uri": self.redirect_uri,
            "scope": self.scope,
            "state": state,
        }
        query_string = urlencode(params)
        return f"{self.authorize_url}?{query_string}"

    def handle_callback(self, authorization_code: str):
        """
        Called after the user returns from NetSuite with ?code=... in the URL.
        Exchanges the code for an access_token (and optionally refresh_token),
        then stores them in the IntegrationAccessToken table with "NETSUITE" type.

        :param authorization_code: The code returned from NetSuite
        """
        # 1) Build the POST body for token exchange
        data = {
            "grant_type": "authorization_code",
            "code": authorization_code,
            "redirect_uri": self.redirect_uri,
        }

        # NetSuite requires basic auth with (client_id, client_secret).
        auth = (self.integration.netsuite_client_id, self.integration.netsuite_client_secret)

        headers = {"Content-Type": "application/x-www-form-urlencoded"}

        # 2) Request the tokens
        resp = requests.post(self.token_url, data=data, headers=headers, auth=auth)
        if resp.status_code != 200:
            raise Exception(f"NetSuite token exchange failed: {resp.status_code} {resp.text}")

        tokens = resp.json()
        print(tokens)
        access_token = tokens.get("access_token")
        refresh_token = tokens.get("refresh_token")
        expires_in = tokens.get("expires_in", 3600)

        if not access_token:
            raise Exception("No access_token in NetSuite token response.")

        # 3) Save to DB
        self.save_tokens(access_token, refresh_token, expires_in)

    def save_tokens(self, access_token: str, refresh_token: str | None, expires_in: int):
        """
        Persists the tokens to IntegrationAccessToken with integration_type="NETSUITE".
        If refresh_token is None, it means NetSuite didn't provide one (some configs).
        """
        expire_time = timezone.now() + timedelta(seconds=int(expires_in))

        defaults = {
            "token": access_token,
            "expires_at": expire_time,
        }
        if refresh_token:
            defaults["refresh_token"] = refresh_token

        IntegrationAccessToken.objects.update_or_create(
            integration=self.integration,
            integration_type=INTEGRATION_TYPE_NETSUITE,
            defaults=defaults
        )

    def get_access_token(self) -> str:
        """
        Returns a valid NetSuite access_token. If it doesn't exist or is expired,
        we attempt to refresh if a refresh_token is available. Otherwise, error out
        instructing re-authorization.
        """
        try:
            token_obj = IntegrationAccessToken.objects.get(
                integration=self.integration,
                integration_type=INTEGRATION_TYPE_NETSUITE
            )
        except IntegrationAccessToken.DoesNotExist:
            raise Exception("No NetSuite token found. Authorize first.")

        now = timezone.now()
        if token_obj.expires_at <= now:
            # Expired token -> try refresh if we have a refresh_token
            return self._refresh_token(token_obj)
        else:
            return token_obj.token

    def _refresh_token(self, token_obj: IntegrationAccessToken) -> str:
        """
        Refreshes NetSuite tokens if a refresh_token is stored. If none, user must re-auth.
        """
        if not token_obj.refresh_token:
            raise Exception("No refresh token. Must re-authorize with NetSuite.")

        data = {
            "grant_type": "refresh_token",
            "refresh_token": token_obj.refresh_token,
        }
        auth = (self.integration.netsuite_client_id, self.integration.netsuite_client_secret)
        headers = {"Content-Type": "application/x-www-form-urlencoded"}

        resp = requests.post(self.token_url, data=data, headers=headers, auth=auth)
        print(resp.status_code, resp.text, "_refresh_token")
        if resp.status_code != 200:
            raise Exception(f"Failed to refresh NetSuite token: {resp.status_code} {resp.text}")


        tokens = resp.json()
        new_access_token = tokens.get("access_token")
        new_refresh_token = tokens.get("refresh_token", token_obj.refresh_token)
        expires_in = tokens.get("expires_in", 3600)

        if not new_access_token:
            raise Exception("No 'access_token' returned by NetSuite refresh.")

        self.save_tokens(new_access_token, new_refresh_token, expires_in)
        return new_access_token
