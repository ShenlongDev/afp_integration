import requests
from django.core.cache import cache
from django.utils import timezone
from integrations.models.models import Integration, IntegrationAccessToken
from datetime import timedelta

class NetSuiteAuthService:
    def __init__(self, consolidation_key: str):
        self.consolidation_key = consolidation_key
        self.base_url = f"https://{consolidation_key}.suitetalk.api.netsuite.com"
        self.auth_url = f"https://{consolidation_key}.app.netsuite.com/app/login/oauth2/authorize.nl"
        self.token_url = f"{self.base_url}/services/rest/auth/oauth2/v1/token"


    def save_access_token(self, token: str, integration: Integration) -> None:
        """Save the access token for the specific NetSuite account."""
        IntegrationAccessToken.objects.update_or_create(
            integration=integration,
            integration_type="NETSUITE",
            token=token,
            expires_at=timezone.now() + timedelta(seconds=3600)
        )

    def get_access_token(self, integration: Integration) -> str:
        """Get cached token or refresh if expired and save it."""
        cache_key = f"netsuite_token_{self.consolidation_key}"
        token = cache.get(cache_key)
       
        if not token:
            token = self._refresh_token(integration)
            # Cache token for slightly less than its expiry time
            cache.set(cache_key, token, timeout=1700)  # 30 minutes - 100 seconds
           
        return token

    def _refresh_token(self, integration: Integration) -> str:
        """Refresh OAuth 2.0 access token and save it."""
        refresh_token = integration.netsuite_refresh_token
        data = {
            'grant_type': 'refresh_token',
            'refresh_token': refresh_token,
        }
       
        headers = {
            'Cache-Control': 'no-cache',
            'Content-Type': 'application/x-www-form-urlencoded',
        }
       
        response = requests.post(
            self.token_url,
            data=data,
            headers=headers,
            auth=(integration.netsuite_client_id, integration.netsuite_client_secret)
        )
        
        token = response.json()['access_token']
        self.save_access_token(token, integration)  
        return token
    
    