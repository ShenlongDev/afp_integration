from typing import Dict
import requests
from django.conf import settings
from django.core.cache import cache


class NetSuiteAuthService:
    def __init__(self, consolidation_key: str):
        self.consolidation_key = consolidation_key
        self.base_url = f"https://{consolidation_key}.suitetalk.api.netsuite.com"
        self.auth_url = f"https://{consolidation_key}.app.netsuite.com/app/login/oauth2/authorize.nl"
        self.token_url = f"{self.base_url}/services/rest/auth/oauth2/v1/token"


    def get_access_token(self) -> str:
        """Get cached token or refresh if expired"""
        cache_key = f"netsuite_token_{self.consolidation_key}"
        token = cache.get(cache_key)
       
        if not token:
            token = self._refresh_token()
            # Cache token for slightly less than its expiry time
            cache.set(cache_key, token, timeout=1700)  # 30 minutes - 100 seconds
           
        return token


    def _refresh_token(self) -> str:
        """Refresh OAuth 2.0 access token"""
        data = {
            'grant_type': 'refresh_token',
            'refresh_token': settings.NETSUITE_REFRESH_TOKEN,
        }
       
        headers = {
            'Cache-Control': 'no-cache',
            'Content-Type': 'application/x-www-form-urlencoded',
        }
       
        response = requests.post(
            self.token_url,
            data=data,
            headers=headers,
            auth=(settings.NETSUITE_CLIENT_ID, settings.NETSUITE_CLIENT_SECRET)
        )
        response.raise_for_status()
       
        return response.json()['access_token']
