from typing import Dict, Iterator, Optional
import requests
from django.conf import settings
from .auth import NetSuiteAuthService
from integrations.models.models import Integration
import logging

logger = logging.getLogger(__name__)

class NetSuiteClient:
    def __init__(self, consolidation_key: str, integration: Integration):
        self.consolidation_key = consolidation_key
        self.auth_service = NetSuiteAuthService(integration)
        self.token = self.auth_service.get_access_token()

def execute_suiteql(
    self,
    query: str,
    min_id: Optional[str] = None,
    offset: Optional[int] = None,
    limit: int = 1000
) -> Iterator[Dict]:
    url = f"https://{self.consolidation_key}.suitetalk.api.netsuite.com/services/rest/query/v1/suiteql"
    headers = {
        "Authorization": f"Bearer {self.token}",
        "Prefer": "transient"
    }
    params = {"limit": limit}
    if offset is not None:
        params["offset"] = offset
    if min_id is not None:
        query = query.replace("$min", str(min_id))
    data = {"q": query}
    logger.debug(f"Executing SuiteQL Query: {query}")
    logger.debug(f"With params: {params}")

    retry_attempt = 0
    while True:
        response = requests.post(url, headers=headers, json=data, params=params)
        if response.status_code == 401 and retry_attempt == 0:
            logger.info("Unauthorized response received, refreshing token and retrying query.")
            from integrations.models.models import IntegrationAccessToken
            try:
                token_obj = IntegrationAccessToken.objects.get(
                    integration=self.auth_service.integration,
                    integration_type="NETSUITE"
                )
            except IntegrationAccessToken.DoesNotExist:
                raise Exception("No token found; please authorize with NetSuite first.")
            # Force a refresh of the token.
            self.token = self.auth_service._refresh_token(token_obj)
            headers["Authorization"] = f"Bearer {self.token}"
            retry_attempt += 1
            continue  # Retry the request with the new token.

        if response.status_code != 200:
            raise Exception(f"SuiteQL Request Failed: {response.status_code} - {response.text}")

        results = response.json()
        logger.debug(f"SuiteQL Query Results: {results}")
        yield from results.get('items', [])

        if len(results.get('items', [])) < limit:
            break  # No more data.
        params["offset"] = params.get("offset", 0) + limit
        retry_attempt = 0 
