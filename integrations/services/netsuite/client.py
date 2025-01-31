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
        """Execute SuiteQL query with optional pagination."""
        url = f"https://{self.consolidation_key}.suitetalk.api.netsuite.com/services/rest/query/v1/suiteql"
       
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Prefer": "transient"
        }

        params = {"limit": limit}
        if offset is not None:
            params["offset"] = offset

        # Replace parameters in query
        if min_id is not None:
            query = query.replace("$min", str(min_id))

        data = {"q": query}

        logger.debug(f"Executing SuiteQL Query: {query}")
        logger.debug(f"With params: {params}")

        while True:
            response = requests.post(url, headers=headers, json=data, params=params)
            if response.status_code != 200:
                logger.error(f"SuiteQL Request Failed: {response.status_code} - {response.text}")
                raise Exception(f"SuiteQL Request Failed: {response.status_code} - {response.text}")

            results = response.json()
            logger.debug(f"SuiteQL Query Results: {results}")

            yield from results.get('items', [])
           
            # Check if we have more data
            if len(results.get('items', [])) < limit:
                break
               
            params["offset"] = params.get("offset", 0) + limit
