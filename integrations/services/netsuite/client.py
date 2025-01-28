from typing import Dict, Iterator
import requests
from django.conf import settings
from .auth import NetSuiteAuthService


class NetSuiteClient:
    def __init__(self, consolidation_key: str):
        self.consolidation_key = consolidation_key
        self.base_url = f"https://{consolidation_key}.suitetalk.api.netsuite.com"
        self.auth_service = NetSuiteAuthService(consolidation_key)


    def execute_suiteql(
        self,
        query: str,
        min_id: str = None,
        offset: int = None,
        limit: int = 1000
    ) -> Iterator[Dict]:
        """Execute SuiteQL query with pagination"""
        url = f"{self.base_url}/services/rest/query/v1/suiteql"
       
        headers = {
            "Authorization": f"Bearer {self.auth_service.get_access_token()}",
            "Prefer": "transient"
        }


        params = {"limit": limit}
        if offset is not None:
            params["offset"] = offset


        # Replace parameters in query
        if min_id is not None:
            query = query.replace("$min", str(min_id))


        data = {"q": query}


        while True:
            response = requests.post(url, headers=headers, json=data, params=params)
            response.raise_for_status()
           
            results = response.json()
            yield from results['items']
           
            # Check if we have more data
            if len(results['items']) < limit:
                break
               
            params["offset"] = params.get("offset", 0) + limit