import requests
import time
from datetime import datetime, timedelta
from django.utils import timezone
from typing import Iterator, Dict, Any


def get_xero_access_token(xero_integration) -> str:
    """
    Retrieve a valid Xero access token for the given XeroIntegration record.
    If the existing token is not expired, use it; otherwise request a new one
    from Xero's identity endpoint using client_credentials.
    """

    if xero_integration.access_token and xero_integration.access_token_expires_at:
        # If we still have > 1 minute left, return the existing token
        if xero_integration.access_token_expires_at > timezone.now() + timedelta(minutes=1):
            return xero_integration.access_token

    # Otherwise, fetch a new token
    token_url = "https://identity.xero.com/connect/token"
    auth = (xero_integration.client_id, xero_integration.client_secret)
    data = {
        "grant_type": "client_credentials",
        "scope": xero_integration.scopes,
    }

    response = requests.post(token_url, data=data, auth=auth)
    response.raise_for_status()

    token_json = response.json()
    new_access_token = token_json["access_token"]
    expires_in = token_json.get("expires_in", 1800)  # typically 1800 seconds (30 min)

    # Update the DB model
    xero_integration.access_token = new_access_token
    xero_integration.access_token_expires_at = timezone.now() + timedelta(seconds=expires_in)
    xero_integration.save(update_fields=["access_token", "access_token_expires_at"])

    return new_access_token


def get_journals(xero_integration, since_date: datetime | None = None, offset: int | None = None) -> Iterator[Dict[str, Any]]:
    """
    Retrieve journals for the specified XeroIntegration's tenant.
    """
    access_token = get_xero_access_token(xero_integration)  # get fresh or existing token

    headers = {
        "Authorization": f"Bearer {access_token}",
        "xero-tenant-id": xero_integration.tenant_id,
        "Accept": "application/json",
    }
    if since_date:
        # Format in RFC-3339 for If-Modified-Since
        headers["If-Modified-Since"] = since_date.strftime("%a, %d %b %Y %H:%M:%S GMT")

    endpoint = "https://api.xero.com/api.xro/2.0/Journals"
    
    while True:
        params = {"offset": offset} if offset else {}
        resp = requests.get(endpoint, headers=headers, params=params)
        resp.raise_for_status()
        data = resp.json()["Journals"]
        
        # yield each journal
        for journal in data:
            yield journal

        if len(data) < 100:
            break

        # increment offset
        offset = data[-1]["JournalNumber"]
        
        
def get_accounts(xero_integration, since_date=None):
    access_token = get_xero_access_token(xero_integration)
    headers = {
        "Authorization": f"Bearer {access_token}",
        "xero-tenant-id": xero_integration.tenant_id,
        "Accept": "application/json",
    }
    if since_date:
        headers["If-Modified-Since"] = since_date.strftime("%a, %d %b %Y %H:%M:%S GMT")

    url = "https://api.xero.com/api.xro/2.0/Accounts"
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json()["Accounts"]
