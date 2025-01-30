import requests
import logging
from datetime import datetime
from typing import Any, Iterator, Optional

logger = logging.getLogger(__name__)

def _get_items(
    api_url: str,
    item_key: str,
    access_token: str,
    tenant_id: str,
    since_date: Optional[datetime] = None
) -> Iterator[dict[str, Any]]:
    """
    Generic function to retrieve items from Xero API with pagination.
    """
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Xero-Tenant-Id': tenant_id,
        'Accept': 'application/json'
    }

    if since_date:
        # Correct: Add If-Modified-Since to headers
        headers['If-Modified-Since'] = since_date.strftime("%a, %d %b %Y %H:%M:%S GMT")

    params = {'page': 1}  # Start from page 1

    while True:
        try:
            response = requests.get(api_url, headers=headers, params=params)
            if response.status_code != 200:
                logger.error(f"Failed to retrieve {item_key}: {response.text}")
                break

            data = response.json()
            items = data.get(item_key, [])

            if not items:  # Xero returns empty array when no more pages
                logger.info(f"No more {item_key}. Ending pagination.")
                break

            for item in items:
                yield item

            params['page'] += 1  # Next page

        except requests.exceptions.RequestException as e:
            logger.exception(f"Request failed: {e}")
            break

def get_invoices(
    access_token: str,
    tenant_id: str,
    since_date: Optional[datetime] = None
) -> Iterator[dict[str, Any]]:
    """
    Retrieve invoices for a specified tenant from Xero.

    :param access_token: The user OAuth 2.0 access token.
    :param tenant_id: The Xero tenant ID for which to retrieve invoices.
    :param since_date: (Optional) If provided, only invoices created or
                       modified since this date will be returned.
    :yield: A dictionary with invoice data, ordered by modification date
            (oldest first).

    See also:
        https://developer.xero.com/documentation/api/accounting/invoices
    """
    api_url = "https://api.xero.com/api.xro/2.0/Invoices"
    return _get_items(
        api_url=api_url,
        item_key="Invoices",
        access_token=access_token,
        tenant_id=tenant_id,
        since_date=since_date,
    )

def get_bank_transactions(
    access_token: str,
    tenant_id: str,
    since_date: Optional[datetime] = None
) -> Iterator[dict[str, Any]]:
    """
    Retrieve bank transactions for a specified tenant from Xero.

    :param access_token: The user OAuth 2.0 access token.
    :param tenant_id: The Xero tenant ID for which to retrieve bank transactions.
    :param since_date: (Optional) If provided, only transactions created or
                       modified since this date will be returned.
    :yield: A dictionary with transaction data, ordered by modification date
            (oldest first).

    See also:
        https://developer.xero.com/documentation/api/accounting/banktransactions
    """
    api_url = "https://api.xero.com/api.xro/2.0/BankTransactions"
    return _get_items(
        api_url=api_url,
        item_key="BankTransactions",
        access_token=access_token,
        tenant_id=tenant_id,
        since_date=since_date,
    )

def get_contacts(
    access_token: str,
    tenant_id: str,
    since_date: Optional[datetime] = None
) -> Iterator[dict[str, Any]]:
    """
    Retrieve contacts for a specified tenant from Xero.

    :param access_token: The user OAuth 2.0 access token.
    :param tenant_id: The Xero tenant ID for which to retrieve contacts.
    :param since_date: (Optional) If provided, only contacts created or
                       modified since this date will be returned.
    :yield: A dictionary with contact data, ordered by modification date
            (oldest first).

    See also:
        https://developer.xero.com/documentation/api/accounting/contacts
    """
    api_url = "https://api.xero.com/api.xro/2.0/Contacts"
    return _get_items(
        api_url=api_url,
        item_key="Contacts",
        access_token=access_token,
        tenant_id=tenant_id,
        since_date=since_date,
    )

def get_journals(
    access_token: str,
    tenant_id: str,
    since_date: Optional[datetime] = None
) -> Iterator[dict[str, Any]]:
    """
    Retrieve journals for a specified tenant from Xero.

    :param access_token: The user OAuth 2.0 access token.
    :param tenant_id: The Xero tenant ID for which to retrieve journals.
    :param since_date: (Optional) If provided, only journals created or
                       modified since this date will be returned.
    :yield: A dictionary with journal data, ordered by modification date
            (oldest first).

    See also:
        https://developer.xero.com/documentation/api/accounting/journals
    """
    api_url = "https://api.xero.com/api.xro/2.0/Journals"
    return _get_items(
        api_url=api_url,
        item_key="Journals",
        access_token=access_token,
        tenant_id=tenant_id,
        since_date=since_date,
    )
