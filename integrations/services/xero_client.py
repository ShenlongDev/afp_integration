import requests
from datetime import datetime, timedelta
from django.utils import timezone
from typing import Iterator, Dict, Any
from integrations.models import IntegrationAccessToken
from rest_framework.response import Response


def get_xero_access_token(xero_integration) -> str:
    """
    Retrieve a valid Xero access token for the given XeroIntegration record.
    If the existing token is not expired, use it; otherwise request a new one
    from Xero's identity endpoint using client_credentials.
    """

    if xero_integration.access_token and xero_integration.access_token_expires_at:
        if xero_integration.access_token_expires_at > timezone.now() + timedelta(minutes=1):
            return xero_integration.access_token

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
    expires_in = token_json.get("expires_in", 1800)  
    
    xero_integration.access_token = new_access_token
    xero_integration.access_token_expires_at = timezone.now() + timedelta(seconds=expires_in)
    xero_integration.save(update_fields=["access_token", "access_token_expires_at"])

    return new_access_token


def get_journals(xero_integration, since_date: datetime | None = None, offset: int | None = None) -> Iterator[Dict[str, Any]]:
    """
    Retrieve journals for the specified XeroIntegration's tenant.
    """
    access_token = get_xero_access_token(xero_integration) 

    headers = {
        "Authorization": f"Bearer {access_token}",
        "xero-tenant-id": xero_integration.tenant_id,
        "Accept": "application/json",
    }
    if since_date:
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


def authorize_xero(integration, scope):
    client_id = integration.xero_client_id
    client_secret = integration.xero_client_secret
    if not client_id or not client_secret:
        return Response(
            {"detail": "Xero client credentials not set on this Integration."},
            status=400
        )

    token_url = "https://identity.xero.com/connect/token"
    auth = (client_id, client_secret)

    data = {
        "grant_type": "client_credentials",
        "scope": scope
    }

    resp = requests.post(token_url, data=data, auth=auth)
    if resp.status_code != 200:
        return Response(
            {"detail": f"Xero token request failed: {resp.text}"},
            status=resp.status_code
        )

    token_data = resp.json()
    access_token = token_data["access_token"]
    expires_in = token_data.get("expires_in", 3600)
    expires_at = timezone.now() + timedelta(seconds=expires_in)

    IntegrationAccessToken.objects.create(
        integration=integration,
        integration_type="XERO",
        token=access_token,
        token_type="bearer",
        expires_at=expires_at
    )

    return Response(
        {
            "detail": "Xero token acquired successfully.",
            "access_token": access_token,
            "expires_at": expires_at.isoformat(),
        },
        status=200
    )
    
    
def get_valid_xero_token(integration: Integration) -> str:
    """
    Fetch the most recent, non-expired token from IntegrationAccessToken
    for the given Xero integration. If none is valid, raise or handle re-auth.
    """
    now = timezone.now()
    token_obj = (
        IntegrationAccessToken.objects.filter(
            integration=integration,
            integration_type="XERO",
            expires_at__gt=now
        )
        .order_by("-created_at")
        .first()
    )
    if not token_obj:
        raise ValueError("No valid Xero token found. Please re-authorize this integration.")

    return token_obj.token


def parse_xero_datetime(dt_str: str):
    """
    Xero may return datetimes in ISO8601 or /Date(...)/ format.
    We'll do a simple parse for ISO. If it's the /Date(...)/
    format, you'd parse differently.
    """
    if not dt_str:
        return None
    try:
        return date_parse(dt_str)
    except:
        # If it's /Date(123456789+0000)/, parse that with a custom approach
        # omitted here for brevity
        return None
    
    
@transaction.atomic
def sync_xero_chart_of_accounts(integration: Integration, since_date=None):
    """
    Pull /Accounts from Xero, upsert into ChartOfAccounts,
    create or rename the dynamic tables as needed.
    """
    if integration.integration_type != "XERO":
        raise ValueError("Integration is not type XERO, cannot sync Xero Chart of Accounts.")

    # 1) Retrieve a valid short-lived token
    access_token = get_valid_xero_token(integration)
    tenant_id = integration.xero_tenant_id

    headers = {
        "Authorization": f"Bearer {access_token}",
        "xero-tenant-id": tenant_id,
        "Accept": "application/json",
    }
    if since_date:
        headers["If-Modified-Since"] = since_date.strftime("%a, %d %b %Y %H:%M:%S GMT")

    url = "https://api.xero.com/api.xro/2.0/Accounts"
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    accounts_data = resp.json().get("Accounts", [])

    for acct in accounts_data:
        account_id = acct["AccountID"]
        code = acct.get("Code") or ""
        name = acct.get("Name") or ""
        old_table = None

        try:
            co = ChartOfAccounts.objects.get(account_id=account_id, integration=integration)
            old_table = co.table_name
        except ChartOfAccounts.DoesNotExist:
            co = ChartOfAccounts(integration=integration, account_id=account_id)

        co.code = code
        co.name = name
        co.status = acct.get("Status")
        co.account_type = acct.get("Type")
        co.tax_type = acct.get("TaxType")
        co.currency_code = acct.get("CurrencyCode")
        co.description = acct.get("Description")
        co.updated_utc = parse_xero_datetime(acct.get("UpdatedDateUTC"))

        new_table_name = sanitize_table_name(code, name)

        if not co.table_name:
            # first time creation
            co.table_name = new_table_name
            co.save()
            create_account_table(new_table_name)
        else:
            # rename if changed
            if old_table != new_table_name:
                rename_account_table(old_table, new_table_name)
                co.table_name = new_table_name

        co.save()


@transaction.atomic
def import_xero_journal_lines(integration: Integration, since_date=None):
    """
    Fetch /Journals from Xero, insert each JournalLine into the
    correct dynamic account table.
    If since_date is set, pass it as If-Modified-Since to only get new journals.
    """
    if integration.integration_type != "XERO":
        raise ValueError("Integration is not type XERO, cannot import Xero Journals.")

    access_token = get_valid_xero_token(integration)
    tenant_id = integration.xero_tenant_id

    headers = {
        "Authorization": f"Bearer {access_token}",
        "xero-tenant-id": tenant_id,
        "Accept": "application/json"
    }
    if since_date:
        headers["If-Modified-Since"] = since_date.strftime("%a, %d %b %Y %H:%M:%S GMT")

    url = "https://api.xero.com/api.xro/2.0/Journals"
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    journals_data = resp.json().get("Journals", [])

    for journal in journals_data:
        journal_date_str = journal.get("JournalDate")
        journal_date = parse_xero_datetime(journal_date_str)
        journal_id = journal.get("JournalID") 
        reference = journal.get("Reference")
        source_currency = journal.get("SourceCurrencyCode", "")
        status = journal.get("Status", "")

        lines = journal.get("JournalLines", [])
        for line in lines:
            acct_code = line.get("AccountCode")
            if not acct_code:
                continue 

            try:
                co = integration.chart_of_accounts.get(code=acct_code)
            except ChartOfAccounts.DoesNotExist:
                continue

            row_data = {
                "insights_unique_id": journal_id,  
                "date": journal_date,
                "reference": reference,
                "currency_code": source_currency,
                "status": status,
                "description": line.get("Description", ""),
                "quantity": 1,  # Xero doesn't provide quantity in JournalLine?
                "unit_amount": line.get("NetAmount", 0),
                "account_code": acct_code,
                "item_code": None, 
                "line_item_id": line.get("JournalLineID", ""), 
                "tax_type": line.get("TaxType"),
                "tax_amount": line.get("TaxAmount", 0),
                "line_amount": line.get("GrossAmount", 0),
                "tracking": None, 
            }

            insert_transaction_row(co.table_name, row_data)