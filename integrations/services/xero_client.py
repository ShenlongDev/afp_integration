import requests
from datetime import datetime, timedelta
from dateutil.parser import parse as date_parse
from django.db import transaction
from django.utils import timezone
from rest_framework.response import Response
from rest_framework import status

from integrations.models.models import (
    Integration,
    IntegrationAccessToken,
    ChartOfAccounts,
    OrphanBankTransaction
)
from integrations.services.dynamic_tables import (
    sanitize_table_name,
    create_account_table,
    rename_account_table,
    insert_transaction_row
)

def get_valid_xero_token(integration: Integration) -> str:
    """
    Retrieve a valid Xero access token for the given Integration.
    If expired or not found, request a new one.
    """
    now = timezone.now()
    token_obj = (
        IntegrationAccessToken.objects.filter(
            integration=integration,
            integration_type="XERO",
            expires_at__gt=now + timedelta(minutes=1)
        )
        .order_by("-created_at")
        .first()
    )
    if token_obj:
        return token_obj.token

    # No valid token found; request a new one
    return request_new_xero_token(integration)

def request_new_xero_token(integration: Integration) -> str:
    """
    Request a new Xero access token using client_credentials and store it.
    """
    client_id = integration.xero_client_id
    client_secret = integration.xero_client_secret
    scopes = integration.xero_scopes or "accounting.transactions accounting.settings accounting.reports.read accounting.journals.read accounting.budgets.read"

    if not client_id or not client_secret:
        raise ValueError("Xero client credentials not set on this Integration.")

    token_url = "https://identity.xero.com/connect/token"
    auth = (client_id, client_secret)
    data = {
        "grant_type": "client_credentials",
        "scope": scopes,
    }

    response = requests.post(token_url, data=data, auth=auth)
    response.raise_for_status()

    token_data = response.json()
    access_token = token_data["access_token"]
    expires_in = token_data.get("expires_in", 1800)
    expires_at = timezone.now() + timedelta(seconds=expires_in)

    IntegrationAccessToken.objects.create(
        integration=integration,
        integration_type="XERO",
        token=access_token,
        expires_at=expires_at
    )

    return access_token

def parse_xero_datetime(dt_str: str):
    """
    Parse Xero datetime strings into Python datetime objects.
    """
    if not dt_str:
        return None
    try:
        return date_parse(dt_str)
    except:
        # Handle Xero's /Date(...) format if necessary
        return None

@transaction.atomic
def sync_xero_chart_of_accounts(integration: Integration, since_date=None):
    """
    Synchronize Chart of Accounts from Xero into local database and dynamic tables.
    """
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
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    accounts_data = response.json().get("Accounts", [])

    for acct in accounts_data:
        account_id = acct["AccountID"]
        code = acct.get("Code") or ""
        name = acct.get("Name") or ""
        org_name = integration.org.name

        try:
            co = ChartOfAccounts.objects.get(integration=integration, account_id=account_id)
            old_table = co.table_name
        except ChartOfAccounts.DoesNotExist:
            co = ChartOfAccounts(integration=integration, account_id=account_id)
            old_table = None

        co.code = code
        co.name = name
        co.status = acct.get("Status")
        co.account_type = acct.get("Type")
        co.tax_type = acct.get("TaxType")
        co.currency_code = acct.get("CurrencyCode")
        co.description = acct.get("Description")
        co.updated_utc = parse_xero_datetime(acct.get("UpdatedDateUTC"))

        # Generate unique table name using org name to prevent collisions
        new_table_name = sanitize_table_name(org_name, code, name)

        if not co.table_name:
            # First-time creation
            co.table_name = new_table_name
            co.save()
            create_account_table(new_table_name)
        else:
            # Rename table if the account name or code has changed
            if old_table != new_table_name:
                rename_account_table(old_table, new_table_name)
                co.table_name = new_table_name
                co.save()

        co.save()

@transaction.atomic
def import_xero_journal_lines(integration: Integration, since_date=None):
    """
    Import Journal Lines from Xero and insert them into respective dynamic account tables.
    """
    access_token = get_valid_xero_token(integration)
    tenant_id = integration.xero_tenant_id

    headers = {
        "Authorization": f"Bearer {access_token}",
        # "xero-tenant-id": tenant_id,
        "Accept": "application/json"
    }
    if since_date:
        headers["If-Modified-Since"] = since_date.strftime("%a, %d %b %Y %H:%M:%S GMT")

    url = "https://api.xero.com/api.xro/2.0/Journals"
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    journals_data = response.json().get("Journals", [])

    for journal in journals_data:
        journal_date = parse_xero_datetime(journal.get("JournalDate"))
        journal_id = journal.get("JournalID") 
        reference = journal.get("Reference")
        source_currency = journal.get("SourceCurrencyCode", "")
        status = journal.get("Status", "")

        lines = journal.get("JournalLines", [])
        for line in lines:
            acct_code = line.get("AccountCode")
            if not acct_code:
                continue  # Skip lines without an AccountCode

            try:
                co = ChartOfAccounts.objects.get(integration=integration, code=acct_code)
            except ChartOfAccounts.DoesNotExist:
                # Handle orphan: store in OrphanBankTransaction for manual mapping
                OrphanBankTransaction.objects.create(
                    integration=integration,
                    raw_data=line
                )
                continue

            row_data = {
                "insights_unique_id": journal_id,  
                "date": journal_date,
                "reference": reference,
                "currency_code": source_currency,
                "status": status,
                "description": line.get("Description", ""),
                "quantity": 1,  # Xero JournalLines typically don't have quantity
                "unit_amount": line.get("NetAmount", 0),
                "account_code": acct_code,
                "item_code": line.get("ItemCode"), 
                "line_item_id": line.get("JournalLineID", ""), 
                "tax_type": line.get("TaxType"),
                "tax_amount": line.get("TaxAmount", 0),
                "line_amount": line.get("GrossAmount", 0),
                "tracking": line.get("TrackingCategories")  # Store as JSON
            }

            insert_transaction_row(co.table_name, row_data)


def authorize_xero(integration):
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
        "scope": "accounting.transactions accounting.settings accounting.reports.read accounting.journals.read accounting.budgets.read"
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
    
    