import requests
from datetime import datetime, timedelta
from dateutil.parser import parse as date_parse
from django.db import transaction
from django.utils import timezone
from rest_framework.response import Response
from integrations.models.models import (
    Integration,
    IntegrationAccessToken,
)
from integrations.models.xero.raw import XeroAccountsRaw, XeroJournalsRaw


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
    Fetch chart of accounts from Xero and store them in your normal (raw or staging) table.
    No dynamic table creation. We'll do a raw upsert or store the raw JSON, 
    similar to the old merges but in Python.
    """
    access_token = get_valid_xero_token(integration)
    tenant_id = integration.xero_tenant_id

    headers = {
        "Authorization": f"Bearer {access_token}",
        "xero-tenant-id": tenant_id,
        "Accept": "application/json",
    }
    if since_date:
        # Xero's If-Modified-Since must be formatted
        headers["If-Modified-Since"] = since_date.strftime("%a, %d %b %Y %H:%M:%S GMT")

    url = "https://api.xero.com/api.xro/2.0/Accounts"
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    accounts_data = response.json().get("Accounts", [])

    now_ts = timezone.now()

    for acct in accounts_data:
        account_id = acct["AccountID"]
        # We'll do a simple "get or create" or "update or create" approach
        raw, created = XeroAccountsRaw.objects.update_or_create(
            tenant_id=tenant_id,
            account_id=account_id,
            defaults={
                "name": acct.get("Name"),
                "status": acct.get("Status"),
                "type": acct.get("Type"),
                "updated_date_utc": parse_xero_datetime(acct.get("UpdatedDateUTC")),
                "raw_payload": acct,  # store entire JSON if desired
                "ingestion_timestamp": now_ts,
                "source_system": "XERO",
            }
        )
        raw.save()


@transaction.atomic
def import_xero_journal_lines(integration: Integration, since_date=None):
    """
    Import Journal Lines from Xero, storing them in normalized raw/staging table 
    (no dynamic table creation).
    """
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
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    journals_data = response.json().get("Journals", [])

    now_ts = timezone.now()

    for journal in journals_data:
        journal_id = journal.get("JournalID") 
        reference = journal.get("Reference")
        journal_date = parse_xero_datetime(journal.get("JournalDate"))
        lines = journal.get("JournalLines", [])

        for line in lines:
            line_id = line.get("JournalLineID")
            # Upsert into your raw/staging table, e.g. XeroJournalsRaw
            XeroJournalsRaw.objects.update_or_create(
                tenant_id=tenant_id,
                journal_id=journal_id,
                defaults={
                    "reference": reference,
                    "journal_date": journal_date,
                    "raw_payload": journal,  # store entire journal JSON if you want
                    "ingestion_timestamp": now_ts,
                    "source_system": "XERO"
                }
            )

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
    
    