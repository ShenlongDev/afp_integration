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
from integrations.models.xero.transformations import XeroJournalLines
import logging
import datetime

logger = logging.getLogger(__name__)

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
    scopes = "accounting.transactions accounting.settings accounting.reports.read accounting.journals.read accounting.budgets.read"

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

def parse_xero_datetime(xero_date_str):
    """
    Parses Xero's date format and returns a Python datetime object.
    Example input: "/Date(1671753600000+0000)/"
    """
    if not xero_date_str:
        return None
    try:
        timestamp = int(xero_date_str.strip("/Date()").split("+")[0])
        return datetime.datetime.fromtimestamp(timestamp / 1000, tz=datetime.timezone.utc)
    except (ValueError, IndexError) as e:
        logger.error(f"Error parsing date string '{xero_date_str}': {e}")
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
                "raw_payload": acct, 
                "ingestion_timestamp": now_ts,
                "source_system": "XERO",
            }
        )
        raw.save()


@transaction.atomic
def import_xero_journal_lines(integration: Integration, since_date=None):
    """
    Import Journal Lines from Xero, storing them in normalized raw/staging tables 
    (no dynamic table creation).
    """
    try:
        access_token = get_valid_xero_token(integration)
        tenant_id = integration.xero_tenant_id
        
        # Debugging tenant_id
        logger.debug(f"Retrieved tenant_id: {tenant_id}")

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
            try:
                journal_id = journal.get("JournalID")
                journal_number = journal.get("JournalNumber")
                reference = journal.get("Reference")
                journal_date = parse_xero_datetime(journal.get("JournalDate"))
                created_date_utc = parse_xero_datetime(journal.get("CreatedDateUTC"))
                raw_payload = journal  # Assuming you assign raw_payload correctly
                ingestion_timestamp = now_ts  # Ensure ingestion_timestamp is set
                source_system = "XERO"

                if not journal_id:
                    logger.error(f"Missing 'JournalID' in journal data: {journal}")
                    continue

                # Upsert into XeroJournalsRaw
                journal_raw, created = XeroJournalsRaw.objects.update_or_create(
                    tenant_id=tenant_id,
                    journal_id=journal_id,
                    defaults={
                        "journal_number": journal_number,
                        "reference": reference,
                        "journal_date": journal_date,
                        "created_date_utc": created_date_utc,
                        "raw_payload": raw_payload,
                        "ingestion_timestamp": ingestion_timestamp,
                        "source_system": source_system
                    }
                )
                if created:
                    logger.info(f"Created XeroJournalsRaw: {journal_id}")
                else:
                    logger.info(f"Updated XeroJournalsRaw: {journal_id}")

                # Process Journal Lines
                lines = journal.get("JournalLines", [])
                for line in lines:
                    try:
                        line_id = line.get("JournalLineID")
                        account_id = line.get("AccountID")
                        account_code = line.get("AccountCode")
                        account_type = line.get("AccountType")  # Ensure this is retrieved
                        account_name = line.get("AccountName")
                        description = line.get("Description", "")
                        net_amount = line.get("NetAmount")
                        gross_amount = line.get("GrossAmount")
                        tax_amount = line.get("TaxAmount")
                        tracking_categories = line.get("TrackingCategories", [])

                        if not line_id:
                            logger.error(f"Missing 'JournalLineID' in journal line: {line}")
                            continue

                        # Extract tracking categories if needed
                        tracking_category_name = None
                        tracking_category_option = None
                        if tracking_categories:
                            # Assuming you want the first tracking category
                            tracking_category = tracking_categories[0]
                            tracking_category_name = tracking_category.get("Name")
                            tracking_category_option = tracking_category.get("Option")

                        # Debugging tenant_id before update_or_create
                        logger.debug(f"Upserting Journal Line with tenant_id: {tenant_id}")

                        # Upsert into XeroJournalLines
                        journal_line, line_created = XeroJournalLines.objects.update_or_create(
                            journal_line_id=line_id,
                            defaults={
                                "journal_id": journal_id,
                                "journal_number": journal_number,
                                "tenant_id": tenant_id,
                                "reference": reference,
                                "source_id": journal.get("SourceID"),
                                "source_type": journal.get("SourceType"),
                                "account_id": account_id,
                                "account_code": account_code,
                                "account_type": account_type,
                                "account_name": account_name,
                                "description": description,
                                "net_amount": net_amount,
                                "gross_amount": gross_amount,
                                "tax_amount": tax_amount,
                                "journal_date": journal_date,
                                "created_date_utc": created_date_utc,
                                "ingestion_timestamp": ingestion_timestamp,
                                "source_system": source_system,
                                "tracking_category_name": tracking_category_name,
                                "tracking_category_option": tracking_category_option,
                                "tracking_categories": tracking_categories
                            }
                        )
                        if line_created:
                            logger.info(f"Created XeroJournalLines: {line_id}")
                        else:
                            logger.info(f"Updated XeroJournalLines: {line_id}")

                    except Exception as e:
                        logger.exception(f"Failed to process journal line: {line_id} - Error: {e}")

            except Exception as e:
                logger.exception(f"Failed to process journal: {journal_id} - Error: {e}")

    except requests.exceptions.RequestException as req_err:
        logger.error(f"Request error while importing Xero journal lines: {req_err}")
    except Exception as e:
        logger.exception(f"Unexpected error in import_xero_journal_lines: {e}")

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
    
    