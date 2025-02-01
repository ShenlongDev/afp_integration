import requests
import re
import logging
import datetime
from datetime import timedelta, datetime
from zoneinfo import ZoneInfo
from django.utils import timezone
from django.db import transaction
from rest_framework.response import Response

from integrations.models.models import Integration, IntegrationAccessToken
from integrations.models.xero.raw import (
    XeroAccountsRaw,
    XeroJournalsRaw,
    XeroContactsRaw,
    XeroInvoicesRaw,
    XeroBankTransactionsRaw,
    XeroBudgetsRaw,
    XeroBudgetPeriodBalancesRaw
)
from integrations.models.xero.transformations import (
    XeroJournalLines,
    XeroInvoiceLineItems,
)



logger = logging.getLogger(__name__)


### 1. Token & Utility Helpers ###

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
    scopes = "accounting.transactions accounting.settings accounting.reports.read accounting.journals.read accounting.budgets.read accounting.contacts"

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


def parse_xero_datetime(xero_date_str: str):
    """
    Parse a Xero date string, e.g. '/Date(1672533421427+0000)/'
    or an ISO8601 '2023-10-12T00:00:00' into a Python datetime.
    """
    if not xero_date_str:
        return None
    
    # Check for the /Date(...) format
    match = re.match(r'/Date\((\d+)([+-]\d{4})\)/', xero_date_str)
    if match:
        timestamp_ms, offset_str = match.groups()
        timestamp = int(timestamp_ms) / 1000.0
        dt = datetime.fromtimestamp(timestamp, tz=ZoneInfo("UTC"))
        # offset parsing if needed
        return dt

    # else assume it's ISO format
    try:
        # e.g. "2023-09-12T00:00:00"
        return datetime.fromisoformat(xero_date_str.replace("Z", "+00:00"))
    except ValueError:
        logger.warning(f"Unknown date format: {xero_date_str}")
        return None


def build_headers(integration: Integration, since_date=None) -> dict:
    """
    Helper to build the Xero request headers with valid token, 
    plus optional If-Modified-Since if needed.
    """
    token = get_valid_xero_token(integration)
    tenant_id = integration.xero_tenant_id
    headers = {
        "Authorization": f"Bearer {token}",
        "xero-tenant-id": tenant_id,
        "Accept": "application/json"
    }
    if since_date:
        headers["If-Modified-Since"] = since_date.strftime("%a, %d %b %Y %H:%M:%S GMT")
    return headers


### 2. Chart of Accounts ###

@transaction.atomic
def sync_xero_chart_of_accounts(integration: Integration, since_date=None):
    """
    Fetch chart of accounts from Xero and store them in XeroAccountsRaw.
    """
    logger.info("Syncing Xero Chart of Accounts...")
    headers = build_headers(integration, since_date)
    url = "https://api.xero.com/api.xro/2.0/Accounts"
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    accounts_data = response.json().get("Accounts", [])

    now_ts = timezone.now()
    print(accounts_data[:100], "accounts_data")

    for acct in accounts_data:
        account_id = acct.get("AccountID")
        if not account_id:
            logger.warning("Account entry missing 'AccountID'. Skipping record.")
            continue
        try:
            obj, created = XeroAccountsRaw.objects.update_or_create(
                tenant_id=integration.xero_tenant_id,
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
            obj.save()
           
        except Exception as e:
            logger.error(f"Error saving XeroAccountsRaw for AccountID {account_id}: {e}")
    logger.info(f"Imported/Updated {len(accounts_data)} Xero Accounts.")


### 3. Journal Lines (and transform into General Ledger) ###

@transaction.atomic
def import_xero_journal_lines(integration: Integration, since_date=None):
    """
    Import Xero Journals + lines into XeroJournalsRaw, XeroJournalLines,
    then optionally transform them into XeroGeneralLedger.
    """
    logger.info("Importing Xero Journals & Lines...")
    headers = build_headers(integration, since_date)
    url = "https://api.xero.com/api.xro/2.0/Journals"
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    journals_data = response.json().get("Journals", [])

    now_ts = timezone.now()

    for journal in journals_data:
        journal_id = journal.get("JournalID")
        if not journal_id:
            logger.warning("Skipping journal with no JournalID.")
            continue

        # Upsert into XeroJournalsRaw
        jr_defaults = {
            "journal_number": journal.get("JournalNumber"),
            "reference": journal.get("Reference"),
            "journal_date": parse_xero_datetime(journal.get("JournalDate")),
            "created_date_utc": parse_xero_datetime(journal.get("CreatedDateUTC")),
            "raw_payload": journal,
            "ingestion_timestamp": now_ts,
            "source_system": "XERO"
        }
        XeroJournalsRaw.objects.update_or_create(
            tenant_id=integration.xero_tenant_id,
            journal_id=journal_id,
            defaults=jr_defaults
        )

        # Process lines
        lines = journal.get("JournalLines", [])
        for line in lines:
            line_id = line.get("JournalLineID")
            if not line_id:
                logger.warning(f"Skipping line in Journal {journal_id} with no JournalLineID.")
                continue

            # handle tracking categories
            tcat = line.get("TrackingCategories", [])
            tracking_name = None
            tracking_option = None
            if tcat:
                # if you want only first
                tracking_name = tcat[0].get("Name")
                tracking_option = tcat[0].get("Option")

            jline_defaults = {
                "journal_number": journal.get("JournalNumber"),
                "tenant_id": integration.xero_tenant_id,
                "reference": journal.get("Reference"),
                "source_id": journal.get("SourceID"),
                "source_type": journal.get("SourceType"),
                "account_id": line.get("AccountID"),
                "account_code": line.get("AccountCode"),
                "account_type": line.get("AccountType"),
                "account_name": line.get("AccountName"),
                "description": line.get("Description"),
                "net_amount": line.get("NetAmount"),
                "gross_amount": line.get("GrossAmount"),
                "tax_amount": line.get("TaxAmount"),
                "journal_date": parse_xero_datetime(journal.get("JournalDate")),
                "created_date_utc": parse_xero_datetime(journal.get("CreatedDateUTC")),
                "ingestion_timestamp": now_ts,
                "source_system": "XERO",
                "tracking_category_name": tracking_name,
                "tracking_category_option": tracking_option,
            }
            XeroJournalLines.objects.update_or_create(
                journal_line_id=line_id,
                defaults=jline_defaults
            )

    logger.info("Completed Xero Journal import & transform.")


### 4. Contacts ###
def get_contacts(access_token: str, tenant_id: str, since_date=None):
    """
    Generator that fetches Xero Contacts with optional If-Modified-Since.
    Each request can be paged if needed. For simplicity, we do a single fetch.
    """
    url = "https://api.xero.com/api.xro/2.0/Contacts"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "xero-tenant-id": tenant_id,
        "Accept": "application/json"
    }
    if since_date:
        headers["If-Modified-Since"] = since_date.strftime("%a, %d %b %Y %H:%M:%S GMT")

    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json().get("Contacts", [])
    for contact in data:
        yield contact


@transaction.atomic
def import_xero_contacts(integration: Integration, since_date=None):
    """
    Import Xero contacts into XeroContactsRaw.
    """
    logger.info("Importing Xero Contacts...")
    access_token = get_valid_xero_token(integration)
    tenant_id = integration.xero_tenant_id

    now_ts = timezone.now()

    for contact in get_contacts(access_token, tenant_id, since_date):
        contact_id = contact.get("ContactID")
        if not contact_id:
            logger.warning("Skipping contact with no ContactID.")
            continue
        XeroContactsRaw.objects.update_or_create(
            tenant_id=tenant_id,
            contact_id=contact_id,
            defaults={
                "name": contact.get("Name"),
                "updated_date_utc": parse_xero_datetime(contact.get("UpdatedDateUTC")),
                "raw_payload": contact,
                "ingestion_timestamp": now_ts,
                "source_system": "XERO"
            }
        )
    logger.info("Completed Xero Contacts import.")


### 5. Invoices ###

def get_invoices(access_token: str, tenant_id: str, since_date=None):
    """
    Example: fetch Xero invoices. We can handle pagination if needed. 
    For simplicity, single fetch. 
    """
    url = "https://api.xero.com/api.xro/2.0/Invoices"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "xero-tenant-id": tenant_id,
        "Accept": "application/json"
    }
    if since_date:
        headers["If-Modified-Since"] = since_date.strftime("%a, %d %b %Y %H:%M:%S GMT")

    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json().get("Invoices", [])
    return data  # or yield each invoice

@transaction.atomic
def import_xero_invoices(integration: Integration, since_date=None):
    """
    Import Xero invoices + line items into XeroInvoicesRaw 
    and XeroInvoiceLineItems if you have such a model.
    """
    logger.info("Importing Xero Invoices...")
    access_token = get_valid_xero_token(integration)
    tenant_id = integration.xero_tenant_id

    invoices = get_invoices(access_token, tenant_id, since_date)
    now_ts = timezone.now()

    for inv in invoices:
        invoice_id = inv.get("InvoiceID")
        if not invoice_id:
            logger.warning("Skipping invoice with no InvoiceID.")
            continue

        XeroInvoicesRaw.objects.update_or_create(
            tenant_id=tenant_id,
            invoice_id=invoice_id,
            defaults={
                "invoice_number": inv.get("InvoiceNumber"),
                "reference": inv.get("Reference"),
                "date": parse_xero_datetime(inv.get("Date")),
                "due_date": parse_xero_datetime(inv.get("DueDate")),
                "updated_date_utc": parse_xero_datetime(inv.get("UpdatedDateUTC")),
                "fully_paid_on_date": parse_xero_datetime(inv.get("FullyPaidOnDate")),
                "raw_payload": inv,
                "ingestion_timestamp": now_ts,
                "source_system": "XERO"
            }
        )
        # Process line items
        for line in inv.get('LineItems', []):
            XeroInvoiceLineItems.objects.update_or_create(
                line_item_id=line['LineItemID'],
                invoice_id=inv['InvoiceID'],
                defaults={
                    'tenant_id': tenant_id,
                    'description': line.get('Description'),
                    'quantity': line.get('Quantity'),
                    'unit_amount': line.get('UnitAmount'),
                    'account_code': line.get('AccountCode'),
                }
            )
    logger.info("Completed Xero Invoices import.")


### 6. Bank Transactions ###

def get_bank_transactions(access_token: str, tenant_id: str, since_date=None):
    """
    Example: fetch Xero bank transactions. 
    """
    url = "https://api.xero.com/api.xro/2.0/BankTransactions"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "xero-tenant-id": tenant_id,
        "Accept": "application/json"
    }
    if since_date:
        headers["If-Modified-Since"] = since_date.strftime("%a, %d %b %Y %H:%M:%S GMT")

    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json().get("BankTransactions", [])


@transaction.atomic
def import_xero_bank_transactions(integration: Integration, since_date=None):
    logger.info("Importing Xero Bank Transactions...")
    access_token = get_valid_xero_token(integration)
    tenant_id = integration.xero_tenant_id

    now_ts = timezone.now()

    transactions = get_bank_transactions(access_token, tenant_id, since_date)
    for bt in transactions:
        bt_id = bt.get("BankTransactionID")
        if not bt_id:
            logger.warning("Skipping bank transaction with no BankTransactionID.")
            continue
        XeroBankTransactionsRaw.objects.update_or_create(
            bank_transaction_id=bt_id,
            tenant_id=tenant_id,
            defaults={
                "type": bt.get("Type"),
                "status": bt.get("Status"),
                "date": parse_xero_datetime(bt.get("Date")),
                "updated_date_utc": parse_xero_datetime(bt.get("UpdatedDateUTC")),
                "raw_payload": bt,
                "ingestion_timestamp": now_ts,
                "source_system": "XERO",
            }
        )
        
        for line in bt.get('LineItems', []):
            XeroInvoiceLineItems.objects.update_or_create(
                line_item_id=line['LineItemID'],
                invoice_id=bt['InvoiceID'],
                defaults={
                    'tenant_id': tenant_id,
                    'description': line.get('Description'),
                    'quantity': line.get('Quantity'),
                    'unit_amount': line.get('UnitAmount'),
                    'account_code': line.get('AccountCode'),
                    # Add other fields as needed
                }
            )
        
        

    logger.info("Completed Xero Bank Transactions import.")


### 7. Budgets + Budget Period Balances ###

def get_budgets(access_token: str, tenant_id: str):
    """
    Example function: Xero has a 'GET /Budgets' endpoint? 
    Actually, budget is partial doc in Xero. We'll assume an example.
    """
    url = "https://api.xero.com/api.xro/2.0/Budgets"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "xero-tenant-id": tenant_id,
        "Accept": "application/json"
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json().get("Budgets", [])


def get_budget_period_balances(access_token: str, tenant_id: str, budget_id: str):
    """
    For demonstration, we show a pseudo approach:
    A real Xero endpoint might not exist for "Budget Period Balances" specifically. 
    We'll assume you have some approach or custom logic. 
    """
    # e.g. "GET https://api.xero.com/api.xro/2.0/Budgets/{budget_id}/periodBalances"
    # This might not be an actual endpoint in Xero. 
    url = f"https://api.xero.com/api.xro/2.0/Budgets/{budget_id}/periodBalances"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "xero-tenant-id": tenant_id,
        "Accept": "application/json"
    }
    # This is hypothetical, you can adapt to real Xero docs
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        # Suppose the response is "PeriodBalances"
        return response.json().get("PeriodBalances", [])
    except requests.exceptions.HTTPError as e:
        if response.status_code == 404:
            # Maybe no period balances
            return []
        else:
            raise


@transaction.atomic
def import_xero_budgets(integration: Integration):
    """
    Import budgets and period balances from Xero.
    """
    logger.info("Importing Xero Budgets & Period Balances...")
    access_token = get_valid_xero_token(integration)
    tenant_id = integration.xero_tenant_id

    now_ts = timezone.now()

    # 1) Import budgets
    budgets = get_budgets(access_token, tenant_id)
    for budget in budgets:
        budget_id = budget.get("BudgetID")
        if not budget_id:
            logger.warning("Skipping budget with no BudgetID.")
            continue
        # upsert into XeroBudgetsRaw
        XeroBudgetsRaw.objects.update_or_create(
            budget_id=budget_id,
            tenant_id=tenant_id,
            defaults={
                "status": budget.get("Status"),
                "type": budget.get("Type"),
                "description": budget.get("Description"),
                "updated_date_utc": parse_xero_datetime(budget.get("UpdatedDateUTC")),
                "raw_payload": budget,
                "ingestion_timestamp": now_ts,
                "source_system": "XERO"
            }
        )

        # 2) Budget period balances
        period_balances = get_budget_period_balances(access_token, tenant_id, budget_id)
        for pb in period_balances:
            account_id = pb.get("AccountID")
            period = pb.get("Period")
            XeroBudgetPeriodBalancesRaw.objects.update_or_create(
                budget_id=budget_id,
                tenant_id=tenant_id,
                account_id=account_id,
                period=period,
                defaults={
                    "account_code": pb.get("AccountCode"),
                    "amount": pb.get("Amount"),
                    "notes": pb.get("Notes"),
                    "updated_date_utc": parse_xero_datetime(pb.get("UpdatedDateUTC")),
                    "ingestion_timestamp": now_ts,
                    "source_system": "XERO"
                }
            )

    logger.info("Completed Xero Budgets & Period Balances import.")


@transaction.atomic
def map_general_ledger(integration):
    """
    Merge data from:
      - XeroJournalLines (line-level)
      - XeroJournalsRaw (header data)
      - XeroAccountsRaw (account fields)
      - XeroInvoicesRaw (to fill invoice_number / invoice_url if line references an invoice)
    and write to XeroGeneralLedger.

    We fill all fields that exist in XeroGeneralLedger, including:
      org, tenant_id, tenant_name,
      journal_id, journal_number, journal_date, created_date,
      journal_line_id, journal_reference,
      source_id, source_type,
      tracking_category_name, tracking_category_option,
      account_id, account_code, account_type, account_name,
      account_status, account_tax_type, account_class,
      account_currency_code, account_reporting_code, account_reporting_code_name,
      account_description, account_system_account,
      statement, bank_account_type,
      journal_line_description, net_amount, gross_amount, tax_amount,
      invoice_number, invoice_url.

    The 'statement' field is inferred from 'Class' in the account raw_payload if present:
        if Class in ["REVENUE", "EXPENSE"] => "PL" else => "BS".
    This can be customized.

    This function does:
      1) For each XeroJournalLines record, find matching XeroJournalsRaw by (tenant_id, journal_id).
      2) Also find matching XeroAccountsRaw by (tenant_id, account_id) to fill account_ fields.
      3) If line.source_type == "ACCPAY"/"ACCREC" or similar, we attempt to find XeroInvoicesRaw by line.source_id for invoice_number, invoice_url.
      4) Upsert into XeroGeneralLedger using (tenant_id, journal_id, journal_line_id) as unique key,
         also set "org" = integration.org, "tenant_name" = integration.org.name.
    """

    from integrations.models.xero.transformations import XeroJournalLines
    from integrations.models.xero.raw import (
        XeroJournalsRaw, XeroAccountsRaw, XeroInvoicesRaw
    )
    from integrations.models.xero.analytics import XeroGeneralLedger

    # 1) Retrieve all lines
    lines = XeroJournalLines.objects.all()
    org_obj = integration.org
    org_name = org_obj.name if org_obj else None

    for line in lines:
        line_tenant_id = line.tenant_id
        line_journal_id = line.journal_id
        line_journal_line_id = line.journal_line_id

        # if not line_tenant_id or not line_journal_id or not line_journal_line_id:
        #     logger.warning(
        #         f"Skipping line with missing IDs: tenant_id={line_tenant_id}, "
        #         f"journal_id={line_journal_id}, line_id={line_journal_line_id}"
        #     )
        #     continue

        # (a) Find matching journal header
        try:
            header = XeroJournalsRaw.objects.get(
                tenant_id=line_tenant_id,
                journal_id=line_journal_id
            )
        except XeroJournalsRaw.DoesNotExist:
            header = None

        # (b) Find matching account raw if desired
        account_raw = None
        if line.account_id:
            try:
                account_raw = XeroAccountsRaw.objects.get(
                    tenant_id=line_tenant_id,
                    account_id=line.account_id
                )
            except XeroAccountsRaw.DoesNotExist:
                account_raw = None

        # (c) If source_type or source_id indicates an invoice, e.g. "ACCPAY" or "ACCREC", find invoice
        invoice_raw = None
        invoice_number = None
        invoice_url = None
        if line.source_type in ["ACCPAY", "ACCREC"]:  # or whatever your logic is
            if line.source_id:
                try:
                    invoice_raw = XeroInvoicesRaw.objects.get(
                        tenant_id=line_tenant_id,
                        invoice_id=line.source_id
                    )
                except XeroInvoicesRaw.DoesNotExist:
                    invoice_raw = None

        if invoice_raw:
            # Usually, Xero's raw_payload might contain "InvoiceNumber" or "Url"
            inv_payload = invoice_raw.raw_payload or {}
            invoice_number = inv_payload.get("InvoiceNumber")
            invoice_url = inv_payload.get("Url")

        # 2) Build the dictionary for the General Ledger record
        data = {}

        # (i) From the header if available
        if header:
            data["tenant_id"] = header.tenant_id
            data["journal_id"] = header.journal_id
            data["journal_number"] = header.journal_number
            if header.journal_date:
                # header.journal_date is a DateTimeField, but we only store a Date
                data["journal_date"] = header.journal_date.date()
            else:
                data["journal_date"] = None
            data["created_date"] = header.created_date_utc
            data["journal_reference"] = header.reference
        else:
            # fallback to line's data
            data["tenant_id"] = line_tenant_id
            data["journal_id"] = line_journal_id
            try:
                data["journal_number"] = (
                    int(line.journal_number) if line.journal_number else None
                )
            except (TypeError, ValueError):
                data["journal_number"] = None
            data["journal_date"] = line.journal_date
            data["created_date"] = line.created_date_utc
            data["journal_reference"] = line.reference

        # (ii) Fields from the line
        data["journal_line_id"] = line_journal_line_id
        data["source_id"] = line.source_id
        data["source_type"] = line.source_type
        data["tracking_category_name"] = line.tracking_category_name
        data["tracking_category_option"] = line.tracking_category_option
        data["account_id"] = line.account_id
        data["account_code"] = line.account_code
        data["account_type"] = line.account_type
        data["account_name"] = line.account_name
        data["journal_line_description"] = line.description
        data["net_amount"] = line.net_amount
        data["gross_amount"] = line.gross_amount
        data["tax_amount"] = line.tax_amount

        # (iii) Account detail fields from account_raw
        # For example, let's parse from account_raw.status or account_raw.raw_payload
        if account_raw:
            # Some Xero fields might be in top-level columns
            data["account_status"] = account_raw.status  # if you store it
            acct_payload = account_raw.raw_payload or {}
            data["account_tax_type"] = acct_payload.get("TaxType")
            data["account_class"] = acct_payload.get("Class")
            data["account_currency_code"] = acct_payload.get("CurrencyCode")
            data["account_reporting_code"] = acct_payload.get("ReportingCode")
            data["account_reporting_code_name"] = acct_payload.get("ReportingCodeName")
            data["account_description"] = acct_payload.get("Description")
            data["account_system_account"] = acct_payload.get("SystemAccount")
            data["bank_account_type"] = acct_payload.get("BankAccountType")

            # optional: a statement logic
            acct_class = acct_payload.get("Class", "").upper()
            if acct_class in ["REVENUE", "EXPENSE"]:
                data["statement"] = "PL"
            else:
                data["statement"] = "BS"
        else:
            data["account_status"] = None
            data["account_tax_type"] = None
            data["account_class"] = None
            data["account_currency_code"] = None
            data["account_reporting_code"] = None
            data["account_reporting_code_name"] = None
            data["account_description"] = None
            data["account_system_account"] = None
            data["bank_account_type"] = None
            data["statement"] = None

        # (iv) Invoice fields if we found an invoice
        data["invoice_number"] = invoice_number
        data["invoice_url"] = invoice_url

        # (v) Additional context from integration org
        # E.g. tenant_name from integration.org
        data["tenant_name"] = org_name

        # 3) Upsert into XeroGeneralLedger
        XeroGeneralLedger.objects.update_or_create(
            tenant_id=data["tenant_id"],
            journal_id=data["journal_id"],
            journal_line_id=data["journal_line_id"],
            defaults={
                **data,
                "org": org_obj
            }
        )

    logger.info("map_general_ledger finished: Mapped all lines into XeroGeneralLedger successfully.")


### 8. Master function to import everything ###

@transaction.atomic
def import_xero_data(integration: Integration, since_date=None):
    """
    Master function to import all Xero data we care about:
    1) Chart of Accounts
    2) Journal lines
    3) Contacts
    4) Invoices
    5) Bank Transactions
    6) Budgets
    """
    logger.info("Starting full Xero data import...")

    # 1. Accounts
    sync_xero_chart_of_accounts(integration, since_date)

    # 2. Journal Lines (which in turn calls transform to general ledger)
    import_xero_journal_lines(integration, since_date)

    # 3. Contacts
    import_xero_contacts(integration, since_date)

    # 4. Invoices
    import_xero_invoices(integration, since_date)

    # 5. Bank Transactions
    import_xero_bank_transactions(integration, since_date)

    # 6. Budgets
    import_xero_budgets(integration)
    
    # 7. General Ledger
    map_general_ledger(integration)

    logger.info("Finished full Xero data import successfully.")
