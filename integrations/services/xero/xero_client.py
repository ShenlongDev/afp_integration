import requests
import re
import logging
import time  # Used for sleep on retry
from datetime import timedelta, datetime
from zoneinfo import ZoneInfo
from django.utils import timezone
from django.db import transaction
from integrations.services.utils import BatchUtils
from integrations.models.models import (
    Integration, 
    IntegrationAccessToken, 
    SyncTableLogs
)
from integrations.models.xero.raw import (
    XeroAccountsRaw,
    XeroJournalsRaw,
    XeroContactsRaw,
    XeroInvoicesRaw,
    XeroBankTransactionsRaw,
    XeroBudgetsRaw,
    XeroBudgetPeriodBalancesRaw,
)
from integrations.models.xero.transformations import (
    XeroJournalLines,
    XeroInvoiceLineItems,
    XeroJournalLineTrackingCategories
)
from integrations.models.xero.analytics import (
    XeroGeneralLedger, 
    XeroGeneralLedger1, 
    XeroGeneralLedger3
)

logger = logging.getLogger(__name__)

# -------------------------------------------------------------------
# NEW HELPER FUNCTION
# -------------------------------------------------------------------
def request_with_retry(method: str, url: str, **kwargs) -> requests.Response:
    """
    Performs an HTTP request with the given method and parameters.
    If a 429 (Too Many Requests) error is received, waits for 30 seconds and retries.
    This function can be used for all API calls.
    """
    while True:
        response = requests.request(method, url, **kwargs)
        try:
            response.raise_for_status()
            return response
        except requests.HTTPError as e:
            if response.status_code == 429:
                logger.warning(
                    "429 Too Many Requests for url %s. Waiting 30 seconds before retrying.", url
                )
                time.sleep(30)
            else:
                raise


class XeroDataImporter:
    """
    A refactored class-based approach to Xero data importing.
    Shared values (integration, since_date, etc.) are stored in __init__.
    """

    def __init__(self, integration: Integration, since_date=None):
        self.integration = integration
        self.since_date = since_date
        self.client_id = integration.xero_client_id
        self.client_secret = integration.xero_client_secret
        self.tenant_id = str(integration.org.id)
        
        if since_date is None:
            self.since_date = timezone.now().date()
        
    def get_paginated_results(self, url: str, result_key: str, extra_params: dict = None) -> list:
        results = []
        page = 1
        params = extra_params.copy() if extra_params else {}
        logger = logging.getLogger(__name__)

        while True:
            params.update({"page": page})
            headers = self.build_headers()
            response = requests.get(url, headers=headers, params=params)

            # Handle rate limit (HTTP 429)
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 60))
                logger.warning(f"Rate limit hit on page {page}. Retrying after {retry_after} seconds.")
                time.sleep(retry_after)
                continue

            response.raise_for_status()
            page_results = response.json().get(result_key, [])
            logger.info(f"Fetched {len(page_results)} records on page {page}")
            if not page_results:
                break
            results.extend(page_results)
            if len(page_results) < 100:
                break
            page += 1
        return results


    def request_new_xero_token(self):
        if not self.client_id or not self.client_secret:
            raise ValueError("Xero client credentials not set on this Integration.")

        token_url = "https://identity.xero.com/connect/token"
        auth = (self.client_id, self.client_secret)
        scopes = (
            "accounting.transactions accounting.settings accounting.reports.read "
            "accounting.journals.read accounting.budgets.read accounting.contacts"
        )
        data = {
            "grant_type": "client_credentials",
            "scope": scopes,
        }

        # Use our helper function to perform the POST request.
        response = request_with_retry("post", token_url, data=data, auth=auth)
        token_data = response.json()
        access_token = token_data["access_token"]
        expires_in = token_data.get("expires_in", 1800)
        expires_at = timezone.now() + timedelta(seconds=expires_in)

        IntegrationAccessToken.objects.update_or_create(
            integration=self.integration,
            integration_type="XERO",
            defaults={
                "token": access_token,
                "expires_at": expires_at
            }
        )
        return access_token

    def get_valid_xero_token(self) -> str:
        now = timezone.now()
        token_obj = (
            IntegrationAccessToken.objects.filter(
                integration=self.integration,
                integration_type="XERO",
                expires_at__gt=now + timedelta(minutes=1)
            )
            .order_by("-created_at")
            .first()
        )
        if token_obj:
            return token_obj.token

        return self.request_new_xero_token()

    def parse_xero_datetime(self, xero_date_str: str):
        if not xero_date_str:
            return None

        match = re.match(r'/Date\((\d+)([+-]\d{4})\)/', xero_date_str)
        if match:
            timestamp_ms, offset_str = match.groups()
            timestamp = int(timestamp_ms) / 1000.0
            dt = datetime.fromtimestamp(timestamp, tz=ZoneInfo("UTC"))
            return dt

        try:
            return datetime.fromisoformat(xero_date_str.replace("Z", "+00:00"))
        except ValueError:
            logger.warning(f"Unknown date format: {xero_date_str}")
            return None

    def build_headers(self, offset=None) -> dict:
        token = self.get_valid_xero_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json"
        }
        if self.since_date:
            headers["If-Modified-Since"] = self.since_date.strftime("%a, %d %b %Y %H:%M:%S GMT")
        if offset is not None:
            logger.debug(f"build_headers called with offset: {offset}")
        return headers

    def log_import_event(self, module_name: str, fetched_records: int):
        SyncTableLogs.objects.create(
            module_name=module_name,
            integration='XERO',
            organization=self.integration.org,
            fetched_records=fetched_records,
            last_updated_time=timezone.now(),
            last_updated_date=timezone.now().date()
        )

    def sync_xero_chart_of_accounts(self):
        logger.info("Syncing Xero Chart of Accounts...")
        headers = self.build_headers()
        url = "https://api.xero.com/api.xro/2.0/Accounts"

        # Use our retry helper instead of a direct requests.get call.
        response = request_with_retry("get", url, headers=headers)
        accounts_data = response.json().get("Accounts", [])
        now_ts = timezone.now()

        def process_account(acct):
            account_id = acct.get("AccountID")
            if not account_id:
                logger.warning("Account entry missing 'AccountID'. Skipping record.")
                return
            try:
                obj, created = XeroAccountsRaw.objects.update_or_create(
                    tenant_id=self.integration.org.id,
                    account_id=account_id,
                    defaults={
                        "name": acct.get("Name"),
                        "status": acct.get("Status"),
                        "type": acct.get("Type"),
                        "updated_date_utc": self.parse_xero_datetime(acct.get("UpdatedDateUTC")),
                        "raw_payload": acct,
                        "ingestion_timestamp": now_ts,
                        "source_system": "XERO",
                    }
                )
                obj.save()
            except Exception as e:
                logger.error(f"Error saving XeroAccountsRaw for AccountID {account_id}: {e}")

        BatchUtils.process_in_batches(accounts_data, process_account, batch_size=1000)
        self.log_import_event(module_name="xero_accounts", fetched_records=len(accounts_data))
        logger.info(f"Imported/Updated {len(accounts_data)} Xero Accounts.")

    def get_journals(self, offset=None):
        url = "https://api.xero.com/api.xro/2.0/Journals"
        headers = self.build_headers(offset=offset)
        params = {}
        if offset is not None:
            params["offset"] = offset

        # Use our retry helper for GET
        response = request_with_retry("get", url, headers=headers, params=params)
        journals = response.json().get("Journals", [])
        return journals

    def import_xero_journal_lines(self):
        logger.info("Importing Xero Journals & Lines with pagination...")
        offset = None
        total_fetched = 0

        while True:
            journals = self.get_journals(offset=offset)
            total_fetched += len(journals)
            logger.info(f"Fetched {len(journals)} journals")
            if not journals:
                break

            now_ts = timezone.now()

            def process_journal(journal):
                logger.debug(f"Processing journal: {journal}")
                journal_id = journal.get("JournalID")
                if not journal_id:
                    logger.warning("Skipping journal with no JournalID.")
                    return
                jr_defaults = {
                    "journal_number": journal.get("JournalNumber"),
                    "reference": journal.get("Reference"),
                    "journal_date": self.parse_xero_datetime(journal.get("JournalDate")),
                    "created_date_utc": self.parse_xero_datetime(journal.get("CreatedDateUTC")),
                    "raw_payload": journal,
                    "ingestion_timestamp": now_ts,
                    "source_system": "XERO"
                }
                XeroJournalsRaw.objects.update_or_create(
                    tenant_id=self.integration.org.id,
                    journal_id=journal_id,
                    defaults=jr_defaults
                )
                for line in journal.get("JournalLines", []):
                    line_id = line.get("JournalLineID")
                    if not line_id:
                        logger.warning(f"Skipping line in Journal {journal_id} with no JournalLineID.")
                        continue
                    # Process tracking categories from the journal line.
                    tcat = line.get("TrackingCategories", [])
                    # Use the first tracking category (if any) for default fields.
                    tracking_name = tcat[0].get("Name") if tcat else None
                    tracking_option = tcat[0].get("Option") if tcat else None

                    jline_defaults = {
                        "tenant_id": self.integration.org.id,
                        "journal_id": journal_id,
                        "reference": journal.get("Reference"),
                        "source_id": journal.get("SourceID"),
                        "journal_number": journal.get("JournalNumber"),
                        "source_type": journal.get("SourceType"),
                        "account_id": line.get("AccountID"),
                        "account_code": line.get("AccountCode"),
                        "account_type": line.get("AccountType"),
                        "account_name": line.get("AccountName"),
                        "description": line.get("Description"),
                        "net_amount": line.get("NetAmount"),
                        "gross_amount": line.get("GrossAmount"),
                        "tax_amount": line.get("TaxAmount"),
                        "journal_date": self.parse_xero_datetime(journal.get("JournalDate")),
                        "created_date_utc": self.parse_xero_datetime(journal.get("CreatedDateUTC")),
                        "ingestion_timestamp": now_ts,
                        "source_system": "XERO",
                        "tracking_category_name": tracking_name,
                        "tracking_category_option": tracking_option,
                    }
                    XeroJournalLines.objects.update_or_create(
                        journal_line_id=line_id,
                        defaults=jline_defaults
                    )
                    # Now, process each tracking category from the journal line.
                    if tcat:
                        for tracking in tcat:
                            XeroJournalLineTrackingCategories.objects.update_or_create(
                                tenant_id=self.integration.org.id,
                                journal_line_id=line_id,
                                tracking_category_id=tracking.get("TrackingCategoryID"),
                                defaults={
                                    # Optionally, set line_item_id as the same as line_id.
                                    "line_item_id": line_id,
                                    "tracking_option_id": tracking.get("TrackingOptionID"),
                                    "name": tracking.get("Name"),
                                    "option": tracking.get("Option"),
                                    "ingestion_timestamp": now_ts,
                                    "source_system": "XERO",
                                }
                            )

            BatchUtils.process_in_batches(journals, process_journal, batch_size=1000)
            if len(journals) < 100:
                break
            offset = journals[-1].get("JournalNumber")
            logger.info(f"Pagination: next offset: {offset}")

        self.log_import_event(module_name="xero_journal_lines", fetched_records=total_fetched)
        logger.info("Completed Xero Journal import & transform with pagination.")


    def get_contacts(self):
        return self.get_paginated_results("https://api.xero.com/api.xro/2.0/Contacts", "Contacts")


    def import_xero_contacts(self):
        logger.info("Importing Xero Contacts...")
        now_ts = timezone.now()
        contacts = self.get_contacts()

        def process_contact(contact):
            contact_id = contact.get("ContactID")
            if not contact_id:
                logger.warning("Skipping contact with no ContactID.")
                return
            XeroContactsRaw.objects.update_or_create(
                tenant_id=self.integration.org.id,
                contact_id=contact_id,
                defaults={
                    "name": contact.get("Name"),
                    "updated_date_utc": self.parse_xero_datetime(contact.get("UpdatedDateUTC")),
                    "raw_payload": contact,
                    "ingestion_timestamp": now_ts,
                    "source_system": "XERO"
                }
            )

        BatchUtils.process_in_batches(contacts, process_contact, batch_size=1000)
        self.log_import_event(module_name="xero_contacts", fetched_records=len(contacts))
        logger.info("Completed Xero Contacts import.")


    def get_invoices(self):
        return self.get_paginated_results("https://api.xero.com/api.xro/2.0/Invoices", "Invoices")


    def import_xero_invoices(self):
        logger.info("Importing Xero Invoices...")
        invoices = self.get_invoices()
        now_ts = timezone.now()

        def process_invoice(inv):
            invoice_id = inv.get("InvoiceID")
            if not invoice_id:
                logger.warning("Skipping invoice with no InvoiceID.")
                return
            XeroInvoicesRaw.objects.update_or_create(
                tenant_id=self.integration.org.id,
                invoice_id=invoice_id,
                defaults={
                    "invoice_number": inv.get("InvoiceNumber"),
                    "reference": inv.get("Reference"),
                    "date": self.parse_xero_datetime(inv.get("Date")),
                    "due_date": self.parse_xero_datetime(inv.get("DueDate")),
                    "updated_date_utc": self.parse_xero_datetime(inv.get("UpdatedDateUTC")),
                    "fully_paid_on_date": self.parse_xero_datetime(inv.get("FullyPaidOnDate")),
                    "raw_payload": inv,
                    "ingestion_timestamp": now_ts,
                    "source_system": "XERO"
                }
            )
            for line in inv.get('LineItems', []):
                XeroInvoiceLineItems.objects.update_or_create(
                    line_item_id=line['LineItemID'],
                    invoice_id=inv['InvoiceID'],
                    defaults={
                        "contact_id": inv.get("Contact", {}).get("ContactID"),
                        "contact_name": inv.get("Contact", {}).get("Name"),
                        "reference": inv.get("Reference"),
                        "date": self.parse_xero_datetime(inv.get("Date")),
                        "due_date": self.parse_xero_datetime(inv.get("DueDate")),
                        "updated_date_utc": self.parse_xero_datetime(inv.get("UpdatedDateUTC")),
                        "fully_paid_on_date": self.parse_xero_datetime(inv.get("FullyPaidOnDate")),
                        "invoice_number": inv.get("InvoiceNumber"),
                        "tax_amount": line.get("TaxAmount"),
                        "line_amount": line.get("LineAmount"),
                        "url": inv.get("Url"),
                        "type": inv.get("Type"),
                        'tenant_id': self.integration.org.id,
                        'description': line.get('Description'),
                        'quantity': line.get('Quantity'),
                        'unit_amount': line.get('UnitAmount'),
                        'account_code': line.get('AccountCode'),
                    }
                )

        BatchUtils.process_in_batches(invoices, process_invoice, batch_size=10000)
        self.log_import_event(module_name="xero_invoices", fetched_records=len(invoices))
        logger.info("Completed Xero Invoices import.")


    def get_bank_transactions(self):
        url = "https://api.xero.com/api.xro/2.0/BankTransactions"
        headers = {
            "Authorization": f"Bearer {self.get_valid_xero_token()}",
            "Accept": "application/json"
        }
        if self.since_date:
            headers["If-Modified-Since"] = self.since_date.strftime("%a, %d %b %Y %H:%M:%S GMT")

        results = []
        page = 1
        page_size = 1000000

        while True:
            params = {"page": page}
            response = request_with_retry("get", url, headers=headers, params=params)
            bank_transactions = response.json().get("BankTransactions", [])
            results.extend(bank_transactions)
            if len(bank_transactions) < page_size:
                break
            page += 1

        return results


    def import_xero_bank_transactions(self):
        logger.info("Importing Xero Bank Transactions...")
        now_ts = timezone.now()
        transactions = self.get_bank_transactions()

        def process_transaction(bt):
            bt_id = bt.get("BankTransactionID")
            if not bt_id:
                logger.warning("Skipping bank transaction with no BankTransactionID.")
                return
            XeroBankTransactionsRaw.objects.update_or_create(
                bank_transaction_id=bt_id,
                tenant_id=self.integration.org.id,
                defaults={
                    "type": bt.get("Type"),
                    "status": bt.get("Status"),
                    "date": self.parse_xero_datetime(bt.get("Date")),
                    "updated_date_utc": self.parse_xero_datetime(bt.get("UpdatedDateUTC")),
                    "raw_payload": bt,
                    "ingestion_timestamp": now_ts,
                    "source_system": "XERO",
                }
            )
            for line in bt.get('LineItems', []):
                XeroInvoiceLineItems.objects.update_or_create(
                    line_item_id=line['LineItemID'],
                    defaults={
                        'tenant_id': self.integration.org.id,
                        'description': line.get('Description'),
                        'quantity': line.get('Quantity'),
                        'unit_amount': line.get('UnitAmount'),
                        'account_code': line.get('AccountCode'),
                        'tax_type': line.get('TaxType'),
                        'tax_amount': line.get('TaxAmount'),
                        'line_amount': line.get('LineAmount'),
                    }
                )

        BatchUtils.process_in_batches(transactions, process_transaction, batch_size=1000)
        self.log_import_event(module_name="xero_bank_transactions", fetched_records=len(transactions))
        logger.info("Completed Xero Bank Transactions import.")


    def get_budgets(self):
        return self.get_paginated_results("https://api.xero.com/api.xro/2.0/Budgets", "Budgets")


    def get_budget_period_balances(self, budget_id: str):
        url = f"https://api.xero.com/api.xro/2.0/Budgets/{budget_id}"
        headers = {
            "Authorization": f"Bearer {self.get_valid_xero_token()}",
            "Accept": "application/json"
        }
        try:
            response = request_with_retry("get", url, headers=headers, params={
                "DateFrom": "2024-01-01",
                "DateTo": "2025-02-11"
            })
            return response.json().get("Budgets", [])
        except requests.exceptions.HTTPError as e:
            if response.status_code == 404:
                return []
            else:
                raise


    def import_xero_budgets(self):
        logger.info("Importing Xero Budgets & Period Balances...")
        now_ts = timezone.now()
        budgets = self.get_budgets()

        def process_budget(budget):
            budget_id = budget.get("BudgetID")
            if not budget_id:
                logger.warning("Skipping budget with no BudgetID.")
                return
            XeroBudgetsRaw.objects.update_or_create(
                budget_id=budget_id,
                tenant_id=self.integration.org.id,
                defaults={
                    "tenant_name": self.integration.org,
                    "status": budget.get("Status"),
                    "type": budget.get("Type"),
                    "description": budget.get("Description"),
                    "updated_date_utc": self.parse_xero_datetime(budget.get("UpdatedDateUTC")),
                    "raw_payload": budget,
                    "ingestion_timestamp": now_ts,
                    "source_system": "XERO"
                }
            )
            bp_response = self.get_budget_period_balances(budget_id)
            if not bp_response:
                logger.warning(f"No period balances found for budget_id: {budget_id}")
                return
            for b_item in bp_response:
                tracking_list = b_item.get("Tracking", [])
                tracking_obj = tracking_list[0] if tracking_list else {}
                budget_lines = b_item.get("BudgetLines", [])
                for line in budget_lines:
                    account_id = line.get("AccountID")
                    account_code = line.get("AccountCode")
                    try:
                        account = XeroAccountsRaw.objects.get(
                            tenant_id=self.integration.org.id,
                            account_id=account_id
                        )
                        reporting_code = account.raw_payload.get("ReportingCode")
                        reporting_code_name = account.raw_payload.get("ReportingCodeName")
                        account_name = account.name
                    except XeroAccountsRaw.DoesNotExist:
                        account_name = None
                    raw_balances = line.get("BudgetBalances", [])
                    sorted_balances = sorted(raw_balances, key=lambda x: x.get("Period"))
                    for pb in sorted_balances:
                        period = pb.get("Period")
                        amount = pb.get("Amount")
                        notes = pb.get("Notes")
                        updated_date_utc = self.parse_xero_datetime(b_item.get("UpdatedDateUTC"))
                        XeroBudgetPeriodBalancesRaw.objects.update_or_create(
                            budget_id=budget_id,
                            tenant_id=self.integration.org.id,
                            account_id=account_id,
                            period=period,
                            defaults={
                                "tenant_name": self.integration.org,
                                "account_code": account_code,
                                "account_name": account_name,
                                "reporting_code": reporting_code,
                                "reporting_code_name": reporting_code_name,
                                "amount": amount,
                                "notes": notes,
                                "updated_date_utc": updated_date_utc,
                                "ingestion_timestamp": now_ts,
                                "source_system": "XERO",
                                "tracking_category_id": tracking_obj.get("TrackingCategoryID"),
                                "tracking_category_name": tracking_obj.get("Name"),
                                "tracking_category_option": tracking_obj.get("Option")
                            }
                        )

        BatchUtils.process_in_batches(budgets, process_budget, batch_size=1000)
        self.log_import_event(module_name="xero_budgets", fetched_records=len(budgets))
        logger.info("Completed Xero Budgets & Period Balances import.")


    def map_xero_general_ledger(self):
        """
        Upsert (update or create) Xero General Ledger entries in batches.
        This function computes the latest desired state from the staging tables,
        then compares it against the existing GL records (keyed by tenant_id and journal_line_id).
        Instead of deleting old entries, it only updates changed records and creates new ones,
        processing in batches (10K per go) to be robust for large volumes.
        """
        from django.db import transaction, close_old_connections

        # 1) Identify the newest row per (tenant_id, journal_line_id) from the staging table.
        all_lines = XeroJournalLines.objects.order_by('-journal_date')
        latest_by_line = {}
        for line in all_lines:
            key = (line.tenant_id, line.journal_line_id)
            if key not in latest_by_line:
                latest_by_line[key] = line

        # 2) Build a dict mapping (tenant_id, journal_line_id) -> desired GL data.
        new_data_dict = {}
        for (tenant_id, journal_line_id), jl in latest_by_line.items():
            try:
                # Get the tracking category (if exists).
                try:
                    jtc = XeroJournalLineTrackingCategories.objects.get(
                        tenant_id=tenant_id,
                        journal_line_id=journal_line_id
                    )
                except XeroJournalLineTrackingCategories.DoesNotExist:
                    jtc = None

                # Get account information (if available).
                try:
                    acct = XeroAccountsRaw.objects.get(
                        tenant_id=tenant_id,
                        account_id=jl.account_id
                    )
                except XeroAccountsRaw.DoesNotExist:
                    acct = None

                contact_name = None
                invoice_description_fallback = None
                invoice_number = None
                invoice_url = None

                # Retrieve invoice metadata for ACCPAY / ACCREC types.
                if jl.source_type in ["ACCPAY", "ACCREC"]:
                    try:
                        inv = XeroInvoicesRaw.objects.get(
                            tenant_id=tenant_id,
                            invoice_id=jl.source_id
                        )
                        inv_payload = inv.raw_payload or {}
                        contact_name = inv_payload.get("Contact", {}).get("Name")
                        invoice_description_fallback = inv_payload.get("Description")
                        invoice_number = inv_payload.get("Reference")
                        invoice_url = inv_payload.get("Url")
                    except XeroInvoicesRaw.DoesNotExist:
                        pass
                else:
                    try:
                        bt = XeroBankTransactionsRaw.objects.get(
                            tenant_id=tenant_id,
                            bank_transaction_id=jl.source_id
                        )
                        bt_payload = bt.raw_payload or {}
                        contact_name = bt_payload.get("Contact", {}).get("Name")
                    except XeroBankTransactionsRaw.DoesNotExist:
                        pass

                # Build a candidate description.
                if contact_name or jl.description:
                    base = f"{contact_name} - " if contact_name else ""
                    desc_candidate = base + jl.description if jl.description else invoice_description_fallback
                else:
                    desc_candidate = invoice_description_fallback

                # Finalize the journal reference.
                ref = jl.reference
                if ref == desc_candidate:
                    final_journal_reference = ref
                elif ref is not None and desc_candidate is not None:
                    final_journal_reference = f"{ref} - {desc_candidate}"
                elif ref is None and desc_candidate is not None:
                    final_journal_reference = desc_candidate
                else:
                    final_journal_reference = ref

                invoice_description = desc_candidate if desc_candidate else invoice_description_fallback

                if acct:
                    account_code = acct.raw_payload.get("Code") if acct.raw_payload else None
                    account_type = acct.raw_payload.get("Type") if acct.raw_payload else None
                    account_name = acct.raw_payload.get("Name") if acct.raw_payload else None
                    account_status = acct.status
                    account_tax_type = (acct.raw_payload or {}).get("TaxType")
                    account_class = (acct.raw_payload or {}).get("Class")
                    # New fields added for reporting code and reporting name:
                    reporting_code = (acct.raw_payload or {}).get("ReportingCode")
                    reporting_code_name = (acct.raw_payload or {}).get("ReportingCodeName")
                    acct_class_value = (acct.raw_payload or {}).get("Class", "").upper()
                    statement_val = "PL" if acct_class_value in ["REVENUE", "EXPENSE"] else "BS"
                else:
                    account_code = jl.account_code
                    account_type = jl.account_type
                    account_name = jl.account_name
                    account_status = None
                    account_tax_type = None
                    account_class = None
                    reporting_code = None
                    reporting_code_name = None
                    statement_val = None

                # Retrieve the organization from Integration using tenant_id.
                integration = Integration.objects.get(id=tenant_id)
                org = integration.org

                # Prepare the data dict for the GL record.
                data = {
                    "org": org,
                    "tenant_id": tenant_id,
                    "tenant_name": org.name,
                    "journal_id": jl.journal_id,
                    "journal_number": int(jl.journal_number) if jl.journal_number else None,
                    "journal_date": jl.journal_date,
                    "created_date": jl.created_date_utc,
                    "journal_line_id": journal_line_id,
                    "journal_reference": final_journal_reference,
                    "source_id": jl.source_id,
                    "source_type": jl.source_type,
                    "tracking_category_name": jtc.name if jtc else None,
                    "tracking_category_option": jtc.option if jtc else None,
                    "account_id": jl.account_id,
                    "account_code": account_code or jl.account_code,
                    "account_type": account_type or jl.account_type,
                    "account_name": account_name or jl.account_name,
                    "account_status": account_status,
                    "account_tax_type": account_tax_type,
                    "account_class": account_class,
                    "reporting_code": reporting_code,
                    "reporting_code_name": reporting_code_name,
                    "statement": statement_val,
                    "bank_account_type": None,
                    "journal_line_description": jl.description,
                    "net_amount": jl.net_amount,
                    "gross_amount": jl.gross_amount,
                    "tax_amount": jl.tax_amount,
                    "invoice_number": invoice_number,
                    "invoice_url": invoice_url,
                }
                new_data_dict[(tenant_id, journal_line_id)] = data

            except Exception as e:
                logger.error(
                    "Error processing staging record for tenant_id: %s, journal_line_id: %s: %s",
                    tenant_id, journal_line_id, e, exc_info=True
                )
                continue

        # 3) Fetch existing GL rows for the keys we have and partition into updates vs. creates.
        keys = list(new_data_dict.keys())
        tenant_ids = {tenant_id for tenant_id, _ in keys}
        journal_line_ids = {journal_line_id for _, journal_line_id in keys}

        existing_objs = XeroGeneralLedger.objects.filter(
            tenant_id__in=tenant_ids,
            journal_line_id__in=journal_line_ids
        )
        existing_map = {
            (obj.tenant_id, obj.journal_line_id): obj
            for obj in existing_objs
            if (obj.tenant_id, obj.journal_line_id) in new_data_dict
        }

        update_list = []
        create_list = []
        for key, data in new_data_dict.items():
            if key in existing_map:
                obj = existing_map[key]
                # Update each field on the existing object.
                for field, value in data.items():
                    setattr(obj, field, value)
                update_list.append(obj)
            else:
                create_list.append(XeroGeneralLedger(**data))

        # 4) Bulk create new records and bulk update existing ones in batches.
        batch_size = 10000
        created_count = BatchUtils.bulk_create_batches(XeroGeneralLedger, create_list, batch_size=batch_size)

        update_count = 0
        if update_list:
            update_fields = [
                "org", "tenant_name", "journal_id", "journal_number", "journal_date", "created_date", "journal_reference",
                "source_id", "source_type", "tracking_category_name", "tracking_category_option", "account_id",
                "account_code", "account_type", "account_name", "account_status", "account_tax_type", "account_class",
                "reporting_code", "reporting_code_name", "statement", "bank_account_type", "journal_line_description",
                "net_amount", "gross_amount", "tax_amount", "invoice_number", "invoice_url",
            ]
            for i in range(0, len(update_list), batch_size):
                batch = update_list[i:i + batch_size]
                XeroGeneralLedger.objects.bulk_update(batch, fields=update_fields, batch_size=batch_size)
                update_count += len(batch)
                close_old_connections()

        total_processed = created_count + update_count
        self.log_import_event(module_name="xero_general_ledger", fetched_records=total_processed)
        logger.info(
            "map_xero_general_ledger: Created %s rows, updated %s rows, total processed %s.",
            created_count, update_count, total_processed
        )

        
    def map_xero_general_ledger_2(self):
        """
        Upsert (update or create) Xero General Ledger entries in chunks.
        This version processes the staging data in chunks of 1000 records,
        building the desired state and then immediately saving each chunk.
        It mirrors the logic of the original method, but omits the
        'reporting_code' and 'reporting_code_name' fields since they are not
        defined on the XeroGeneralLedger model.
        """
        from django.db import transaction, close_old_connections
        import logging
        logger = logging.getLogger(__name__)

        # 1) Identify the newest row per (tenant_id, journal_line_id) from the staging table.
        all_lines = XeroJournalLines.objects.order_by('-journal_date')
        latest_by_line = {}
        for line in all_lines:
            key = (line.tenant_id, line.journal_line_id)
            if key not in latest_by_line:
                latest_by_line[key] = line

        # Convert the dict items into a list for chunking.
        items = list(latest_by_line.items())
        total_records = len(items)
        logger.info("Total staging records to process: %s", total_records)

        # Define chunk size.
        chunk_size = 1000
        total_processed = 0

        # Process in chunks.
        for chunk_start in range(0, total_records, chunk_size):
            chunk = items[chunk_start:chunk_start + chunk_size]
            new_data_chunk = {}
            # Build desired data for each record in the current chunk.
            for (tenant_id, journal_line_id), jl in chunk:
                try:
                    # Retrieve tracking category (if exists).
                    try:
                        jtc = XeroJournalLineTrackingCategories.objects.get(
                            tenant_id=tenant_id,
                            journal_line_id=journal_line_id
                        )
                    except XeroJournalLineTrackingCategories.DoesNotExist:
                        jtc = None

                    # Retrieve account information (if available).
                    try:
                        acct = XeroAccountsRaw.objects.get(
                            tenant_id=tenant_id,
                            account_id=jl.account_id
                        )
                    except XeroAccountsRaw.DoesNotExist:
                        acct = None

                    contact_name = None
                    invoice_description_fallback = None
                    invoice_number = None
                    invoice_url = None

                    # Retrieve invoice metadata for ACCPAY / ACCREC types.
                    if jl.source_type in ["ACCPAY", "ACCREC"]:
                        try:
                            inv = XeroInvoicesRaw.objects.get(
                                tenant_id=tenant_id,
                                invoice_id=jl.source_id
                            )
                            inv_payload = inv.raw_payload or {}
                            contact_name = inv_payload.get("Contact", {}).get("Name")
                            invoice_description_fallback = inv_payload.get("Description")
                            invoice_number = inv_payload.get("Reference")
                            invoice_url = inv_payload.get("Url")
                        except XeroInvoicesRaw.DoesNotExist:
                            pass
                    else:
                        try:
                            bt = XeroBankTransactionsRaw.objects.get(
                                tenant_id=tenant_id,
                                bank_transaction_id=jl.source_id
                            )
                            bt_payload = bt.raw_payload or {}
                            contact_name = bt_payload.get("Contact", {}).get("Name")
                        except XeroBankTransactionsRaw.DoesNotExist:
                            pass

                    # Build a candidate description.
                    if contact_name or jl.description:
                        base = f"{contact_name} - " if contact_name else ""
                        desc_candidate = base + jl.description if jl.description else invoice_description_fallback
                    else:
                        desc_candidate = invoice_description_fallback

                    # Finalize the journal reference.
                    ref = jl.reference
                    if ref == desc_candidate:
                        final_journal_reference = ref
                    elif ref is not None and desc_candidate is not None:
                        final_journal_reference = f"{ref} - {desc_candidate}"
                    elif ref is None and desc_candidate is not None:
                        final_journal_reference = desc_candidate
                    else:
                        final_journal_reference = ref

                    invoice_description = desc_candidate if desc_candidate else invoice_description_fallback

                    if acct:
                        account_reporting_code = acct.raw_payload.get("ReportingCode") if acct.raw_payload else None
                        account_reporting_code_name = acct.raw_payload.get("ReportingCodeName") if acct.raw_payload else None
                        account_code = acct.raw_payload.get("Code") if acct.raw_payload else None
                        account_type = acct.raw_payload.get("Type") if acct.raw_payload else None
                        account_name = acct.raw_payload.get("Name") if acct.raw_payload else None
                        account_status = acct.status
                        account_tax_type = (acct.raw_payload or {}).get("TaxType")
                        account_class = (acct.raw_payload or {}).get("Class")
                        
                        acct_class_value = (acct.raw_payload or {}).get("Class", "").upper()
                        statement_val = "PL" if acct_class_value in ["REVENUE", "EXPENSE"] else "BS"
                    else:
                        account_code = jl.account_code
                        account_type = jl.account_type
                        account_name = jl.account_name
                        account_status = None
                        account_tax_type = None
                        account_class = None
                        statement_val = None
                        account_reporting_code = None
                        account_reporting_code_name = None

                    # Retrieve the organization from Integration using tenant_id.
                    integration = Integration.objects.get(id=tenant_id)
                    org = integration.org

                    # Prepare the data dict for the GL record (reporting fields omitted).
                    data = {
                        "org": org,
                        "tenant_id": tenant_id,
                        "tenant_name": org.name,
                        "journal_id": jl.journal_id,
                        "journal_number": int(jl.journal_number) if jl.journal_number else None,
                        "journal_date": jl.journal_date,
                        "created_date": jl.created_date_utc,
                        "journal_line_id": journal_line_id,
                        "journal_reference": final_journal_reference,
                        "source_id": jl.source_id,
                        "source_type": jl.source_type,
                        "tracking_category_name": jtc.name if jtc else None,
                        "tracking_category_option": jtc.option if jtc else None,
                        "account_id": jl.account_id,
                        "account_code": account_code or jl.account_code,
                        "account_type": account_type or jl.account_type,
                        "account_name": account_name or jl.account_name,
                        "account_status": account_status,
                        "account_reporting_code": account_reporting_code,
                        "account_reporting_code_name": account_reporting_code_name,
                        "account_tax_type": account_tax_type,
                        "account_class": account_class,
                        "statement": statement_val,
                        "bank_account_type": None,
                        "journal_line_description": jl.description,
                        "net_amount": jl.net_amount,
                        "gross_amount": jl.gross_amount,
                        "tax_amount": jl.tax_amount,
                        "invoice_number": invoice_number,
                        "invoice_url": invoice_url,
                    }
                    new_data_chunk[(tenant_id, journal_line_id)] = data

                except Exception as e:
                    logger.error(
                        "Error processing staging record for tenant_id: %s, journal_line_id: %s: %s",
                        tenant_id, journal_line_id, e, exc_info=True
                    )
                    continue

            # 3) Fetch existing GL rows for the keys in this chunk and partition into updates vs. creates.
            current_keys = list(new_data_chunk.keys())
            tenant_ids = {tenant_id for tenant_id, _ in current_keys}
            journal_line_ids = {journal_line_id for _, journal_line_id in current_keys}
            existing_objs = XeroGeneralLedger.objects.filter(
                tenant_id__in=tenant_ids,
                journal_line_id__in=journal_line_ids
            )
            existing_map = {
                (obj.tenant_id, obj.journal_line_id): obj
                for obj in existing_objs
                if (obj.tenant_id, obj.journal_line_id) in new_data_chunk
            }

            update_list = []
            create_list = []
            for key, data in new_data_chunk.items():
                if key in existing_map:
                    obj = existing_map[key]
                    for field, value in data.items():
                        setattr(obj, field, value)
                    update_list.append(obj)
                else:
                    create_list.append(XeroGeneralLedger(**data))

            # 4) Bulk save this chunk.
            with transaction.atomic():
                if create_list:
                    XeroGeneralLedger.objects.bulk_create(create_list, batch_size=len(create_list))
                if update_list:
                    update_fields = [
                        "org", "tenant_name", "journal_id", "journal_number", "journal_date", "created_date", "journal_reference",
                        "source_id", "source_type", "tracking_category_name", "tracking_category_option", "account_id",
                        "account_code", "account_type", "account_name", "account_status", "account_tax_type", "account_class",
                        "statement", "bank_account_type", "journal_line_description", "net_amount", "gross_amount",
                        "tax_amount", "invoice_number", "invoice_url",
                    ]
                    XeroGeneralLedger.objects.bulk_update(update_list, fields=update_fields, batch_size=len(update_list))
            total_processed += len(new_data_chunk)
            close_old_connections()
            self.log_import_event(module_name="xero_general_ledger_2", fetched_records=total_processed)
            logger.info("map_xero_general_ledger_2: Processed and saved %s records in this chunk, total processed %s.", len(new_data_chunk), total_processed)

        logger.info("map_xero_general_ledger_2: Completed processing of %s records.", total_processed)


    @transaction.atomic
    def map_xero_general_ledger_1(self):
        """
        Recreate Xero General Ledger from the staging tables.
        """
        # 1) Delete existing GL rows
        XeroGeneralLedger.objects.all().delete()

        # 2) Identify the newest row per (tenant_id, journal_line_id).
        all_lines = (
            XeroJournalLines.objects
            .order_by('-journal_date')
        )
        latest_by_line = {}
        for line in all_lines:
            key = (line.tenant_id, line.journal_line_id)
            if key not in latest_by_line:
                latest_by_line[key] = line

        # 3) Build final GL rows
        gl_objects = []

        for (tenant_id, journal_line_id), jl in latest_by_line.items():
            try:
                jtc = XeroJournalLineTrackingCategories.objects.get(
                    tenant_id=tenant_id,
                    journal_line_id=journal_line_id
                )
            except XeroJournalLineTrackingCategories.DoesNotExist:
                jtc = None

            try:
                acct = XeroAccountsRaw.objects.get(
                    tenant_id=tenant_id,
                    account_id=jl.account_id
                )
            except XeroAccountsRaw.DoesNotExist:
                acct = None

            contact_name = None
            invoice_description_fallback = None
            # Retrieve invoice metadata for ACCPAY / ACCREC types.
            invoice_number = None
            invoice_url = None
            if jl.source_type in ["ACCPAY", "ACCREC"]:
                try:
                    inv = XeroInvoicesRaw.objects.get(
                        tenant_id=tenant_id,
                        invoice_id=jl.source_id
                    )
                    inv_payload = inv.raw_payload or {}
                    contact_name = inv_payload.get("Contact", {}).get("Name")
                    invoice_description_fallback = inv_payload.get("Description")
                    invoice_number = inv_payload.get("Reference")
                    invoice_url = inv_payload.get("Url")
                except XeroInvoicesRaw.DoesNotExist:
                    pass
            else:
                try:
                    bt = XeroBankTransactionsRaw.objects.get(
                        tenant_id=tenant_id,
                        bank_transaction_id=jl.source_id
                    )
                    bt_payload = bt.raw_payload or {}
                    contact_name = bt_payload.get("Contact", {}).get("Name")
                except XeroBankTransactionsRaw.DoesNotExist:
                    pass

            desc_candidate = None
            if contact_name or jl.description:
                base = (contact_name + " - ") if contact_name else ""
                if jl.description:
                    desc_candidate = base + jl.description
                else:
                    desc_candidate = invoice_description_fallback
            else:
                desc_candidate = invoice_description_fallback

            ref = jl.reference
            if ref == desc_candidate:
                final_journal_reference = ref
            elif ref is not None and desc_candidate is not None:
                final_journal_reference = f"{ref} - {desc_candidate}"
            elif ref is None and desc_candidate is not None:
                final_journal_reference = desc_candidate
            else:
                final_journal_reference = ref

            invoice_description = desc_candidate if desc_candidate else invoice_description_fallback

            if acct:
                account_code = acct.raw_payload.get("Code") if acct.raw_payload else None
                account_type = acct.raw_payload.get("Type") if acct.raw_payload else None
                account_name = acct.raw_payload.get("Name") if acct.raw_payload else None
                account_status = acct.status
                account_tax_type = (acct.raw_payload or {}).get("TaxType")
                account_class = (acct.raw_payload or {}).get("Class")
                acct_class_value = (acct.raw_payload or {}).get("Class", "").upper()
                statement_val = "PL" if acct_class_value in ["REVENUE", "EXPENSE"] else "BS"
            else:
                account_code = jl.account_code
                account_type = jl.account_type
                account_name = jl.account_name
                account_status = None
                account_tax_type = None
                account_class = None
                statement_val = None

            gl_obj = XeroGeneralLedger(
                org=self.integration.org,
                tenant_id=tenant_id,
                tenant_name=self.integration.org.name,
                journal_id=jl.journal_id,
                journal_number=(int(jl.journal_number) if jl.journal_number else None),
                journal_date=jl.journal_date,
                created_date=jl.created_date_utc,
                journal_line_id=journal_line_id,
                journal_reference=final_journal_reference,
                source_id=jl.source_id,
                source_type=jl.source_type,
                tracking_category_name=(jtc.name if jtc else None),
                tracking_category_option=(jtc.option if jtc else None),
                account_id=jl.account_id,
                account_code=account_code or jl.account_code,
                account_type=account_type or jl.account_type,
                account_name=account_name or jl.account_name,
                account_status=account_status,
                account_tax_type=account_tax_type,
                account_class=account_class,
                statement=statement_val,
                bank_account_type=None,
                journal_line_description=jl.description,
                net_amount=jl.net_amount,
                gross_amount=jl.gross_amount,
                tax_amount=jl.tax_amount,
                invoice_number=invoice_number,
                invoice_url=invoice_url
            )
            gl_objects.append(gl_obj)

        # Save in batches of 1000 for smooth operations
        XeroGeneralLedger1.objects.bulk_create(gl_objects, batch_size=1000)
        self.log_import_event(module_name="xero_general_ledger", fetched_records=len(gl_objects))
        logger.info(f"map_xero_general_ledger: Inserted {len(gl_objects)} rows (latest lines only).")


    def map_xero_general_ledger_3(self):
        """
        Upsert (update or create) Xero General Ledger entries for a single tenant 
        (self.integration.org.id) in chunks. This version processes the staging data 
        in chunks of 1000 records, builds the desired state, and immediately saves each 
        chunk. It includes all fields from the original method.
        """
        from django.db import transaction, close_old_connections
        import logging
        logger = logging.getLogger(__name__)

        # Use the specific tenant_id from the integration.
        tenant_id = self.integration.org.id

        # 1) Identify the newest row per journal_line_id for this tenant.
        all_lines = XeroJournalLines.objects.filter(tenant_id=tenant_id).order_by('-journal_date')
        latest_by_line = {}
        for line in all_lines:
            # Use journal_line_id as the unique key (tenant is fixed).
            key = line.journal_line_id
            if key not in latest_by_line:
                latest_by_line[key] = line

        # Convert the dict items into a list for chunking.
        items = list(latest_by_line.items())
        total_records = len(items)
        logger.info("Total staging records to process for tenant %s: %s", tenant_id, total_records)

        # Define chunk size.
        chunk_size = 10000
        total_processed = 0

        # Process in chunks.
        for chunk_start in range(0, total_records, chunk_size):
            chunk = items[chunk_start:chunk_start + chunk_size]
            new_data_chunk = {}
            # Build desired data for each record in this chunk.
            for journal_line_id, jl in chunk:
                try:
                    # Retrieve tracking category (if exists) using filter() to avoid MultipleObjectsReturned.
                    qs = XeroJournalLineTrackingCategories.objects.filter(
                        tenant_id=tenant_id,
                        journal_line_id=journal_line_id
                    ).order_by('ingestion_timestamp')
                    if qs.exists():
                        jtc = qs.first()
                        if qs.count() > 1:
                            logger.warning(
                                "Multiple tracking categories found for tenant %s and journal_line_id %s; using the first.",
                                tenant_id, journal_line_id
                            )
                    else:
                        jtc = None

                    # Retrieve account information (if available).
                    try:
                        acct = XeroAccountsRaw.objects.get(
                            tenant_id=tenant_id,
                            account_id=jl.account_id
                        )
                    except XeroAccountsRaw.DoesNotExist:
                        acct = None

                    contact_name = None
                    invoice_description_fallback = None
                    invoice_number = None
                    invoice_url = None

                    # Retrieve invoice metadata for ACCPAY / ACCREC types.
                    if jl.source_type in ["ACCPAY", "ACCREC"]:
                        try:
                            inv = XeroInvoicesRaw.objects.get(
                                tenant_id=tenant_id,
                                invoice_id=jl.source_id
                            )
                            inv_payload = inv.raw_payload or {}
                            contact_name = inv_payload.get("Contact", {}).get("Name")
                            invoice_description_fallback = inv_payload.get("Description")
                            invoice_number = inv_payload.get("Reference")
                            invoice_url = inv_payload.get("Url")
                        except XeroInvoicesRaw.DoesNotExist:
                            pass
                    else:
                        try:
                            bt = XeroBankTransactionsRaw.objects.get(
                                tenant_id=tenant_id,
                                bank_transaction_id=jl.source_id
                            )
                            bt_payload = bt.raw_payload or {}
                            contact_name = bt_payload.get("Contact", {}).get("Name")
                        except XeroBankTransactionsRaw.DoesNotExist:
                            pass

                    # Build a candidate description.
                    if contact_name or jl.description:
                        base = f"{contact_name} - " if contact_name else ""
                        desc_candidate = base + jl.description if jl.description else invoice_description_fallback
                    else:
                        desc_candidate = invoice_description_fallback

                    # Finalize the journal reference.
                    ref = jl.reference
                    if ref == desc_candidate:
                        final_journal_reference = ref
                    elif ref is not None and desc_candidate is not None:
                        final_journal_reference = f"{ref} - {desc_candidate}"
                    elif ref is None and desc_candidate is not None:
                        final_journal_reference = desc_candidate
                    else:
                        final_journal_reference = ref

                    invoice_description = desc_candidate if desc_candidate else invoice_description_fallback

                    if acct:
                        account_reporting_code = acct.raw_payload.get("ReportingCode") if acct.raw_payload else None
                        account_reporting_code_name = acct.raw_payload.get("ReportingCodeName") if acct.raw_payload else None
                        account_code = acct.raw_payload.get("Code") if acct.raw_payload else None
                        account_type = acct.raw_payload.get("Type") if acct.raw_payload else None
                        account_name = acct.raw_payload.get("Name") if acct.raw_payload else None
                        account_status = acct.status
                        account_tax_type = (acct.raw_payload or {}).get("TaxType")
                        account_class = (acct.raw_payload or {}).get("Class")
                        acct_class_value = (acct.raw_payload or {}).get("Class", "").upper()
                        statement_val = "PL" if acct_class_value in ["REVENUE", "EXPENSE"] else "BS"
                    else:
                        account_code = jl.account_code
                        account_type = jl.account_type
                        account_name = jl.account_name
                        account_status = None
                        account_tax_type = None
                        account_class = None
                        statement_val = None
                        account_reporting_code = None
                        account_reporting_code_name = None

                    # Use the current integration.
                    org = self.integration.org

                    # Prepare the data dict for the GL record.
                    data = {
                        "org": org,
                        "tenant_id": tenant_id,
                        "tenant_name": org.name,
                        "journal_id": jl.journal_id,
                        "journal_number": int(jl.journal_number) if jl.journal_number else None,
                        "journal_date": jl.journal_date,
                        "created_date": jl.created_date_utc,
                        "journal_line_id": journal_line_id,
                        "journal_reference": final_journal_reference,
                        "source_id": jl.source_id,
                        "source_type": jl.source_type,
                        "tracking_category_name": jtc.name if jtc else None,
                        "tracking_category_option": jtc.option if jtc else None,
                        "account_id": jl.account_id,
                        "account_code": account_code or jl.account_code,
                        "account_type": account_type or jl.account_type,
                        "account_name": account_name or jl.account_name,
                        "account_status": account_status,
                        "account_reporting_code": account_reporting_code,
                        "account_reporting_code_name": account_reporting_code_name,
                        "account_tax_type": account_tax_type,
                        "account_class": account_class,
                        "statement": statement_val,
                        "bank_account_type": None,
                        "journal_line_description": jl.description,
                        "net_amount": jl.net_amount,
                        "gross_amount": jl.gross_amount,
                        "tax_amount": jl.tax_amount,
                        "invoice_number": invoice_number,
                        "invoice_url": invoice_url,
                    }
                    new_data_chunk[journal_line_id] = data

                except Exception as e:
                    logger.error(
                        "Error processing staging record for tenant_id: %s, journal_line_id: %s: %s",
                        tenant_id, journal_line_id, e, exc_info=True
                    )
                    continue

            # 3) Fetch existing GL rows for the keys in this chunk and partition into updates vs. creates.
            current_keys = list(new_data_chunk.keys())
            existing_objs = XeroGeneralLedger3.objects.filter(
                tenant_id=tenant_id,
                journal_line_id__in=current_keys
            )
            existing_map = {obj.journal_line_id: obj for obj in existing_objs if obj.journal_line_id in new_data_chunk}

            update_list = []
            create_list = []
            for key, data in new_data_chunk.items():
                if key in existing_map:
                    obj = existing_map[key]
                    for field, value in data.items():
                        setattr(obj, field, value)
                    update_list.append(obj)
                else:
                    create_list.append(XeroGeneralLedger3(**data))

            # 4) Bulk save this chunk.
            with transaction.atomic():
                if create_list:
                    XeroGeneralLedger3.objects.bulk_create(create_list, batch_size=len(create_list))
                if update_list:
                    update_fields = [
                        "org", "tenant_name", "journal_id", "journal_number", "journal_date", "created_date", "journal_reference",
                        "source_id", "source_type", "tracking_category_name", "tracking_category_option", "account_id",
                        "account_code", "account_type", "account_name", "account_status", "account_tax_type", "account_class",
                        "account_reporting_code", "account_reporting_code_name", "statement", "bank_account_type", "journal_line_description",
                        "net_amount", "gross_amount", "tax_amount", "invoice_number", "invoice_url",
                    ]
                    XeroGeneralLedger3.objects.bulk_update(update_list, fields=update_fields, batch_size=len(update_list))
            total_processed += len(new_data_chunk)
            close_old_connections()
            self.log_import_event(module_name="xero_general_ledger_3", fetched_records=total_processed)
            logger.info("map_xero_general_ledger_3: Processed and saved %s records in this chunk, total processed %s.", len(new_data_chunk), total_processed)

        logger.info("map_xero_general_ledger_3: Completed processing of %s records.", total_processed)


    @transaction.atomic
    def import_xero_data(self):
        """
        Master function to import all Xero data we care about.
        """
        # 1. Accounts
        print("Importing Xero Chart of Accounts...")
        self.sync_xero_chart_of_accounts()

        # 2. Journal Lines (with pagination)
        print("Importing Xero Journal Lines...")
        self.import_xero_journal_lines()

        # 3. Contacts
        print("Importing Xero Contacts...")
        self.import_xero_contacts()

        # 4. Invoices
        print("Importing Xero Invoices...")
        self.import_xero_invoices()

        # 5. Bank Transactions
        print("Importing Xero Bank Transactions...")
        self.import_xero_bank_transactions()

        # 6. Budgets
        print("Importing Xero Budgets...")
        self.import_xero_budgets()

        # # 7. General Ledger
        # print("Mapping Xero General Ledger...")
        # self.map_xero_general_ledger()

        logger.info("Finished full Xero data import successfully.")
        

