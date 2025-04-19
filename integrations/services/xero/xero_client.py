import requests
import re
import logging
import time  # Used for sleep on retry
from datetime import timedelta, datetime, date
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

    def __init__(self, integration: Integration, since_date=None, until_date=None):
        self.integration = integration
        self.since_date = since_date
        self.until_date = until_date
        self.client_id = integration.xero_client_id
        self.client_secret = integration.xero_client_secret
        self.tenant_id = str(integration.org.id)
        
        if since_date is None:
            self.since_date = timezone.now().date()
        if until_date is None:
            self.until_date = timezone.now().date()
        
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
        
        # If since_date is provided, convert it to a datetime object if needed.
        if self.since_date:
            date_obj = None
            if isinstance(self.since_date, str):
                try:
                    # Assuming the string is in the format "YYYY-MM-DD"
                    date_obj = datetime.strptime(self.since_date, "%Y-%m-%d")
                except Exception as e:
                    logger.error("Error parsing self.since_date '%s': %s", self.since_date, e, exc_info=True)
            else:
                date_obj = self.since_date
            
            if date_obj:
                headers["If-Modified-Since"] = date_obj.strftime("%a, %d %b %Y %H:%M:%S GMT")
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


    def get_budget_period_balances(self, budget_id: str, until_date=None):
        url = f"https://api.xero.com/api.xro/2.0/Budgets/{budget_id}"
        headers = {
            "Authorization": f"Bearer {self.get_valid_xero_token()}",
            "Accept": "application/json"
        }
        try:
            # Convert since_date to string format if it's a date object
            date_from = self.since_date
            if isinstance(date_from, (datetime, date)):
                date_from = date_from.strftime("%Y-%m-%d")
            
            # Use until_date parameter or fallback to today's date
            date_to = until_date
            if isinstance(date_to, (datetime, date)):
                date_to = date_to.strftime("%Y-%m-%d")
            elif date_to is None:
                date_to = timezone.now().date().strftime("%Y-%m-%d")
            
            response = request_with_retry("get", url, headers=headers, params={
                "DateFrom": date_from,
                "DateTo": date_to
            })
            return response.json().get("Budgets", [])

        except requests.exceptions.HTTPError as e:
            pass


    def import_xero_budgets(self, until_date=None):
        until_date = self.until_date
        logger.info(f"Importing Xero Budgets & Period Balances from {self.since_date} to {until_date}...")
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
            bp_response = self.get_budget_period_balances(budget_id, until_date)
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

        logger.info("Finished full Xero data import successfully.")
        

