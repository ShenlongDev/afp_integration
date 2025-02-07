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
    XeroBudgetPeriodBalancesRaw,
    XeroConnectionsRaw
)
from integrations.models.xero.transformations import (
    XeroJournalLines,
    XeroInvoiceLineItems,
    XeroJournalLineTrackingCategories
)
from integrations.models.xero.analytics import XeroGeneralLedger


logger = logging.getLogger(__name__)


class XeroDataImporter:
    """
    A refactored class-based approach to Xero data importing.
    Shared values (integration, since_date, etc.) are stored in __init__.
    The existing business logic is preserved in the instance methods.
    """

    def __init__(self, integration: Integration, since_date=None):
        """
        Store commonly used attributes, such as:
          - Integration object
          - since_date for If-Modified-Since headers
          - references to client_id, client_secret, and tenant_id
        """
        self.integration = integration
        self.since_date = since_date
        self.client_id = integration.xero_client_id
        self.client_secret = integration.xero_client_secret
        self.tenant_id = integration.xero_tenant_id
        
    
    def request_new_xero_token(self):
        """
        Request a new Xero access token using client_credentials and store it.
        """
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

        response = requests.post(token_url, data=data, auth=auth)
        response.raise_for_status()

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

    ### 1. Token & Utility Helpers ###

    def get_valid_xero_token(self) -> str:
        """
        Retrieve a valid Xero access token for self.integration.
        If expired or not found, request a new one.
        """
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

        # No valid token found; request a new one
        return self.request_new_xero_token()


    def parse_xero_datetime(self, xero_date_str: str):
        """
        Parse a Xero date string, e.g. '/Date(1672533421427+0000)/'
        or an ISO8601 '2023-10-12T00:00:00' into a Python datetime.
        """
        if not xero_date_str:
            return None

        match = re.match(r'/Date\((\d+)([+-]\d{4})\)/', xero_date_str)
        if match:
            timestamp_ms, offset_str = match.groups()
            timestamp = int(timestamp_ms) / 1000.0
            dt = datetime.fromtimestamp(timestamp, tz=ZoneInfo("UTC"))
            return dt

        # else assume it's ISO format
        try:
            return datetime.fromisoformat(xero_date_str.replace("Z", "+00:00"))
        except ValueError:
            logger.warning(f"Unknown date format: {xero_date_str}")
            return None

    def build_headers(self) -> dict:
        """
        Helper to build the Xero request headers with valid token,
        plus optional If-Modified-Since if needed.
        """
        token = self.get_valid_xero_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "xero-tenant-id": self.tenant_id,
            "Accept": "application/json"
        }
        if self.since_date:
            headers["If-Modified-Since"] = self.since_date.strftime("%a, %d %b %Y %H:%M:%S GMT")
        return headers

    ### 2. Chart of Accounts ###
    @transaction.atomic
    def sync_xero_chart_of_accounts(self):
        """
        Fetch chart of accounts from Xero and store them in XeroAccountsRaw.
        """
        logger.info("Syncing Xero Chart of Accounts...")
        headers = self.build_headers()
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
        logger.info(f"Imported/Updated {len(accounts_data)} Xero Accounts.")

    ### 3. Journal Lines (and transform into General Ledger) ###
    @transaction.atomic
    def import_xero_journal_lines(self):
        """
        Import Xero Journals + lines into XeroJournalsRaw, XeroJournalLines.
        """
        logger.info("Importing Xero Journals & Lines...")
        headers = self.build_headers()
        url = "https://api.xero.com/api.xro/2.0/Journals"
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        journals_data = response.json().get("Journals", [])

        now_ts = timezone.now()
        # print(journals_data[:100], "journals_data")
        for journal in journals_data:
            journal_id = journal.get("JournalID")
            if not journal_id:
                logger.warning("Skipping journal with no JournalID.")
                continue

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

            lines = journal.get("JournalLines", [])
            print(lines[:100], "lines")
            for line in lines:
                line_id = line.get("JournalLineID")
                if not line_id:
                    logger.warning(f"Skipping line in Journal {journal_id} with no JournalLineID.")
                    continue

                tcat = line.get("TrackingCategories", [])
                tracking_name = None
                tracking_option = None
                if tcat:
                    tracking_name = tcat[0].get("Name")
                    tracking_option = tcat[0].get("Option")

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
                print(jline_defaults, "jline_defaults")

        logger.info("Completed Xero Journal import & transform.")

    ### 4. Contacts ###
    def get_contacts(self):
        """
        Generator that fetches Xero Contacts with optional If-Modified-Since.
        Single fetch for simplicity.
        """
        url = "https://api.xero.com/api.xro/2.0/Contacts"
        headers = {
            "Authorization": f"Bearer {self.get_valid_xero_token()}",
            "xero-tenant-id": self.tenant_id,
            "Accept": "application/json"
        }
        if self.since_date:
            headers["If-Modified-Since"] = self.since_date.strftime("%a, %d %b %Y %H:%M:%S GMT")

        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json().get("Contacts", [])
        for contact in data:
            yield contact

    @transaction.atomic
    def import_xero_contacts(self):
        """
        Import Xero contacts into XeroContactsRaw.
        """
        logger.info("Importing Xero Contacts...")
        now_ts = timezone.now()

        for contact in self.get_contacts():
            contact_id = contact.get("ContactID")
            if not contact_id:
                logger.warning("Skipping contact with no ContactID.")
                continue
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
        logger.info("Completed Xero Contacts import.")

    ### 5. Invoices ###
    def get_invoices(self):
        """
        Example: fetch Xero invoices. Single fetch for simplicity.
        """
        url = "https://api.xero.com/api.xro/2.0/Invoices"
        headers = {
            "Authorization": f"Bearer {self.get_valid_xero_token()}",
            "xero-tenant-id": self.tenant_id,
            "Accept": "application/json"
        }
        if self.since_date:
            headers["If-Modified-Since"] = self.since_date.strftime("%a, %d %b %Y %H:%M:%S GMT")

        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json().get("Invoices", [])
        return data

    @transaction.atomic
    def import_xero_invoices(self):
        """
        Import Xero invoices + line items into XeroInvoicesRaw.
        """
        logger.info("Importing Xero Invoices...")
        invoices = self.get_invoices()
        now_ts = timezone.now()

        for inv in invoices:
            invoice_id = inv.get("InvoiceID")
            if not invoice_id:
                logger.warning("Skipping invoice with no InvoiceID.")
                continue

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
                        'tenant_id': self.integration.org.id,
                        'description': line.get('Description'),
                        'quantity': line.get('Quantity'),
                        'unit_amount': line.get('UnitAmount'),
                        'account_code': line.get('AccountCode'),
                    }
                )
        logger.info("Completed Xero Invoices import.")

    ### 6. Bank Transactions ###
    def get_bank_transactions(self):
        """
        Fetch Xero bank transactions using pagination.
        The page size is set; we accumulate until final page.
        """
        url = "https://api.xero.com/api.xro/2.0/BankTransactions"
        headers = {
            "Authorization": f"Bearer {self.get_valid_xero_token()}",
            "xero-tenant-id": self.tenant_id,
            "Accept": "application/json"
        }
        if self.since_date:
            headers["If-Modified-Since"] = self.since_date.strftime("%a, %d %b %Y %H:%M:%S GMT")

        results = []
        page = 1
        page_size = 1000000  # default page size

        while True:
            params = {"page": page}
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            bank_transactions = response.json().get("BankTransactions", [])
            results.extend(bank_transactions)

            if len(bank_transactions) < page_size:
                break
            page += 1

        return results

    @transaction.atomic
    def import_xero_bank_transactions(self):
        logger.info("Importing Xero Bank Transactions...")
        now_ts = timezone.now()

        transactions = self.get_bank_transactions()
        print(transactions)
        for bt in transactions:
            bt_id = bt.get("BankTransactionID")
            if not bt_id:
                logger.warning("Skipping bank transaction with no BankTransactionID.")
                continue
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
                for tracking in line.get('Tracking', []):
                    XeroJournalLineTrackingCategories.objects.update_or_create(
                        line_item_id=line['LineItemID'],
                        tenant_id=self.integration.org.id,
                        tracking_category_id=tracking.get('TrackingCategoryID'),
                        defaults={
                            'name': tracking.get('Name'),
                            'option': tracking.get('Option'),
                        }
                    )

        logger.info("Completed Xero Bank Transactions import.")

    ### 7. Budgets + Budget Period Balances ###
    def get_budgets(self):
        """
        Example function: Xero has a 'GET /Budgets' endpoint?
        This is partially hypothetical.
        """
        url = "https://api.xero.com/api.xro/2.0/Budgets"
        headers = {
            "Authorization": f"Bearer {self.get_valid_xero_token()}",
            "xero-tenant-id": self.tenant_id,
            "Accept": "application/json"
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json().get("Budgets", [])

    def get_budget_period_balances(self, budget_id: str):
        """
        Hypothetical function: e.g. GET /Budgets/{budget_id}/periodBalances
        Not an actual standard Xero endpoint in many cases.
        """
        url = f"https://api.xero.com/api.xro/2.0/Budgets/{budget_id}/periodBalances"
        headers = {
            "Authorization": f"Bearer {self.get_valid_xero_token()}",
            "xero-tenant-id": self.tenant_id,
            "Accept": "application/json"
        }
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            return response.json().get("PeriodBalances", [])
        except requests.exceptions.HTTPError as e:
            if response.status_code == 404:
                return []
            else:
                raise

    @transaction.atomic
    def import_xero_budgets(self):
        """
        Import budgets and period balances from Xero.
        """
        logger.info("Importing Xero Budgets & Period Balances...")
        now_ts = timezone.now()

        budgets = self.get_budgets()
        for budget in budgets:
            budget_id = budget.get("BudgetID")
            if not budget_id:
                logger.warning("Skipping budget with no BudgetID.")
                continue

            XeroBudgetsRaw.objects.update_or_create(
                budget_id=budget_id,
                tenant_id=self.integration.org.id,
                defaults={
                    "status": budget.get("Status"),
                    "type": budget.get("Type"),
                    "description": budget.get("Description"),
                    "updated_date_utc": self.parse_xero_datetime(budget.get("UpdatedDateUTC")),
                    "raw_payload": budget,
                    "ingestion_timestamp": now_ts,
                    "source_system": "XERO"
                }
            )

            period_balances = self.get_budget_period_balances(budget_id)
            for pb in period_balances:
                account_id = pb.get("AccountID")
                period = pb.get("Period")
                XeroBudgetPeriodBalancesRaw.objects.update_or_create(
                    budget_id=budget_id,
                    tenant_id=self.integration.org.id,
                    account_id=account_id,
                    period=period,
                    defaults={
                        "account_code": pb.get("AccountCode"),
                        "amount": pb.get("Amount"),
                        "notes": pb.get("Notes"),
                        "updated_date_utc": self.parse_xero_datetime(pb.get("UpdatedDateUTC")),
                        "ingestion_timestamp": now_ts,
                        "source_system": "XERO"
                    }
                )

        logger.info("Completed Xero Budgets & Period Balances import.")

    @transaction.atomic
    def map_xero_general_ledger(self):
        """
        Recreate Xero General Ledger from the staging tables, 
        mimicking the SQL logic that deletes old rows and inserts 
        fresh data, keeping only the newest ingestion_timestamp per line.

        1) DELETE existing XeroGeneralLedger entries.
        2) Identify the "latest" XeroJournalLines row per (tenant_id, journal_line_id).
        3) For each such line, left-join to:
        - XeroConnectionsRaw for tenant_name
        - XeroJournalLineTrackingCategories for tracking_category
        - XeroAccountsRaw for account details
        - XeroInvoicesRaw or XeroBankTransactionsRaw for contact_name
        4) Build final fields (journal_reference, invoice_description, etc.) 
        using logic that merges reference + contact_name + description.
        5) Insert into XeroGeneralLedger, applying the unique constraint 
        (tenant_id, journal_id, journal_line_id).

        After running, XeroGeneralLedger is fully rebuilt 
        with only the newest lines per ID.
        """
        # 1) Delete existing GL rows
        XeroGeneralLedger.objects.all().delete()

        # 2) Identify the newest row per (tenant_id, journal_line_id).
        #    One approach in Python is:
        #    - gather all lines ordered by ingestion_timestamp desc
        #    - keep first occurrence of (tenant_id, journal_line_id).
        all_lines = (
            XeroJournalLines.objects
            .order_by('-ingestion_timestamp')  # newest first
        )
        latest_by_line = {}
        for line in all_lines:
            key = (line.tenant_id, line.journal_line_id)
            if key not in latest_by_line:
                latest_by_line[key] = line
        

        # 3) Build final GL rows
        gl_objects = []  # We'll collect XeroGeneralLedger objects for insertion.

        for (tenant_id, journal_line_id), jl in latest_by_line.items():

            # b) Lookup tracking category if it exists
            try:
                jtc = XeroJournalLineTrackingCategories.objects.get(
                    tenant_id=tenant_id,
                    journal_line_id=journal_line_id
                )
            except XeroJournalLineTrackingCategories.DoesNotExist:
                jtc = None

            # c) Lookup account
            try:
                acct = XeroAccountsRaw.objects.get(
                    tenant_id=tenant_id, 
                    account_id=jl.account_id
                )
            except XeroAccountsRaw.DoesNotExist:
                acct = None

            # d) Look for invoice or bank to get contact_name & possible description
            contact_name = None
            invoice_description_fallback = None  # We'll combine with line.description
            if jl.source_type in ["ACCPAY", "ACCREC"]:
                # invoice
                try:
                    inv = XeroInvoicesRaw.objects.get(
                        tenant_id=tenant_id,
                        invoice_id=jl.source_id
                    )
                    # For example, maybe store contact name in raw_payload
                    # or perhaps you have separate XeroContactsRaw
                    inv_payload = inv.raw_payload or {}
                    contact_name = inv_payload.get("Contact", {}).get("Name")  # or however you store it
                    invoice_description_fallback = inv_payload.get("Description")  # example
                except XeroInvoicesRaw.DoesNotExist:
                    pass
            else:
                # bank or others
                try:
                    bt = XeroBankTransactionsRaw.objects.get(
                        tenant_id=tenant_id,
                        bank_transaction_id=jl.source_id
                    )
                    bt_payload = bt.raw_payload or {}
                    # Maybe contact is also stored in bt_payload
                    # e.g. "Contact": {"Name": "..."}
                    contact_name = bt_payload.get("Contact", {}).get("Name")
                    invoice_description_fallback = bt_payload.get("Description")
                except XeroBankTransactionsRaw.DoesNotExist:
                    pass

            # e) Build desc_candidate = IFNULL(contact_name + ' - ', '') + jl.description 
            #    or if jl.description is None => use invoice_description_fallback
            #    This mimics your "CASE WHEN" logic in SQL.
            desc_candidate = None
            if contact_name or jl.description:
                # "Base" = contact_name + " - " if contact_name else ""
                base = (contact_name + " - ") if contact_name else ""
                # If jl.description is not None, prefer that
                if jl.description:
                    desc_candidate = base + jl.description
                else:
                    # fallback if line description is None
                    desc_candidate = invoice_description_fallback
            else:
                # if everything is None, desc_candidate = invoice_description_fallback
                desc_candidate = invoice_description_fallback

            # Now replicate the final logic for journal_reference:
            # CASE
            #   WHEN jl.reference == desc_candidate => jl.reference
            #   WHEN both not None => jl.reference + ' - ' + desc_candidate
            #   WHEN jl.reference is None => desc_candidate
            #   ELSE jl.reference
            # END
            ref = jl.reference
            if ref == desc_candidate:
                final_journal_reference = ref
            elif ref is not None and desc_candidate is not None:
                final_journal_reference = f"{ref} - {desc_candidate}"
            elif ref is None and desc_candidate is not None:
                final_journal_reference = desc_candidate
            else:
                final_journal_reference = ref

            # Similarly, "invoice_description" in your SQL is basically desc_candidate, 
            # or if desc_candidate is None => i.description. We'll treat it similarly:
            invoice_description = desc_candidate if desc_candidate else invoice_description_fallback

            # f) Merge account fields
            if acct:
                account_code = acct.raw_payload.get("Code") if acct.raw_payload else None
                account_type = acct.raw_payload.get("Type") if acct.raw_payload else None
                account_name = acct.raw_payload.get("Name") if acct.raw_payload else None
                account_status = acct.status
                account_tax_type = (acct.raw_payload or {}).get("TaxType")
                account_class = (acct.raw_payload or {}).get("Class")
                # etc. ...
                # If the account's "Class" is 'REVENUE'/'EXPENSE' => statement='PL' else 'BS'
                acct_class_value = (acct.raw_payload or {}).get("Class", "").upper()
                statement_val = "PL" if acct_class_value in ["REVENUE", "EXPENSE"] else "BS"
            else:
                # fallback to line fields or None
                account_code = jl.account_code
                account_type = jl.account_type
                account_name = jl.account_name
                account_status = None
                account_tax_type = None
                account_class = None
                statement_val = None

            # g) Build the XeroGeneralLedger record 
            gl_obj = XeroGeneralLedger(
                org=self.integration.org,  # If you have an Organisation or link in your code
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
                # For clarity, you might define more fields from the account model
                statement=statement_val,
                bank_account_type=None,  # or from account, if stored
                journal_line_description=jl.description,
                net_amount=jl.net_amount,
                gross_amount=jl.gross_amount,
                tax_amount=jl.tax_amount,
                invoice_number=None,     # set if you want to store an invoice number
                invoice_url=None         # set if you want to store an invoice URL
            )
            # If you want that "invoice_description" etc. 
            # (You haven't shown those fields in your XeroGeneralLedger model, 
            #  so either add them or skip.)
            gl_objects.append(gl_obj)

        # 4) Bulk insert all GL rows
        XeroGeneralLedger.objects.bulk_create(gl_objects)

        logger.info(f"map_xero_general_ledger: Inserted {len(gl_objects)} rows (latest lines only).")

    ### 8. Master function to import everything ###
    @transaction.atomic
    def import_xero_data(self):
        """
        Master function to import all Xero data we care about:
        1) Chart of Accounts
        2) Journal lines
        3) Contacts
        4) Invoices
        5) Bank Transactions
        6) Budgets
        7) General Ledger
        """
        logger.info("Starting full Xero data import...")

        # # 1. Accounts
        # self.sync_xero_chart_of_accounts()

        # # 2. Journal Lines
        # self.import_xero_journal_lines()

        # # 3. Contacts
        # self.import_xero_contacts()

        # # 4. Invoices
        # self.import_xero_invoices()

        # # 5. Bank Transactions
        # self.import_xero_bank_transactions()

        # # 6. Budgets
        # self.import_xero_budgets()

        # 7. General Ledger
        self.map_xero_general_ledger()

        logger.info("Finished full Xero data import successfully.")


