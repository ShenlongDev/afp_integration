import logging
import re
from decimal import Decimal, InvalidOperation
from datetime import datetime, date, timezone as dt_timezone
from typing import Optional

from django.db import close_old_connections, transaction
from django.utils import timezone
from dateutil import tz
from dateutil.parser import parse as dateutil_parse
from integrations.services.utils import BatchUtils, compute_unique_key

from .client import NetSuiteClient
from integrations.models.models import Integration, SyncTableLogs, Organisation
from integrations.models.netsuite.analytics import (
    NetSuiteVendors,
    NetSuiteSubsidiaries,
    NetSuiteDepartments,
    NetSuiteEntity,
    NetSuiteAccountingPeriods,
    NetSuiteAccounts,
    NetSuiteTransactions,
    NetSuiteTransactionLine,
    NetSuiteTransactionAccountingLine,
    NetSuiteTransformedTransaction,
    NetSuiteBudgets,
    NetSuiteLocations,
    NetSuiteGeneralLedger,
)
from core.models import Site, IntegrationSiteMapping

logger = logging.getLogger(__name__)


def bool_from_str(val: Optional[str]) -> bool:
    """Convert 'T'/'F' (or similar) strings to boolean."""
    if not val:
        return False
    return val.strip().upper() in ('T', 'TRUE', 'YES')


def decimal_or_none(value):
    try:
        return Decimal(value) if value is not None else None
    except (InvalidOperation, TypeError):
        return None


class NetSuiteImporter:
    """
    A robust importer for NetSuite data using batch processing.
    All import/transform methods support optional date filtering.
    """
    def __init__(self, integration: Integration, since_date: Optional[str] = None, until_date: Optional[str] = None):
        self.integration = integration
        # Get settings from the integration.settings JSON field
        self.settings = integration.settings or {}
        
        # Get the account_id from settings
        account_id = self.settings.get("account_id")
        if not account_id:
            raise ValueError("Integration settings missing 'account_id'.")
        
        self.client = NetSuiteClient(account_id, integration)
        self.org = integration.organisation
        self.now_ts = timezone.now()
        self.tenant_id = integration.organisation.id
        self.since_date = since_date or timezone.datetime.combine(date.today(), datetime.min.time()).strftime("%Y-%m-%d %H:%M:%S")
        self.until_date = until_date  # May be None
        if since_date is None:
            self.since_date = timezone.now().date()

    def log_import_event(self, module_name: str, fetched_records: int):
        SyncTableLogs.objects.create(
            module_name=module_name,
            integration='NETSUITE',
            organisation=self.org,
            fetched_records=fetched_records,
            last_updated_time=timezone.now(),
            last_updated_date=timezone.now().date()
        )

    def build_date_clause(self, field: str, since: Optional[str] = None, until: Optional[str] = None) -> str:
        """
        Build a SuiteQL date filtering clause for the given field.
        Both since and until are expected in "YYYY-MM-DD HH:MI:SS" format.
        """
        clause = ""
        if since:
            clause += f" AND {field} >= TO_DATE('{since}', 'YYYY-MM-DD HH24:MI:SS')"
        if until:
            clause += f" AND {field} <= TO_DATE('{until}', 'YYYY-MM-DD HH24:MI:SS')"
        return clause

    # ------------------------------------------------------------
    # 1) Import Vendors
    # ------------------------------------------------------------
    def import_vendors(self):
        logger.info("Importing NetSuite Vendors...")
        date_clause = self.build_date_clause("LASTMODIFIEDDATE", self.since_date, self.until_date)
        query = f"SELECT * FROM Vendor WHERE 1=1 {date_clause}"
        rows = list(self.client.execute_suiteql(query))

        def process_vendor(r):
            vendor_id = r.get("id")
            if not vendor_id:
                return
            subsidiary = r.get("subsidiaryedition") or "Unknown"
            try:
                NetSuiteVendors.objects.update_or_create(
                    vendor_id=vendor_id,
                    defaults={
                        "tenant_id": self.org.id,
                        "entity_id": r.get("entityid"),
                        "is_person": bool_from_str(r.get("isperson")),
                        "is_inactive": bool_from_str(r.get("isinactive")),
                        "email": r.get("email"),
                        "phone": r.get("phone"),
                        "currency": r.get("currency"),
                        "subsidiary": subsidiary,
                        "terms": r.get("terms"),
                        "record_date": self.now_ts,
                    }
                )
            except Exception as e:
                logger.error(f"Error importing vendor row: {e}", exc_info=True)

        BatchUtils.process_in_batches(rows, process_vendor, batch_size=1000)
        self.log_import_event(module_name="netsuite_vendors", fetched_records=len(rows))
        logger.info(f"Imported Vendors: {len(rows)} records processed.")

    # ------------------------------------------------------------
    # 2) Import Subsidiaries
    # ------------------------------------------------------------
    def import_subsidiaries(self):
        logger.info("Importing NetSuite Subsidiaries...")
        date_clause = self.build_date_clause("lastmodifieddate", self.since_date, self.until_date)

        query = f"""
            SELECT id, name, fullname, legalname, iselimination, currency, country, lastmodifieddate
            FROM subsidiary
            WHERE 1=1 {date_clause}
            ORDER BY id
        """
        rows = list(self.client.execute_suiteql(query))

        def process_subsidiary(r):
            sub_id = r.get("id")
            if not sub_id:
                return
            try:
                NetSuiteSubsidiaries.objects.update_or_create(
                    subsidiary_id=sub_id,
                    defaults={
                        "tenant_id": self.org.id,
                        "name": r.get("name"),
                        "name_nohi": r.get("namenohierarchy"),
                        "full_name": r.get("fullname"),
                        "legal_name": r.get("legalname"),
                        "federal_number": r.get("federalnumber"),
                        "is_elimination": bool_from_str(r.get("iselimination")),
                        "currency": r.get("currency"),
                        "country": r.get("country"),
                        "record_date": self.now_ts,
                    }
                )
            except Exception as e:
                logger.error(f"Error importing subsidiary row: {e}", exc_info=True)

        BatchUtils.process_in_batches(rows, process_subsidiary, batch_size=1000)
        self.log_import_event(module_name="netsuite_subsidiaries", fetched_records=len(rows))
        logger.info(f"Imported Subsidiaries: {len(rows)} records processed.")

    # ------------------------------------------------------------
    # 3) Import Departments
    # ------------------------------------------------------------
    def import_departments(self):
        logger.info("Importing NetSuite Departments...")
        query = "SELECT id, name, fullname, subsidiary, isinactive FROM department ORDER BY id"
        rows = list(self.client.execute_suiteql(query))

        def process_department(r):
            dept_id = r.get("id")
            if not dept_id:
                return
            try:
                NetSuiteDepartments.objects.update_or_create(
                    department_id=dept_id,
                    defaults={
                        "tenant_id": self.org.id,
                        "name": r.get("name"),
                        "full_name": r.get("fullname"),
                        "subsidiary": r.get("subsidiary"),
                        "is_inactive": bool_from_str(r.get("isinactive")),
                        "record_date": self.now_ts,
                    }
                )
            except Exception as e:
                logger.error(f"Error importing department row: {e}", exc_info=True)

        BatchUtils.process_in_batches(rows, process_department, batch_size=1000)
        self.log_import_event(module_name="netsuite_departments", fetched_records=len(rows))
        logger.info(f"Imported Departments: {len(rows)} records processed.")

    # ------------------------------------------------------------
    # 4) Import Entities
    # ------------------------------------------------------------
    def import_entities(self):
        logger.info("Importing NetSuite Entities...")
        
        date_clause = self.build_date_clause("lastmodifieddate", self.since_date, self.until_date)
        query = f"SELECT * FROM entity WHERE 1=1 {date_clause}"
        rows = list(self.client.execute_suiteql(query))

        def process_entity(r):
            record_id = r.get("id") 
            ent_id = r.get("entityid") or record_id
            if not record_id:
                return
            subsidiary = r.get("subsidiaryedition") or "Unknown"
            try:
                NetSuiteEntity.objects.update_or_create(
                    id=record_id,  
                    defaults={
                        "tenant_id": self.org.id,
                        "entity_id": ent_id,  
                        "entity_title": r.get("entitytitle"),
                        "type": r.get("type"),
                        "external_id": r.get("externalid"),
                        "company_display_name": r.get("altname"),
                        "legal_name": r.get("legalname"),
                        "is_person": bool_from_str(r.get("isperson")),
                        "is_inactive": bool_from_str(r.get("isinactive")),
                        "parent_entity": r.get("parententity"),
                        "email": r.get("email"),
                        "phone": r.get("phone"),
                        "currency": r.get("currency"),
                        "subsidiary": subsidiary,
                        "terms": r.get("terms"),
                        "last_modified_date": self.parse_datetime(r.get("lastmodifieddate")),
                        "record_date": self.now_ts,
                    }
                )
            except Exception as e:
                logger.error(f"Error importing entity row: {e}", exc_info=True)

        BatchUtils.process_in_batches(rows, process_entity, batch_size=1000)
        self.log_import_event(module_name="netsuite_entities", fetched_records=len(rows))
        logger.info(f"Imported Entities: {len(rows)} records processed.")

    # ------------------------------------------------------------
    # 5) Import Accounting Periods
    # ------------------------------------------------------------
    def import_accounting_periods(self):
        logger.info("Importing NetSuite Accounting Periods...")
        
        date_clause = self.build_date_clause("lastmodifieddate", self.since_date, self.until_date)
        query = f"SELECT * FROM accountingperiod WHERE 1=1 {date_clause}"
        rows = list(self.client.execute_suiteql(query))

        def process_period(r):
            period_id = r.get("id")
            if not period_id:
                return
            start_date_obj = self.parse_date(r.get("startdate"))
            end_date_obj = self.parse_date(r.get("enddate"))
            year_val = start_date_obj.year if start_date_obj else None
            quarter_val = self.get_quarter(start_date_obj.month) if start_date_obj else None
            period_val = start_date_obj.month if start_date_obj else None
            try:
                NetSuiteAccountingPeriods.objects.update_or_create(
                    period_id=period_id,
                    defaults={
                        "tenant_id": self.org.id,
                        "period_name": r.get("periodname"),
                        "start_date": start_date_obj,
                        "end_date": end_date_obj,
                        "closed": bool_from_str(r.get("closed")),
                        "all_locked": bool_from_str(r.get("alllocked")),
                        "fiscal_calendar": r.get("fiscalcalendar"),
                        "year": year_val,
                        "quarter": quarter_val,
                        "period": period_val,
                        "record_date": self.now_ts,
                    }
                )
            except Exception as e:
                logger.error(f"Error importing accounting period row: {e}", exc_info=True)

        BatchUtils.process_in_batches(rows, process_period, batch_size=1000)
        self.log_import_event(module_name="netsuite_accounting_periods", fetched_records=len(rows))
        logger.info(f"Imported Accounting Periods: {len(rows)} records processed.")

    # ------------------------------------------------------------
    # 6) Import Accounts (with pagination)
    # ------------------------------------------------------------
    def import_accounts(self):

        offset = 0
        limit = 1000
        total_imported = 0

        while True:
            close_old_connections()
            date_clause = self.build_date_clause("lastmodifieddate", self.since_date, self.until_date)
            query = f"""
            SELECT *
            FROM Account
            WHERE 1=1 {date_clause}
            ORDER BY ID ASC
            OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY
            """
            rows = list(self.client.execute_suiteql(query))
            if not rows:
                break

            def process_account(r):
                account_id = r.get("id")
                if not account_id:
                    return
                try:
                    NetSuiteAccounts.objects.update_or_create(
                        account_id=account_id,
                        defaults={
                            "tenant_id": self.org.id,
                            "acctnumber": r.get("acctnumber"),
                            "accountsearchdisplaynamecopy": r.get("accountsearchdisplaynamecopy"),
                            "fullname": r.get("fullname"),
                            "accountsearchdisplayname": r.get("accountsearchdisplayname"),
                            "displaynamewithhierarchy": r.get("displaynamewithhierarchy"),
                            "parent": r.get("parent"),
                            "accttype": r.get("accttype"),
                            "sspecacct": r.get("sspecacct"),
                            "description": r.get("description"),
                            "eliminate": bool_from_str(r.get("eliminate")),
                            "externalid": r.get("externalid"),
                            "include_children": bool_from_str(r.get("includechildren")),
                            "inventory": bool_from_str(r.get("inventory")),
                            "is_inactive": bool_from_str(r.get("isinactive")),
                            "is_summary": bool_from_str(r.get("issummary")),
                            "last_modified_date": self.parse_datetime(r.get("lastmodifieddate")),
                            "reconcile_with_matching": bool_from_str(r.get("reconcilewithmatching")),
                            "revalue": bool_from_str(r.get("revalue")),
                            "subsidiary": r.get("subsidiary"),
                            "balance": decimal_or_none(r.get("balance")),
                            "record_date": self.now_ts,
                            "consolidation_key": self.settings.get("account_id"),
                        }
                    )
                except Exception as e:
                    logger.error(f"Error importing account row: {e}", exc_info=True)

            BatchUtils.process_in_batches(rows, process_account, batch_size=1000)
            total_imported += len(rows)
            offset += limit
            logger.debug(f"Imported {len(rows)} accounts at offset {offset}.")
            if len(rows) < limit:
                break

        self.log_import_event(module_name="netsuite_accounts", fetched_records=total_imported)
        logger.info(f"Imported Accounts: {total_imported} records processed.")

    # ------------------------------------------------------------
    # 7) Import Transactions (using keyset pagination and date filtering)
    # ------------------------------------------------------------
    def import_transactions(self, last_import_date: Optional[str] = None):
        logger.info(
            "Importing NetSuite Transactions " +
            ("incrementally..." if (last_import_date or self.since_date) else "(full import)...")
        )

        batch_size = 500
        total_imported = 0
        # Use numeric boundaries if Transaction.ID is numeric.
        min_id = 0

        # Determine the start date and build the date filter clause.
        start_date = last_import_date or self.since_date
        date_filter_clause = self.build_date_clause("LASTMODIFIEDDATE", start_date, None)
        
        while True:
            # Build query for the next batch.
            query = f"""
                SELECT 
                    Transaction.ID,
                    Transaction.TranID,
                    Transaction.TranDate,
                    BUILTIN.DF(Transaction.PostingPeriod) AS PostingPeriod,
                    Transaction.Memo,
                    Transaction.Posting,
                    BUILTIN.DF(Transaction.Status) AS Status,
                    BUILTIN.DF(Transaction.CreatedBy) AS CreatedBy,
                    BUILTIN.DF(Transaction.Subsidiary) AS Subsidiary,
                    BUILTIN.DF(Transaction.Entity) AS Entity,
                    Transaction.Type AS type,
                    Transaction.CreatedDate AS createddate,
                    BUILTIN.DF(Transaction.Currency) AS currency,
                    Transaction.AbbrevType AS abbrevtype,
                    BUILTIN.DF(Transaction.ApprovalStatus) AS approvalstatus,
                    BUILTIN.DF(Transaction.BalSegStatus) AS balsegstatus,
                    Transaction.BillingStatus AS billingstatus,
                    Transaction.CloseDate AS closedate,
                    Transaction.CustomType AS customtype,
                    Transaction.DaysOpen AS daysopen,
                    Transaction.DaysOverdueSearch AS daysoverduesearch,
                    Transaction.DueDate AS duedate,
                    Transaction.ExchangeRate AS exchangerate,
                    Transaction.ExternalId AS externalid,
                    Transaction.ForeignAmountPaid AS foreignamountpaid,
                    Transaction.ForeignAmountUnpaid AS foreignamountunpaid,
                    Transaction.ForeignTotal AS foreigntotal,
                    Transaction.IsFinChrg AS isfinchrg,
                    Transaction.IsReversal AS isreversal,
                    BUILTIN.DF(Transaction.LastModifiedBy) AS lastmodifiedby,
                    Transaction.LastModifiedDate AS lastmodifieddate,
                    Transaction.Nexus AS nexus,
                    Transaction.Number AS number,
                    Transaction.OrdPicked AS ordpicked,
                    Transaction.PaymentHold AS paymenthold,
                    Transaction.PrintedPickingTicket AS printedpickingticket,
                    Transaction.RecordType AS recordtype,
                    Transaction.Source AS source,
                    Transaction.ToBePrinted AS tobeprinted,
                    Transaction.TranDate AS trandate,
                    Transaction.TranDisplayName AS trandisplayname,
                    Transaction.TranId AS tranid,
                    Transaction.TransactionNumber AS transactionnumber,
                    Transaction.Void AS void,
                    Transaction.Voided AS voided,
                    Transaction.Location AS location_id,
                    BUILTIN.DF(Transaction.Terms) AS terms,
                    BUILTIN.DF(Transaction.Location) AS locations
                FROM 
                    Transaction
                WHERE 
                    ID > {min_id}
                    {date_filter_clause}
                ORDER BY 
                    ID ASC
                FETCH NEXT {batch_size} ROWS ONLY
            """
            
            rows = list(self.client.execute_suiteql(query))
            print(f"Fetched {len(rows)} transaction records at min_id {min_id}")
            if not rows:
                break

            for r in rows:
                txn_id = r.get("id")
                if not txn_id:
                    continue

                last_mod = self.parse_datetime(r.get("lastmodifieddate"))
                if not last_mod:
                    continue

                try:
                    NetSuiteTransactions.objects.update_or_create(
                        transactionid=int(txn_id),
                        tenant_id=self.org.id,
                        defaults={
                            "abbrevtype": r.get("abbrevtype"),
                            "approvalstatus": r.get("approvalstatus"),
                            "balsegstatus": r.get("balsegstatus"),
                            "billingstatus": r.get("billingstatus"),
                            "closedate": self.parse_date(r.get("closedate")),
                            "createdby": r.get("createdBy"),
                            "createddate": self.parse_date(r.get("createddate")),
                            "currency": r.get("currency"),
                            "customtype": r.get("customtype"),
                            "daysopen": r.get("daysopen"),
                            "daysoverduesearch": r.get("daysoverduesearch"),
                            "duedate": self.parse_date(r.get("duedate")),
                            "entity": r.get("Entity"),
                            "exchangerate": decimal_or_none(r.get("exchangerate")),
                            "externalid": r.get("externalid"),
                            "foreignamountpaid": decimal_or_none(r.get("foreignamountpaid")),
                            "foreignamountunpaid": decimal_or_none(r.get("foreignamountunpaid")),
                            "foreigntotal": decimal_or_none(r.get("foreigntotal")),
                            "number": decimal_or_none(r.get("number")),
                            "isfinchrg": r.get("isfinchrg"),
                            "isreversal": r.get("isreversal"),
                            "lastmodifiedby": r.get("lastmodifiedby"),
                            "lastmodifieddate": last_mod,
                            "nexus": r.get("nexus"),
                            "ordpicked": r.get("ordpicked"),
                            "paymenthold": r.get("paymenthold"),
                            "posting": r.get("posting"),
                            "postingperiod": r.get("postingperiod"),
                            "printedpickingticket": r.get("printedpickingticket"),
                            "recordtype": r.get("recordtype"),
                            "source": r.get("source"),
                            "status": r.get("status"),
                            "terms": r.get("terms"),
                            "tobeprinted": r.get("tobeprinted"),
                            "trandate": self.parse_date(r.get("trandate")),
                            "trandisplayname": r.get("trandisplayname"),
                            "tranid": r.get("tranid"),
                            "transactionnumber": r.get("transactionnumber"),
                            "type": r.get("type"),
                            "visibletocustomer": r.get("visibletocustomer"),
                            "void_field": r.get("void"),
                            "voided": r.get("voided"),
                            "memo": r.get("memo"),
                            "record_date": last_mod,
                            "consolidation_key": self.settings.get("account_id"),
                        }
                    )
                except Exception as e:
                    logger.error(f"Error importing transaction row: {e}", exc_info=True)
            
            total_imported += len(rows)
            # Update min_id with the last row's ID.
            min_id = rows[-1].get("id")
            
            if len(rows) < batch_size:
                break

        logger.info(f"Completed importing transactions. Total imported: {total_imported}.")


    # ------------------------------------------------------------
    # 9) Import Transaction Lines (with date filtering)
    # ------------------------------------------------------------
    def import_transaction_lines(self, min_id: Optional[str] = None, last_modified_after: Optional[str] = None,
                                            start_date: Optional[str] = None, end_date: Optional[str] = None):
        logger.info("Importing NetSuite Transaction Lines...")
        batch_size = 500

        # The composite pagination will use two boundaries: a transaction and a unique key.
        # Initialize the boundaries. Using "0" is typical if transactions and keys are numeric or lexically orderable.
        last_transaction = min_id or "0"
        last_uniquekey = "0"
        
        # Default dates
        start_date = start_date or self.since_date
        print(f"start_date: {start_date}")
        total_fetched = 0
        date_filter_clause = self.build_date_clause("LINELASTMODIFIEDDATE", since=last_modified_after or start_date, until=end_date)

        line_counter = 0
        while True:
            close_old_connections()
            # Build query using composite conditions.
            # It selects lines where either the transaction is greater than the last fetched
            # or where the transaction equals the last fetched and the uniquekey is greater.
            query = f"""
                SELECT L.memo, L.accountinglinetype, L.cleared, L.closedate, L.commitmentfirm, L.creditforeignamount, 
                    BUILTIN.DF( L.department ) AS department, L.department AS departmentid, L.documentnumber, 
                    L.donotdisplayline, L.eliminate, BUILTIN.DF( L.entity ) AS entity, L.entity AS entityid, 
                    L.expenseaccount AS expenseaccountid, BUILTIN.DF( L.expenseaccount ) AS expenseaccount, 
                    L.foreignamount, L.foreignamountpaid, L.foreignamountunpaid, L.id, L.isbillable, L.isclosed, 
                    L.iscogs, L.iscustomglline, L.isfullyshipped, L.isfxvariance, L.isinventoryaffecting, 
                    L.isrevrectransaction, L.linelastmodifieddate, L.linesequencenumber, L.mainline, 
                    L.matchbilltoreceipt, L.netamount, L.oldcommitmentfirm, L.quantitybilled, L.quantityrejected, 
                    L.quantityshiprecv, BUILTIN.DF( L.subsidiary ) AS subsidiary, L.subsidiary AS subsidiaryid, 
                    L.taxline, L.transaction, L.transactiondiscount, L.uniquekey,
                    L.location AS line_location_id,
                    BUILTIN.DF(L.location) AS line_location_name,
                    L.class 
                FROM TransactionLine L 
                WHERE 
                    (L.transaction > {last_transaction} 
                    OR (L.transaction = {last_transaction} AND L.uniquekey > {last_uniquekey}))
                    {date_filter_clause}
                ORDER BY L.transaction, L.uniquekey ASC
                FETCH FIRST {batch_size} ROWS ONLY
            """
            
            try:
                rows = list(self.client.execute_suiteql(query))
                if not rows:
                    break
                
                logger.info(f"Fetched {len(rows)}, transaction > {last_transaction} or (transaction = {last_transaction} and uniquekey > {last_uniquekey}) {date_filter_clause}.")
                print(f"Fetched {len(rows)}, transaction > {last_transaction} or (transaction = {last_transaction} and uniquekey > {last_uniquekey})")
                # Update boundaries to the last row of the current batch
                last_row = rows[-1]
                last_transaction = last_row.get("transaction")
                last_uniquekey = last_row.get("uniquekey")
                
            except Exception as e:
                logger.error(f"Error importing transaction lines: {e}", exc_info=True)
                return

            def process_line(r):
                nonlocal line_counter
                line_counter += 1
                

                try:
                    last_modified = self.parse_datetime(r.get("linelastmodifieddate"))
                    NetSuiteTransactionLine.objects.update_or_create(
                        uniquekey=r.get("uniquekey"),
                        defaults={
                            "transaction_line_id": r.get("id"),
                            "tenant_id": self.org.id,
                            "is_billable": r.get("isbillable") == 'T', 
                            "is_closed": r.get("isclosed") == 'T',
                            "is_cogs": r.get("iscogs") == 'T',
                            "is_custom_gl_line": r.get("iscustomglline") == 'T',
                            "is_fully_shipped": r.get("isfullyshipped") == 'T',
                            "is_fx_variance": r.get("isfxvariance") == 'T',
                            "is_inventory_affecting": r.get("isinventoryaffecting") == 'T',
                            "is_rev_rec_transaction": r.get("isrevrectransaction") == 'T',
                            "line_last_modified_date": last_modified.date() if last_modified else None,
                            "line_sequence_number": r.get("linesequencenumber"),
                            "location": r.get("location"),
                            "main_line": r.get("mainline") == 'T',
                            "match_bill_to_receipt": r.get("matchbilltoreceipt") == 'T',
                            "memo": r.get("memo"),
                            "net_amount": decimal_or_none(r.get("netamount")),
                            "old_commitment_firm": r.get("oldcommitmentfirm") == 'T',
                            "quantity_billed": decimal_or_none(r.get("quantitybilled")),
                            "quantity_rejected": decimal_or_none(r.get("quantityrejected")),
                            "quantity_ship_recv": decimal_or_none(r.get("quantityshiprecv")),
                            "subsidiary": r.get("subsidiary"),
                            "subsidiaryid": r.get("subsidiaryid"),
                            "tax_line": r.get("taxline") == 'T',
                            "transaction_discount": r.get("transactiondiscount") == 'T',
                            "transactionid": r.get("transaction"),
                            # New fields with proper handling:
                            "accountinglinetype": r.get("accountinglinetype"),
                            "cleared": r.get("cleared") == 'T',
                            "commitmentfirm": r.get("commitmentfirm") == 'T',
                            "department": r.get("department"),
                            "departmentid": r.get("departmentid"),
                            "donotdisplayline": r.get("donotdisplayline") == 'T',
                            "eliminate": r.get("eliminate") == 'T',
                            "entity": r.get("entity"),
                            "entityid": r.get("entityid"),
                            "expenseaccount": r.get("expenseaccount"),
                            "expenseaccountid": r.get("expenseaccountid"),
                            "foreignamount": decimal_or_none(r.get("foreignamount")),
                            "foreignamountpaid": decimal_or_none(r.get("foreignamountpaid")),
                            "foreignamountunpaid": decimal_or_none(r.get("foreignamountunpaid")),
                            "creditforeignamount": decimal_or_none(r.get("creditforeignamount")),
                            "closedate": self.parse_date(r.get("closedate")),
                            "documentnumber": r.get("documentnumber"),
                            "class_field": r.get("class"),
                            "uniquekey": r.get("uniquekey"),
                            "consolidation_key": self.settings.get("account_id"),
                        }
                    )
                except Exception as e:
                    logger.error(f"Error importing transaction line row: {e}", exc_info=True)
            
            BatchUtils.process_in_batches(rows, process_line, batch_size=batch_size)
            total_fetched += len(rows)
            logger.info(f"Processed batch. New boundary: transaction {last_transaction}, uniquekey {last_uniquekey}. Total imported: {total_fetched}.")
            
            # Break if we got less than the batch size (indicating we're at the end of the data)
            if len(rows) < batch_size:
                break

        self.log_import_event(module_name="netsuite_transaction_lines", fetched_records=total_fetched)
        print(f"total fetched: {total_fetched}")
        logger.info("Transaction Line import complete.")


    # ------------------------------------------------------------
    # 10) Import Transaction Accounting Lines (with date filtering and keyset pagination)
    # ------------------------------------------------------------
    def import_transaction_accounting_lines(self, min_transaction: Optional[str] = None,
                                          last_modified_after: Optional[str] = None,
                                          start_date: Optional[str] = None,
                                          end_date: Optional[str] = None):
        logger.info("Importing Transaction Accounting Lines...")
        min_transaction = min_transaction or "0"
        min_transactionline = "0"  
        limit = 500
        total_imported = 0
        start_date = start_date or self.since_date
        data = []
        date_filter_clause = ""
        if last_modified_after:
            date_filter_clause += f" AND LASTMODIFIEDDATE > TO_DATE('{last_modified_after}', 'YYYY-MM-DD HH24:MI:SS')"
        else:
            if start_date:
                date_filter_clause += f" AND LASTMODIFIEDDATE >= TO_DATE('{start_date}', 'YYYY-MM-DD HH24:MI:SS')"
            if end_date:
                date_filter_clause += f" AND LASTMODIFIEDDATE <= TO_DATE('{end_date}', 'YYYY-MM-DD HH24:MI:SS')"

        while True:
            close_old_connections()
            query = f"""
                SELECT
                    TRANSACTION,
                    TRANSACTIONLINE,
                    ACCOUNT,
                    BUILTIN.DF(ACCOUNTINGBOOK) AS ACCOUNTINGBOOK,
                    AMOUNT,
                    AMOUNTLINKED,
                    DEBIT,
                    NETAMOUNT,
                    PAYMENTAMOUNTUNUSED,
                    PAYMENTAMOUNTUSED,
                    POSTING,
                    CREDIT,
                    AMOUNTPAID,
                    AMOUNTUNPAID,
                    LASTMODIFIEDDATE,
                    PROCESSEDBYREVCOMMIT
                FROM TransactionAccountingLine
                WHERE 
                    (TRANSACTION > {min_transaction} 
                    OR (TRANSACTION = {min_transaction} AND TRANSACTIONLINE > {min_transactionline}))
                    {date_filter_clause}
                ORDER BY TRANSACTION ASC, TRANSACTIONLINE ASC
                FETCH NEXT {limit} ROWS ONLY
            """
            try:
                rows = list(self.client.execute_suiteql(query))
                print(f"Fetched {len(rows)} rows with boundaries: TRANSACTION {min_transaction} and TRANSACTIONLINE {min_transactionline}.")
                logger.info(f"Fetched {len(rows)} rows with composite boundary (TRANSACTION > {min_transaction} or (TRANSACTION = {min_transaction} and TRANSACTIONLINE > {min_transactionline})) {date_filter_clause}.")
            except Exception as e:
                logger.error(f"Error importing transaction accounting lines: {e}", exc_info=True)
                return

            if not rows:
                logger.info("No more rows to fetch, ending loop.")
                break

            def process_accounting_line(r):
                try:
                    last_modified = self.parse_datetime(r.get("lastmodifieddate"))
                    NetSuiteTransactionAccountingLine.objects.update_or_create(
                        transaction=int(r.get("transaction")),
                        transaction_line=int(r.get("transactionline")),
                        tenant_id=self.org.id,
                        defaults={
                            "links": r.get("links"),
                            "accountingbook": r.get("accountingbook") if r.get("accountingbook") else None,
                            "account": int(r.get("account")) if r.get("account") else None,
                            "amount": decimal_or_none(r.get("amount")),
                            "amountlinked": decimal_or_none(r.get("amountlinked")),
                            "debit": decimal_or_none(r.get("debit")),
                            "netamount": decimal_or_none(r.get("netamount")),
                            "paymentamountunused": decimal_or_none(r.get("paymentamountunused")),
                            "paymentamountused": decimal_or_none(r.get("paymentamountused")),
                            "posting": r.get("posting"),
                            "credit": decimal_or_none(r.get("credit")),
                            "amountpaid": decimal_or_none(r.get("amountpaid")),
                            "amountunpaid": decimal_or_none(r.get("amountunpaid")),
                            "lastmodifieddate": last_modified,
                            "processedbyrevcommit": r.get("processedbyrevcommit"),
                            # New fields:
                            "consolidation_key": self.settings.get("account_id"),
                            "source_uri": r.get("source_uri"),
                        }
                    )
                except Exception as e:
                    logger.error(f"Error importing transaction accounting line row: {e}", exc_info=True)
            #adding rows to the data list
            # this will be used to save the data in the database later
            data.append(rows)
            total_imported += len(rows)

            last_row = rows[-1]
            new_min_transaction = str(last_row.get("transaction"))
            new_min_transactionline = str(last_row.get("transactionline"))

            logger.info(f"Processed batch. New boundary: TRANSACTION {new_min_transaction}, TRANSACTIONLINE {new_min_transactionline}. Total imported: {total_imported}.")

            if new_min_transaction == min_transaction and new_min_transactionline == min_transactionline:
                logger.warning("Pagination boundaries did not change. Exiting loop to avoid infinite loop.")
                break

            min_transaction, min_transactionline = new_min_transaction, new_min_transactionline

            if len(rows) < limit:
                logger.info("Fewer rows than limit fetched. Likely reached end of records.")
                break

        print(f"Total imported transaction accounting lines: {total_imported}")
        for rows in data:

            BatchUtils.process_in_batches(rows, process_accounting_line, batch_size=limit)
            logger.info(f"Processed batch of {len(rows)} transaction accounting lines.")
        

        self.log_import_event(module_name="netsuite_transaction_accounting_lines", fetched_records=total_imported)
        logger.info(f"Imported Transaction Accounting Lines: {total_imported} records processed.")


        
    def import_general_ledger(self):
        logger.info("Importing NetSuite General Ledger...")

        #creating date clause for sync
        date_clause = self.build_date_clause("lastmodifieddate", self.since_date, self.until_date)
        batch_size = 500
        # count of total rows imported in this suync
        total_imported = 0

        # the key for the minmum value of transaction line unique id
        min_key = 0  

        # list to append data for each batch
        # this will be used to save the data in the database later
        total_data = []

        while True:
            #optimized General Ledger Script using Transaction and TransactionLine
            query2 = f"""
                SELECT
                BUILTIN.DF( L.account ) AS account, L.account AS accountid,
                L.memo, L.accountinglinetype, L.id as lineid, L.cleared, L.closedate, L.commitmentfirm, L.creditforeignamount, 
                        BUILTIN.DF( L.department ) AS department, L.department AS departmentid, L.documentnumber, 
                        L.donotdisplayline, L.eliminate, BUILTIN.DF( L.entity ) AS entity, L.entity AS entityid, 
                        L.expenseaccount AS expenseaccountid, BUILTIN.DF( L.expenseaccount ) AS expenseaccount, 
                        L.foreignamount, L.foreignamountpaid, L.foreignamountunpaid, L.id, L.isbillable, L.isclosed, 
                        L.iscogs, L.iscustomglline, L.isfullyshipped, L.isfxvariance, L.isinventoryaffecting, 
                        L.isrevrectransaction, L.linelastmodifieddate, L.linesequencenumber, L.mainline, 
                        L.matchbilltoreceipt, L.netamount, L.oldcommitmentfirm, L.quantitybilled, L.quantityrejected, 
                        L.quantityshiprecv, BUILTIN.DF( L.subsidiary ) AS subsidiary, L.subsidiary AS subsidiaryid, 
                        L.taxline, L.transaction, L.transactiondiscount, L.uniquekey,
                        L.location AS line_location_id, BUILTIN.DF(L.location) AS line_location_name,
                        L.class, Transaction.ID, Transaction.TranID, Transaction.TranDate,
                        BUILTIN.DF(Transaction.PostingPeriod) AS PostingPeriod,
                        Transaction.Memo,
                        Transaction.Posting,
                        BUILTIN.DF(Transaction.Status) AS Status,
                        BUILTIN.DF(Transaction.CreatedBy) AS CreatedBy,
                        BUILTIN.DF(Transaction.Subsidiary) AS Subsidiary,
                        BUILTIN.DF(Transaction.Entity) AS Entity,
                        Transaction.Type AS type,
                        Transaction.CreatedDate AS createddate,
                        BUILTIN.DF(Transaction.Currency) AS currency,
                        Transaction.AbbrevType AS abbrevtype,
                        BUILTIN.DF(Transaction.ApprovalStatus) AS approvalstatus,
                        BUILTIN.DF(Transaction.BalSegStatus) AS balsegstatus,
                        Transaction.BillingStatus AS billingstatus,
                        Transaction.CloseDate AS closedate,
                        Transaction.CustomType AS customtype,
                        Transaction.DaysOpen AS daysopen,
                        Transaction.DaysOverdueSearch AS daysoverduesearch,
                        Transaction.DueDate AS duedate,
                        Transaction.ExchangeRate AS exchangerate,
                        Transaction.ExternalId AS externalid,
                        Transaction.ForeignAmountPaid AS foreignamountpaid,
                        Transaction.ForeignAmountUnpaid AS foreignamountunpaid,
                        Transaction.ForeignTotal AS foreigntotal,
                        Transaction.IsFinChrg AS isfinchrg,
                        Transaction.IsReversal AS isreversal,
                        BUILTIN.DF(Transaction.LastModifiedBy) AS lastmodifiedby,
                        Transaction.LastModifiedDate AS lastmodifieddate,
                        Transaction.Nexus AS nexus,
                        Transaction.Number AS number,
                        Transaction.OrdPicked AS ordpicked,
                        Transaction.PaymentHold AS paymenthold,
                        Transaction.PrintedPickingTicket AS printedpickingticket,
                        Transaction.RecordType AS recordtype,
                        Transaction.Source AS source,
                        Transaction.ToBePrinted AS tobeprinted,
                        Transaction.TranDate AS trandate,
                        Transaction.TranDisplayName AS trandisplayname,
                        Transaction.TranId AS tranid,
                        Transaction.TransactionNumber AS transactionnumber,
                        Transaction.Void AS void,
                        Transaction.Voided AS voided,
                        Transaction.Location AS location_id,
                        BUILTIN.DF(Transaction.Terms) AS terms,
                        BUILTIN.DF(Transaction.Location) AS locations,
                        GREATEST(-1*L.AMOUNT,0) AS Credit,
                        GREATEST(L.AMOUNT,0) AS Debit
                From TransactionLine L
                Left Join Transaction on L.transaction = Transaction.id
                Where L.uniquekey > {min_key}
                {date_clause}
                Order By L.uniquekey ASC
                Fetch NEXT {batch_size} ROWS ONLY
                """


            rows = list(self.client.execute_suiteql(query2))
            
            logger.info("Fetched rows: ", len(rows), " with boundaries: ", min_key)
            if len(rows)> 0:
                logger.info(f"Fetched {len(rows)} rows with boundaries: .")

                #adding current fetch to the total data list to save later
                total_data.append(rows)

                total_imported += len(rows)
                
                #setting the minimum key to the last row of the current batch
                # this will be used to fetch the next batch of data
                min_key = rows[-1].get("uniquekey")

                if len(rows) < batch_size:
                    logger.info("Fewer rows than limit fetched. Likely reached end of records.")
                    break
            else:
                logger.info(f"No more rows to fetch, ending loop. Total Fetched: {total_imported}")
                break
            
            # #save to json file
            # with open('GLdata.json', 'w') as f:
            #     import json
            #     json.dump(rows, f,indent=4)
        def processRow(r):
            try:
                # print(f"Processing transaction accounting line: {r.get('uniquekey')}")
                last_modified = self.parse_datetime(r.get("lastmodifieddate"))
                NetSuiteGeneralLedger.objects.update_or_create(
                    transaction_line_id=r.get("lineid"),
                    transaction_id=r.get("transaction"),
                    tenant_id=self.org.id,
                    defaults={
                        "tenant_id": self.org.id,
                        "type": r.get("abbrevtype"),
                        'account_id': r.get("accountid"),
                        "account_name": r.get("account"),
                        "accounting_line_type": r.get("accountinglinetype"),
                        "approval_status": r.get("approvalstatus"),
                        "balance_segment_status": r.get("balsegstatus"),
                        "billing_status": r.get("billingstatus"),
                        "cleared": r.get("cleared"),
                        "close_date": self.parse_date(r.get("closedate")),
                        "comitment_firm": r.get("commitmentfirm"),
                        "created_by": r.get("createdby"),
                        "created_date": self.parse_date(r.get("createddate")),
                        "credit_amount": decimal_or_none(r.get("credit")),
                        "credit_foreign_amount": decimal_or_none(r.get("creditforeignamount")),
                        "currency": r.get("currency"),
                        "debit_amount": decimal_or_none(r.get("debit")),
                        "document_number": r.get("documentnumber"),
                        "due_date": self.parse_date(r.get("duedate")),
                        "department": r.get("department"),
                        "department_id": r.get("departmentid"),
                        "entity": r.get("entity"),
                        "entity_id": r.get("entityid"),
                        "exchange_rate": decimal_or_none(r.get("exchangerate")),
                        "expense_account": r.get("expenseaccount"),
                        "expense_account_id": r.get("expenseaccountid"),
                        "external_id": r.get("externalid"),
                        "foreign_amount": decimal_or_none(r.get("foreignamount")),
                        "foreign_amount_paid": decimal_or_none(r.get("foreignamountpaid")),
                        "foreign_amount_unpaid": decimal_or_none(r.get("foreignamountunpaid")),
                        "foreign_total": decimal_or_none(r.get("foreigntotal")),
                        "transaction_id" : r.get("id"),
                        "transaction_line_id": r.get("lineid"),
                        "is_billable": r.get("isbillable"),
                        "is_closed": r.get("isclosed"),
                        "is_cogs": r.get("iscogs"),
                        "is_custom_gl_line": r.get("iscustomglline"),
                        "is_fully_shipped": r.get("isfullyshipped"),
                        "is_inventory_affecting": r.get("isinventoryaffecting"),
                        "is_reversal": r.get("isreversal"),
                        "is_rev_rec_transaction": r.get("isrevrectransaction"),
                        "last_modified_date": last_modified,
                        "last_modified_by": r.get("lastmodifiedby"),
                        "line_sequence_number": r.get("linesequencenumber"),
                        "match_bill_to_receipt": r.get("matchbilltoreceipt"),
                        "memo": r.get("memo"),
                        "net_amount": decimal_or_none(r.get("netamount")),
                        "nexus": r.get("nexus"),
                        "number": r.get("number"),
                        "payment_hold": r.get("paymenthold"),
                        "posting": r.get("posting"),
                        "posting_period": r.get("postingperiod"),
                        "quantity_billed": decimal_or_none(r.get("quantitybilled")),
                        "quantity_rejected": decimal_or_none(r.get("quantityrejected")),
                        "quantity_ship_recv": decimal_or_none(r.get("quantityshiprecv")),
                        "record_type": r.get("recordtype"),
                        "source": r.get("source"),
                        "status": r.get("status"),
                        "subsidiary": r.get("subsidiary"),
                        "subsidiary_id": r.get("subsidiaryid"),
                        "tax_line": r.get("taxline"),
                        "transaction_discount": r.get("transactiondiscount"),
                        "transaction_number": r.get("transactionnumber"),
                        "tran_date": self.parse_date(r.get("trandate")),
                        "tran_display_name": r.get("trandisplayname"),
                        "tran_id": r.get("tranid"),
                        "line_unique_key": r.get("uniquekey"),
                        "void": r.get("void"),
                        "voided": r.get("voided"),
                    })
            except Exception as e:
                logger.error(f"Error importing transaction accounting line row: {e}", exc_info=True)
                return
        
        print("total  Rows fetched: ", total_imported)
        for rows in total_data:
            #pulling the data from the list and processing it in batches
            for row in rows:
                # processing each row inside the batch
                # using single rather than batch import as the data might be too large sometime
                processRow(row)
                



        



    # ------------------------------------------------------------
    # 11) Import Budgets
    # ------------------------------------------------------------
    def import_budgets(self):
        logger.info("Importing NetSuite Budgets...")
        # Build a date clause using lastmodifieddate field
        date_clause = self.build_date_clause("lastmodifieddate", self.since_date, self.until_date)
        # Adjust the query as necessary; here we assume the table name is "Budget"
        query = f"SELECT * FROM Budgets WHERE 1=1 {date_clause}"
        rows = list(self.client.execute_suiteql(query))
        print(f"fetched {len(rows)} budget records")

        def process_budget(r):
            budget_id = r.get("id")
            if not budget_id:
                return
            try:
                NetSuiteBudgets.objects.update_or_create(
                    budget_id=budget_id,
                    defaults={
                        "tenant_id": self.org.id,
                        "account_id": r.get("account"), 
                        "amount": decimal_or_none(r.get("amount")),
                        "fiscal_year": r.get("fiscalyear"),
                        "period": r.get("period"),
                        "last_modified_date": self.parse_datetime(r.get("lastmodifieddate")),
                        "record_date": self.now_ts,
                    }
                )
            except Exception as e:
                logger.error(f"Error importing budget row: {e}", exc_info=True)

        BatchUtils.process_in_batches(rows, process_budget, batch_size=1000)
        self.log_import_event(module_name="netsuite_budgets", fetched_records=len(rows))
        logger.info(f"Imported Budgets: {len(rows)} records processed.")
    
    # ------------------------------------------------------------
    # 12) Import Locations
    # ------------------------------------------------------------
    def import_locations(self):
        logger.info("Importing NetSuite Locations...")
        date_clause = self.build_date_clause("lastmodifieddate", self.since_date, self.until_date)
        query = f"""
        SELECT * 
        FROM location
        WHERE 1=1 {date_clause}
        ORDER BY id
        """
        rows = list(self.client.execute_suiteql(query))

        def process_location(r):
            location_id = r.get("id")
            if not location_id:
                return
            
            try:
                with transaction.atomic():
                    location_name = r.get("name")
                    full_name = r.get("fullname")
                    is_inactive = bool_from_str(r.get("isinactive"))
                    status = 'inactive' if is_inactive else 'active'
                    
                    try:
                        mapping = IntegrationSiteMapping.objects.get(
                            integration=self.integration,
                            external_id=location_id
                        )
                        site = mapping.site
                        site.name = location_name
                        site.status = status
                        site.save(update_fields=["name", "status", "updated_at"])
                        
                    except IntegrationSiteMapping.DoesNotExist:
                        today = timezone.now().date()
                        site = Site(
                            organisation=self.integration.organisation,
                            name=location_name,
                            description=f"NetSuite location: {full_name}" if full_name and full_name != location_name else None,
                            postcode="",
                            region="",
                            opened_date=today,
                            status=status
                        )
                        
                        if r.get("mainaddress"):
                            address_data = r.get("mainaddress")
                            if isinstance(address_data, str):
                                site.address_line1 = address_data
                            elif isinstance(address_data, dict):
                                site.address_line1 = address_data.get("addr1", "")
                                site.address_line2 = address_data.get("addr2", "")
                                site.city = address_data.get("city", "")
                                site.state_code = address_data.get("state", "")
                                site.zip_code = address_data.get("zip", "")
                                site.country = address_data.get("country", "")
                        
                        site.save()
                        
                        mapping = IntegrationSiteMapping.objects.create(
                            site=site,
                            integration=self.integration,
                            external_id=location_id,
                            external_name=full_name,
                            settings={
                                "include_children": bool_from_str(r.get("includechildren")),
                                "parent_location_id": r.get("parent"),
                                "subsidiary_id": r.get("subsidiary"),
                                "last_modified_date": self.parse_datetime(r.get("lastmodifieddate")).isoformat() if self.parse_datetime(r.get("lastmodifieddate")) else None,
                                "netsuite_external_id": r.get("externalid"),
                            }
                        )
                    
                    mapping.external_name = full_name
                    mapping.settings.update({
                        "include_children": bool_from_str(r.get("includechildren")),
                        "parent_location_id": r.get("parent"),
                        "subsidiary_id": r.get("subsidiary"),
                        "last_modified_date": self.parse_datetime(r.get("lastmodifieddate")).isoformat() if self.parse_datetime(r.get("lastmodifieddate")) else None,
                        "netsuite_external_id": r.get("externalid"),
                    })
                    mapping.save(update_fields=["external_name", "settings", "updated_at"])
                    
                    logger.info(f"Processed NetSuite location {location_id}: {location_name}")
                
            except Exception as e:
                logger.error(f"Error importing location row: {e}", exc_info=True)

        BatchUtils.process_in_batches(rows, process_location, batch_size=500)
        self.log_import_event(module_name="netsuite_locations", fetched_records=len(rows))
        logger.info(f"Imported Locations: {len(rows)} records processed.")

    # ------------------------------------------------------------
    # Helper Methods
    # ------------------------------------------------------------
    def parse_date(self, date_str: Optional[str]) -> Optional[datetime.date]:
        if not date_str:
            return None
        try:
            return datetime.strptime(date_str, "%d/%m/%Y").date()
        except ValueError:
            logger.warning(f"Failed to parse date: {date_str}")
            return None

    def parse_datetime(self, datetime_str: Optional[str]) -> Optional[datetime]:
        if not datetime_str:
            return None
        formats = [
            "%d/%m/%Y %H:%M:%S",
            "%d/%m/%Y %H:%M:%S.%f",
            "%d/%m/%Y"
        ]
        for fmt in formats:
            try:
                dt_obj = datetime.strptime(datetime_str, fmt)
                return dt_obj.replace(tzinfo=tz.tzutc())
            except ValueError:
                continue
        try:
            dt_obj = dateutil_parse(datetime_str)
            return dt_obj.astimezone(tz.tzutc())
        except Exception as e:
            logger.warning(f"Failed to parse datetime with fallback: {datetime_str} - {e}")
            return None

    def get_quarter(self, month: Optional[int]) -> Optional[int]:
        if month is None:
            return None
        return (month - 1) // 3 + 1

    def make_aware_datetime(self, d) -> Optional[datetime]:
        if not d:
            return None
        if hasattr(d, 'tzinfo'):
            if d.tzinfo is None:
                return timezone.make_aware(d)
            return d
        try:
            dt_obj = datetime.strptime(d, "%Y-%m-%d")
            return timezone.make_aware(dt_obj)
        except Exception:
            return None

    def extract_yearperiod(self, postingperiod):
        if postingperiod:
            m = re.search(r'FY(\d+)', postingperiod)
            if m:
                try:
                    return int(m.group(1))
                except ValueError:
                    pass
        return None
