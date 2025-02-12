import logging
from django.db import transaction
from django.utils import timezone
from typing import Optional
from datetime import timezone as tz
from .client import NetSuiteClient
from integrations.models.models import Integration, Organisation, SyncTableLogs
from integrations.models.netsuite.analytics import (
    NetSuiteVendors,
    NetSuiteSubsidiaries,
    NetSuiteDepartments,
    NetSuiteEntity,
    NetSuiteAccountingPeriods,
    NetSuiteAccounts,
    NetSuiteTransactions,
    NetSuiteGeneralLedger,
    NetSuiteTransactionLine,
    NetSuiteTransactionAccountingLine,
)
from decimal import Decimal, InvalidOperation
from dateutil import tz
from dateutil.parser import parse as dateutil_parse
from datetime import datetime
import re

logger = logging.getLogger(__name__)


def bool_from_str(val: Optional[str]) -> bool:
    """Convert 'T'/'F' or similar strings to boolean."""
    if val is None:
        return False
    val_upper = val.strip().upper()
    return val_upper in ('T', 'TRUE', 'YES')


def decimal_or_none(value):
    try:
        return Decimal(value) if value is not None else None
    except (InvalidOperation, TypeError):
        return None


class NetSuiteImporter:
    """
    A class to handle importing various NetSuite data into Django models.
    """

    def __init__(self, integration: Integration, since_date: Optional[str] = None, until_date: Optional[str] = None):
        self.integration = integration
        self.consolidation_key = int(integration.netsuite_account_id)
        self.client = NetSuiteClient(self.consolidation_key, integration)
        self.org_name = integration.org
        self.now_ts = timezone.now()
        self.org = Organisation.objects.get(name=self.org_name)
        # since_date and until_date should be strings in the format "YYYY-MM-DD HH:MM:SS"
        self.since_date = since_date
        self.until_date = until_date

    def log_import_event(self, module_name: str, fetched_records: int):
        SyncTableLogs.objects.create(
            module_name=module_name,
            integration='NETSUITE',
            organization=self.integration.org,
            fetched_records=fetched_records,
            last_updated_time=timezone.now(),
            last_updated_date=timezone.now().date()
        )

    def build_date_clause(self, field: str, since: Optional[str] = None, until: Optional[str] = None) -> str:
        """
        Build a SuiteQL date filtering clause for a given field.
        Both since and until should be strings in the format "YYYY-MM-DD HH24:MI:SS".
        For example, if since is provided, returns:
            "AND {field} >= TO_DATE('{since}', 'YYYY-MM-DD HH24:MI:SS')"
        If until is provided, returns:
            "AND {field} <= TO_DATE('{until}', 'YYYY-MM-DD HH24:MI:SS')"
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
    @transaction.atomic
    def import_vendors(self, load_type="drop_and_reload"):
        """Imports NetSuite Vendors into NetSuiteVendors model."""
        logger.info("Importing NetSuite Vendors...")

        if load_type == "drop_and_reload":
            NetSuiteVendors.objects.filter(
                company_name=self.org_name
            ).delete()

        query = "SELECT * FROM Vendor"
        rows = list(self.client.execute_suiteql(query))

        for r in rows:
            try:
                vendor_id = r.get("id")
                if not vendor_id:
                    logger.warning(f"Vendor row missing 'id': {r}")
                    continue

                subsidiary = r.get("subsidiaryedition")  # Correct field mapping

                if not subsidiary:
                    logger.warning(f"Vendor row missing 'subsidiaryedition': {r}")
                    # Depending on your business logic, you might set a default value or skip
                    subsidiary = "Unknown"

                NetSuiteVendors.objects.update_or_create(
                    vendor_id=vendor_id,
                    defaults={
                        "company_name": self.org_name,
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
                logger.error(f"Error importing vendor row={r}: {e}", exc_info=True)

        self.log_import_event(module_name="netsuite_vendors", fetched_records=len(rows))
        logger.info(f"Imported {len(rows)} NetSuite Vendors (load_type={load_type}).")

    # ------------------------------------------------------------
    # 2) Import Subsidiaries
    # ------------------------------------------------------------
    @transaction.atomic
    def import_subsidiaries(self, load_type="drop_and_reload"):
        """Imports NetSuite Subsidiaries into NetSuiteSubsidiaries model."""
        logger.info("Importing NetSuite Subsidiaries...")

        if load_type == "drop_and_reload":
            NetSuiteSubsidiaries.objects.filter(
                company_name=self.org_name
            ).delete()

        query = """
        SELECT
            id,
            name,
            fullname,
            legalname,
            iselimination,
            currency,
            country,
            lastmodifieddate
        FROM subsidiary
        ORDER BY id
        """
        rows = list(self.client.execute_suiteql(query))

        for r in rows:
            try:
                sub_id = r.get("id")
                if not sub_id:
                    logger.warning(f"Subsidiary row missing 'id': {r}")
                    continue

                NetSuiteSubsidiaries.objects.update_or_create(
                    subsidiary_id=sub_id,
                    defaults={
                        "company_name": self.org_name,
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
                logger.error(f"Error importing subsidiary row={r}: {e}", exc_info=True)

        self.log_import_event(module_name="netsuite_subsidiaries", fetched_records=len(rows))
        logger.info(f"Imported {len(rows)} NetSuite Subsidiaries (load_type={load_type}).")


    # ------------------------------------------------------------
    # 3) Import Departments
    # ------------------------------------------------------------
    @transaction.atomic
    def import_departments(self, load_type="drop_and_reload"):
        """Imports NetSuite Departments into NetSuiteDepartments model."""
        logger.info("Importing NetSuite Departments...")

        if load_type == "drop_and_reload":
            NetSuiteDepartments.objects.filter(
                company_name=self.org_name
            ).delete()

        query = """
        SELECT
            id,
            name,
            fullname,
            subsidiary,
            isinactive
        FROM department
        ORDER BY id
        """
        rows = list(self.client.execute_suiteql(query))
        print(f"Importing {len(rows)} departments...")

        for r in rows:
            try:
                dept_id = r.get("id")
                if not dept_id:
                    logger.warning(f"Department row missing 'id': {r}")
                    continue

                NetSuiteDepartments.objects.update_or_create(
                    department_id=dept_id,
                    defaults={
                        "company_name": self.org_name,
                        "name": r.get("name"),
                        "full_name": r.get("fullname"),
                        "subsidiary": r.get("subsidiary"),
                        "is_inactive": bool_from_str(r.get("isinactive")),
                        "record_date": self.now_ts
                    }
                )
            except Exception as e:
                logger.error(f"Error importing department row={r}: {e}", exc_info=True)

        self.log_import_event(module_name="netsuite_departments", fetched_records=len(rows))
        logger.info(f"Imported {len(rows)} NetSuite Departments (load_type={load_type}).")


    # ------------------------------------------------------------
    # 4) Import Entities
    # ------------------------------------------------------------
    @transaction.atomic
    def import_entities(self, load_type="drop_and_reload"):
        """Imports NetSuite Entities into NetSuiteEntity model."""
        logger.info("Importing NetSuite Entities...")

        if load_type == "drop_and_reload":
            NetSuiteEntity.objects.filter(
                company_name=self.org_name
            ).delete()

        query = "SELECT * FROM entity"
        rows = list(self.client.execute_suiteql(query))

        # today = timezone.now().date()
        filtered_rows = [r for r in rows]
        print(f"Importing {len(filtered_rows)} entities, {filtered_rows[:20]}.")


        for r in filtered_rows:
            try:
                ent_id = r.get("entityid")
                if not ent_id:
                    logger.warning(f"Entity row missing 'id': {r}")
                    continue

                subsidiary = r.get("subsidiaryedition") or "Unknown" 

                # Update or create only if the entity exists
                NetSuiteEntity.objects.update_or_create(
                    entity_id=ent_id,
                    defaults={
                        "company_name": r.get("companyname") or self.org_name,
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
                        "record_date": self.now_ts
                    }
                )
            except Exception as e:
                logger.error(f"Error importing entity row={r}: {e}", exc_info=True)

        self.log_import_event(module_name="netsuite_entities", fetched_records=len(filtered_rows))
        logger.info(f"Imported {len(filtered_rows)} NetSuite Entities (load_type={load_type}).")


    # ------------------------------------------------------------
    # 5) Import Accounting Periods
    # ------------------------------------------------------------
    @transaction.atomic
    def import_accounting_periods(self, load_type="drop_and_reload"):
        """Imports NetSuite Accounting Periods into NetSuiteAccountingPeriods model."""
        logger.info("Importing NetSuite Accounting Periods...")

        if load_type == "drop_and_reload":
            NetSuiteAccountingPeriods.objects.filter(
                company_name=self.org_name
            ).delete()

        query = "SELECT * FROM accountingperiod"
        rows = list(self.client.execute_suiteql(query))

        for r in rows:
            try:
                period_id = r.get("id")
                if not period_id:
                    logger.warning(f"Accounting Period row missing 'id': {r}")
                    continue

                start_date = self.parse_date(r.get("startdate"))
                end_date = self.parse_date(r.get("enddate"))

                year = start_date.year if start_date else None
                quarter = self.get_quarter(start_date.month) if start_date else None
                period = start_date.month if start_date else None

                NetSuiteAccountingPeriods.objects.update_or_create(
                    period_id=period_id,
                    defaults={
                        "company_name": self.org_name,
                        "period_name": r.get("periodname"),
                        "start_date": start_date,
                        "end_date": end_date,
                        "closed": bool_from_str(r.get("closed")),
                        "all_locked": bool_from_str(r.get("alllocked")),
                        "fiscal_calendar": r.get("fiscalcalendar"),
                        "year": year,
                        "quarter": quarter,
                        "period": period,
                        "record_date": self.now_ts
                    }
                )
            except Exception as e:
                logger.error(f"Error importing accounting period row={r}: {e}", exc_info=True)

        self.log_import_event(module_name="netsuite_accounting_periods", fetched_records=len(rows))
        logger.info(f"Imported {len(rows)} NetSuite Accounting Periods (load_type={load_type}).")


    # ------------------------------------------------------------
    # 6) Import Accounts
    # ------------------------------------------------------------
    @transaction.atomic
    def import_accounts(self, load_type="drop_and_reload"):
        """
        Imports NetSuite Accounts into the NetSuiteAccounts model using pagination.
        The query and mapping are adjusted to the actual table columns.
        """
        logger.info("Importing NetSuite Accounts...")
        offset = 0
        limit = 1000
        total_imported = 0

        while True:
            query = f"""
            SELECT *
            FROM Account
            ORDER BY ID ASC
            OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY
            """
            rows = list(self.client.execute_suiteql(query))

            if not rows:
                break

            for r in rows:
                try:
                    account_id = r.get("id")
                    if not account_id:
                        logger.warning(f"Account row missing 'ID': {r}")
                        continue

                    NetSuiteAccounts.objects.update_or_create(
                        account_id=account_id,
                        defaults={
                            "company_name": self.org_name,
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
                        }
                    )
                except Exception as e:
                    logger.error(f"Error importing account row={r}: {e}", exc_info=True)

            total_imported += len(rows)
            offset += limit
            if len(rows) < limit:
                break
            logger.debug(f"Imported {len(rows)} accounts at offset {offset}.")


        self.log_import_event(module_name="netsuite_accounts", fetched_records=total_imported)
        logger.info(f"Imported {total_imported} NetSuite Accounts (load_type={load_type}).")


    # ------------------------------------------------------------
    # 7) Import Transactions
    # ------------------------------------------------------------
    @transaction.atomic
    def import_transactions(self, last_import_date: Optional[str] = None):
        """
        Imports NetSuite Transactions into the NetSuiteTransactions model.
        
        If last_import_date is provided (format: "YYYY-MM-DD HH24:MI:SS"), only records with
        LASTMODIFIEDDATE later than that value are returned. If not provided, then if self.since_date is set,
        that is used. (Otherwise, all records are imported.)
        
        Uses keyset pagination on LASTMODIFIEDDATE and ID.
        """
        logger.info("Importing NetSuite Transactions " +
                    ("incrementally..." if (last_import_date or self.since_date) else "(full import)..."))
        # Use parameter precedence: explicitly provided last_import_date overrides self.since_date.
        filter_since = last_import_date or self.since_date
        date_filter_clause = ""
        if filter_since:
            date_filter_clause = f"AND LASTMODIFIEDDATE > TO_DATE('{filter_since}', 'YYYY-MM-DD HH24:MI:SS')"
        limit = 500
        total_imported = 0
        marker = None  # marker is a tuple (marker_date_str, marker_id)

        while True:
            marker_clause = ""
            if marker:
                marker_clause = (
                    f"AND (LASTMODIFIEDDATE, ID) > (TO_DATE('{marker[0]}', 'YYYY-MM-DD HH24:MI:SS'), {marker[1]})"
                )
            query = f"""
                SELECT 
                    ID,
                    ABBREVTYPE,
                    APPROVALSTATUS,
                    BALSEGSTATUS,
                    BILLINGSTATUS,
                    CLOSEDATE,
                    CREATEDBY,
                    CREATEDDATE,
                    CURRENCY,
                    CUSTBODY_CASH_REGISTER,
                    CUSTBODY_NONDEDUCTIBLE_PROCESSED,
                    CUSTBODY_REPORT_TIMESTAMP,
                    CUSTOMTYPE,
                    DAYSOPEN,
                    DAYSOVERDUESEARCH,
                    DUEDATE,
                    ENTITY,
                    EXCHANGERATE,
                    EXTERNALID,
                    FOREIGNAMOUNTPAID,
                    FOREIGNAMOUNTUNPAID,
                    FOREIGNTOTAL,
                    NUMBER,
                    ISFINCHRG,
                    ISREVERSAL,
                    LASTMODIFIEDBY,
                    LASTMODIFIEDDATE,
                    NEXUS,
                    ORDPICKED,
                    PAYMENTHOLD,
                    POSTING,
                    POSTINGPERIOD,
                    PRINTEDPICKINGTICKET,
                    RECORDTYPE,
                    SOURCE,
                    STATUS,
                    TERMS,
                    TOBEPRINTED,
                    TRANDATE,
                    TRANDISPLAYNAME,
                    TRANID,
                    TRANSACTIONNUMBER,
                    TYPE,
                    USEREVENUEARRANGEMENT,
                    VISIBLETOCUSTOMER,
                    VOID,
                    VOIDED,
                    CUSTBODY_NEXUS_NOTC,
                    MEMO
                FROM Transaction
                WHERE 1=1
                    {date_filter_clause}
                    {marker_clause}
                ORDER BY LASTMODIFIEDDATE ASC, ID ASC
                FETCH NEXT {limit} ROWS ONLY
            """
            try:
                rows = list(self.client.execute_suiteql(query))
                logger.info(f"Fetched {len(rows)} transaction records.")
            except Exception as e:
                logger.error(f"Error importing transactions: {e}", exc_info=True)
                return

            if not rows:
                break

            for r in rows:
                try:
                    txn_id = r.get("id")
                    if not txn_id:
                        logger.warning(f"Transaction row missing 'ID': {r}")
                        continue

                    last_mod = self.parse_datetime(r.get("lastmodifieddate"))
                    if not last_mod:
                        logger.warning(f"Could not parse lastmodifieddate for txn {txn_id}")
                        continue

                    NetSuiteTransactions.objects.update_or_create(
                        transactionid=str(txn_id),
                        company_name=self.org,
                        defaults={
                            "links": r.get("links"),
                            "abbrevtype": r.get("abbrevtype"),
                            "approvalstatus": r.get("approvalstatus"),
                            "balsegstatus": r.get("balsegstatus"),
                            "billingstatus": r.get("billingstatus"),
                            "closedate": self.parse_date(r.get("closedate")),
                            "createdby": r.get("createdby"),
                            "createddate": self.parse_date(r.get("createddate")),
                            "currency": r.get("currency"),
                            "custbody5": r.get("custbody5"),
                            "custbody_cash_register": r.get("custbody_cash_register"),
                            "custbody_nondeductible_processed": r.get("custbody_nondeductible_processed"),
                            "custbody_report_timestamp": self.parse_datetime(r.get("custbody_report_timestamp")),
                            "custbody_wrong_subs": r.get("custbody_wrong_subs"),
                            "customtype": r.get("customtype"),
                            "daysopen": r.get("daysopen"),
                            "daysoverduesearch": r.get("daysoverduesearch"),
                            "duedate": self.parse_date(r.get("duedate")),
                            "entity": r.get("entity"),
                            "exchangerate": decimal_or_none(r.get("exchangerate")),
                            "externalid": r.get("externalid"),
                            "foreignamountpaid": decimal_or_none(r.get("foreignamountpaid")),
                            "foreignamountunpaid": decimal_or_none(r.get("foreignamountunpaid")),
                            "foreigntotal": decimal_or_none(r.get("foreigntotal")),
                            "number": decimal_or_none(r.get("number")),
                            "intercoadj": r.get("intercoadj"),
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
                            "subsidiary": r.get("subsidiary"),
                            "terms": r.get("terms"),
                            "tobeprinted": r.get("tobeprinted"),
                            "trandate": self.parse_date(r.get("trandate")),
                            "trandisplayname": r.get("trandisplayname"),
                            "tranid": r.get("tranid"),
                            "transactionnumber": r.get("transactionnumber"),
                            "type": r.get("type"),
                            "userevenuearrangement": r.get("userevenuearrangement"),
                            "visibletocustomer": r.get("visibletocustomer"),
                            "void_field": r.get("void"),
                            "voided": r.get("voided"),
                            "custbody_nexus_notc": r.get("custbody_nexus_notc"),
                            "memo": r.get("memo"),
                            "record_date": last_mod,
                        }
                    )
                except Exception as e:
                    logger.error(f"Error importing transaction row={r}: {e}", exc_info=True)

        self.log_import_event(module_name="netsuite_transactions", fetched_records=total_imported)
        logger.info(f"Imported {total_imported} Transaction records successfully.")

    # ------------------------------------------------------------
    # 8) Import General Ledger
    # ------------------------------------------------------------
    def make_aware_datetime(self, d):
        """
        Convert a date (or date-like) value 'd' into an aware datetime.
        Modify this helper to suit your date conversion.
        """
        if not d:
            return None
        # If d is already a datetime, assume it is naive and localize it:
        if hasattr(d, 'tzinfo'):
            if d.tzinfo is None:
                return timezone.make_aware(d)
            return d
        # Otherwise, assume it's a string and parse it:
        try:
            # Customize the format as needed:
            dt = timezone.datetime.strptime(d, "%Y-%m-%d")
            return timezone.make_aware(dt)
        except Exception:
            return None

    def extract_yearperiod(self, postingperiod):
        """
        Attempt to extract a fiscal year from a posting period string,
        e.g. "P5 - FY24" returns 24.
        Returns None if no match is found.
        """
        if postingperiod:
            m = re.search(r'FY(\d+)', postingperiod)
            if m:
                try:
                    return int(m.group(1))
                except ValueError:
                    pass
        return None

    @transaction.atomic
    def map_net_suite_general_ledger(self):
        """
        Map data from NetSuiteTransactions (header records) plus
        NetSuiteTransactionAccountingLine (detail lines) into NetSuiteGeneralLedger.
        
        For each transaction record:
          - All associated accounting lines are retrieved (joined on transactionid).
          - For each accounting line, we:
              * Convert the numeric 'account' to a string.
              * Look up the account in NetSuiteAccounts (if found, we use its acctnumber).
              * Derive a unique key and a lineid.
              * Map monetary fields and dates.
          - If a transaction has no associated accounting lines, a fallback GL entry is created.
          
        The unique constraint on (company_name, transactionid, linesequencenumber) is used for update_or_create.
        """

        logger.info("Mapping NetSuite General Ledger from Transactions + Accounting Lines...")
        total_mapped = 0

        # Retrieve all transactions (adjust your filter as needed)
        transactions = NetSuiteTransactions.objects.all()

        for txn in transactions:
            
            # For each transaction, find all corresponding accounting lines.
            lines = NetSuiteTransactionAccountingLine.objects.filter(transaction=txn.transactionid)
            print(f"lines: {lines}, txn: {txn}, txn.transactionid: {txn.transactionid}")
            if lines.exists():
                for line in lines:
                    try:
                        # Use the accounting line's numeric account field as a string
                        account_str = str(line.account) if line.account is not None else None
                        account_obj = None
                        if account_str:
                            try:
                                account_obj = NetSuiteAccounts.objects.get(account_id=account_str)
                            except NetSuiteAccounts.DoesNotExist:
                                pass

                        # Decide the GL account value (here we store the string representation)
                        gl_account = account_str

                        # For acctnumber, try to use the account object's acctnumber if found
                        acct_number = account_obj.acctnumber if (account_obj and account_obj.acctnumber) else account_str

                        # Derive a unique key for the GL entry.
                        # If the accounting line provides a consolidation_key, use it; otherwise, build one.
                        unique_key = (line.consolidation_key.strip() 
                                      if line.consolidation_key else 
                                      f"{txn.transactionid}-{line.transaction_line}")

                        # Compute the GL line id; here we concatenate transaction id and the line's sequence.
                        line_id = f"{txn.transactionid}-{line.transaction_line}" if line.transaction_line is not None else f"{txn.transactionid}-0"

                        # Use the transaction line field for the GL's line sequence number.
                        line_seq = int(line.transaction_line) if line.transaction_line is not None else 0

                        # Derive yearperiod from txn.postingperiod if possible.
                        yearperiod = self.extract_yearperiod(txn.postingperiod)

                        # For the record_date, choose the accounting line's lastmodifieddate if present,
                        # otherwise fall back to the transaction's record_date.
                        rec_date = self.make_aware_datetime(line.lastmodifieddate) or self.make_aware_datetime(txn.record_date)

                        # Build the defaults dictionary for the GL entry.
                        defaults = {
                            'abbrevtype':     txn.abbrevtype,
                            'approvalstatus': txn.approvalstatus,
                            'postingperiod':  txn.postingperiod,
                            'yearperiod':     yearperiod,
                            'trandate':       self.make_aware_datetime(txn.trandate),
                            'subsidiary':     txn.subsidiary,
                            'account':        gl_account,
                            'acctnumber':     acct_number,
                            'amount':         line.amount,
                            'debit':          line.debit,
                            'credit':         line.credit,
                            'netamount':      line.netamount,
                            'currency':       txn.currency,
                            'exchangerate':   txn.exchangerate,
                            'record_date':    rec_date,
                        }

                        # Use update_or_create with unique constraint fields.
                        gl_obj, created = NetSuiteGeneralLedger.objects.update_or_create(
                            company_name=txn.company_name,
                            transactionid=txn.transactionid,
                            linesequencenumber=line_seq,
                            defaults={ **defaults,
                                       'uniquekey': unique_key,
                                       'lineid': line_id }
                        )
                        total_mapped += 1
                        if created:
                            logger.info(f"Created GL entry for Txn={txn.transactionid}, line_seq={line_seq}")
                        else:
                            logger.info(f"Updated GL entry for Txn={txn.transactionid}, line_seq={line_seq}")
                    except Exception as e:
                        logger.error(f"Error mapping Txn={txn.transactionid}, line={line.transaction_line}: {e}", exc_info=True)
            # else:
            #     # Create a fallback GL entry if no accounting lines exist.
            #     try:
            #         yearperiod = self.extract_yearperiod(txn.postingperiod)
            #         defaults = {
            #             'abbrevtype':     txn.abbrevtype,
            #             'approvalstatus': txn.approvalstatus,
            #             'postingperiod':  txn.postingperiod,
            #             'yearperiod':     yearperiod,
            #             'trandate':       self.make_aware_datetime(txn.trandate),
            #             'subsidiary':     txn.subsidiary,
            #             'account':        None,
            #             'acctnumber':     None,
            #             'amount':         None,
            #             'debit':          None,
            #             'credit':         None,
            #             'netamount':      None,
            #             'currency':       txn.currency,
            #             'exchangerate':   txn.exchangerate,
            #             'record_date':    self.make_aware_datetime(txn.record_date),
            #         }
            #         gl_obj, created = NetSuiteGeneralLedger.objects.update_or_create(
            #             company_name=txn.company_name,
            #             transactionid=txn.transactionid,
            #             linesequencenumber=0,  # fallback line sequence number
            #             defaults={ **defaults,
            #                        'uniquekey': f"{txn.transactionid}-0",
            #                        'lineid': f"{txn.transactionid}-0" }
            #         )
            #         total_mapped += 1
            #         if created:
            #             logger.info(f"Created fallback GL entry for Txn={txn.transactionid} (no lines)")
            #         else:
            #             logger.info(f"Updated fallback GL entry for Txn={txn.transactionid} (no lines)")
            #     except Exception as e:
            #         logger.error(f"Error mapping fallback GL for Txn={txn.transactionid}: {e}", exc_info=True)

        self.log_import_event(module_name="netsuite_general_ledger", fetched_records=total_mapped)
        logger.info(f"Completed mapping general ledger: {total_mapped} entries processed.")

    # ------------------------------------------------------------
    # 9) Import Transaction Lines
    # ------------------------------------------------------------
    @transaction.atomic
    def import_transaction_lines(self, min_id: Optional[str] = None, last_modified_after: Optional[str] = None,
                                 start_date: Optional[str] = None, end_date: Optional[str] = None):
        """
        Imports NetSuite Transaction Lines into the NetSuiteTransactionLine model by dynamically
        paging through the results in batches. Optional date filtering is applied directly in SuiteQL.
        
        If no date filters are provided, the import is run on all records.
        """
        logger.info("Importing NetSuite Transaction Lines...")
        batch_size = 500
        if not min_id:
            min_id = "0"
        # If no explicit start_date is provided, use self.since_date if available.
        # if not start_date:
        #     start_date = self.since_date
        # total_fetched = 0

        # # Build date filter clause based on provided parameters.
        # date_filter_clause = self.build_date_clause("LINELASTMODIFIEDDATE", since=last_modified_after or start_date, until=end_date)

        while True:
            query = f"""
                SELECT
                    id,
                    ISBILLABLE,
                    ISCLOSED,
                    ISCOGS,
                    ISCUSTOMGLLINE,
                    ISFULLYSHIPPED,
                    ISFXVARIANCE,
                    ISINVENTORYAFFECTING,
                    ISREVRECTRANSACTION,
                    LINELASTMODIFIEDDATE,
                    LINESEQUENCENUMBER,
                    LOCATION,
                    MAINLINE,
                    MEMO,
                    NETAMOUNT,
                    OLDCOMMITMENTFIRM,
                    QUANTITYBILLED,
                    QUANTITYREJECTED,
                    QUANTITYSHIPRECV,
                    SUBSIDIARY,
                    TAXLINE,
                    TRANSACTIONDISCOUNT
                FROM TransactionLine
                WHERE id > {min_id}
                ORDER BY id ASC
                FETCH NEXT {batch_size} ROWS ONLY
            """
            try:
                rows = list(self.client.execute_suiteql(query))
                logger.info(f"Fetched {len(rows)} transaction line records with id > {min_id}.")
            except Exception as e:
                logger.error(f"Error importing transaction lines: {e}", exc_info=True)
                return

            if not rows:
                break

            for r in rows:
                print(f"row: {r}")
                try:
                    netsuite_id = r.get("id")
                    if not netsuite_id:
                        logger.warning(f"Transaction Line row missing 'id': {r}")
                        continue

                    last_modified = self.parse_datetime(r.get("linelastmodifieddate"))
                    NetSuiteTransactionLine.objects.update_or_create(
                        id=netsuite_id,
                        defaults={
                            "company_name": self.org_name,
                            "is_billable": r.get("isbillable"),
                            "is_closed": r.get("isclosed"),
                            "is_cogs": r.get("iscogs"),
                            "is_custom_gl_line": r.get("iscustomglline"),
                            "is_fully_shipped": r.get("isfullyshipped"),
                            "is_fx_variance": r.get("isfxvariance"),
                            "is_inventory_affecting": r.get("isinventoryaffecting"),
                            "is_rev_rec_transaction": r.get("isrevrectransaction"),
                            "line_last_modified_date": last_modified.date() if last_modified else None,
                            "line_sequence_number": r.get("linesequencenumber"),
                            "links": r.get("links"),
                            "location": r.get("location"),
                            "main_line": r.get("mainline"),
                            "match_bill_to_receipt": r.get("matchbilltoreceipt"),
                            "memo": r.get("memo"),
                            "net_amount": decimal_or_none(r.get("netamount")),
                            "old_commitment_firm": r.get("oldcommitmentfirm"),
                            "quantity_billed": r.get("quantitybilled"),
                            "quantity_rejected": r.get("quantityrejected"),
                            "quantity_ship_recv": r.get("quantityshiprecv"),
                            "source_uri": r.get("source_uri"),
                            "subsidiary": r.get("subsidiary"),
                            "subsidiary_id": r.get("subsidiaryid"),
                            "tax_line": r.get("taxline"),
                            "transaction_discount": r.get("transactiondiscount"),
                            "transaction_id": r.get("transactionid"),
                        }
                    )
                except Exception as e:
                    logger.error(f"Error importing transaction line row={r}: {e}", exc_info=True)

            total_fetched += len(rows)
            min_id = rows[-1].get("id")
            logger.info(f"Processed batch. New min_id set to {min_id}. Total imported so far: {total_fetched}.")
            if len(rows) < batch_size:
                break

        self.log_import_event(module_name="netsuite_transaction_lines", fetched_records=total_fetched)
        logger.info("Transaction Line import complete.")



    # ------------------------------------------------------------
    # 10) Transaction Accounting Lines
    # ------------------------------------------------------------
    @transaction.atomic
    def import_transaction_accounting_lines(self, min_id: Optional[str] = None,
                                            last_modified_after: Optional[str] = None,
                                            start_date: Optional[str] = None,
                                            end_date: Optional[str] = None):
        """
        Imports NetSuite Transaction Accounting Lines into the NetSuiteTransactionAccountingLine model using keyset pagination.
        
        You can optionally restrict the imported rows by a date filter on LASTMODIFIEDDATE.
        
        To import records modified since December, pass, for example:
            last_modified_after="2024-12-01 00:00:00"
        or pass a start_date (and optionally an end_date).
        
        Date strings must be in the format "YYYY-MM-DD HH24:MI:SS".
        """
        logger.info("Importing Transaction Accounting Lines...")
        if not min_id:
            min_id = "0"
        
        limit = 500
        total_imported = 0
        start_date = "2025-01-01"

        # Build date filtering clause.
        # Here we give precedence to last_modified_after if provided; otherwise, we use start_date/end_date.
        date_filter_clause = ""
        if last_modified_after:
            date_filter_clause += f" AND LASTMODIFIEDDATE > TO_DATE('{last_modified_after}', 'YYYY-MM-DD HH24:MI:SS')"
        else:
            if start_date:
                date_filter_clause += f" AND LASTMODIFIEDDATE >= TO_DATE('{start_date}', 'YYYY-MM-DD HH24:MI:SS')"
            if end_date:
                date_filter_clause += f" AND LASTMODIFIEDDATE <= TO_DATE('{end_date}', 'YYYY-MM-DD HH24:MI:SS')"

        while True:
            query = f"""
                SELECT
                    TRANSACTION,
                    TRANSACTIONLINE,
                    ACCOUNT,
                    ACCOUNTINGBOOK,
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
                FROM TransactionAccountingLine L
                WHERE L.TRANSACTION > {min_id}
                    {date_filter_clause}
                ORDER BY L.TRANSACTION ASC
                FETCH NEXT {limit} ROWS ONLY
            """
            try:
                rows = list(self.client.execute_suiteql(query))
                for i in range(3):
                    print(f"row {i}: {rows[i]}")
                print(f"fetched rows: {len(rows)} on page {min_id} {date_filter_clause}")
                logger.info(f"Fetched {len(rows)} transaction accounting line records with TRANSACTION > {min_id}{date_filter_clause}.")
            except Exception as e:
                logger.error(f"Error importing transaction accounting lines: {e}", exc_info=True)
                return

            if not rows:
                break

            for r in rows:
                try:
                    last_modified = self.parse_datetime(r.get("lastmodifieddate"))
                    NetSuiteTransactionAccountingLine.objects.update_or_create(
                        org=self.org,
                        transaction=r.get("transaction"),
                        transaction_line=r.get("transactionline"),
                        defaults={
                            "links": r.get("links"),
                            "account": r.get("account"),
                            "accountingbook": r.get("accountingbook"),
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
                        }
                    )
                except Exception as e:
                    logger.error(f"Error importing transaction accounting line row: {r} - {e}", exc_info=True)

            total_imported += len(rows)
            # Update min_id using the maximum TRANSACTION value from the current batch.
            max_transaction = max(r.get("transaction") for r in rows)
            min_id = str(max_transaction)
            logger.info(f"Batch processed. New min_id set to {min_id}. Total imported so far: {total_imported}.")
            if len(rows) < limit or total_imported > 5000:
                break

        self.log_import_event(module_name="netsuite_transaction_accounting_lines", fetched_records=total_imported)
        logger.info(f"Imported {total_imported} Transaction Accounting Lines successfully.")


    # ------------------------------------------------------------
    # 11) Transform Transaction
    # ------------------------------------------------------------
    @transaction.atomic
    def transform_transactions(self):
        """
        Transforms imported NetSuite data into a unified transformed transaction record.
        For each accounting line record (AL) from NetSuiteTransactionAccountingLine:
          1. Look up the corresponding transaction header (T) from NetSuiteTransactions using AL.transaction.
          2. Look up the corresponding transaction line (L) from NetSuiteTransactionLine using AL.transaction_line.
             (If not found, skip this accounting line.)
          3. Look up the account record from NetSuiteAccounts using AL.account.
          4. Look up subsidiary info from NetSuiteSubsidiaries using L.subsidiary.
          5. Compute derived fields (such as YEARPERIOD from T.postingperiod).
          6. Combine header, line, and accounting line fields into a single record and save it.
        No fallback entry is created if a matching transaction line is missing.
        """
        logger.info("Starting transformation of NetSuite transactions...")
        total_transformed = 0
        from integrations.models.netsuite.analytics import NetSuiteTransformedTransaction

        # Iterate over all accounting line records.
        accounting_lines = NetSuiteTransactionAccountingLine.objects.filter(org=self.org).order_by("transaction", "transaction_line")
        for al in accounting_lines:
            try:
                # Retrieve the header record.
                try:
                    txn = NetSuiteTransactions.objects.get(transactionid=al.transaction, company_name=self.org)
                except NetSuiteTransactions.DoesNotExist:
                    logger.warning(f"Transaction {al.transaction} not found; skipping AL record {al.pk}.")
                    continue

                # Retrieve the transaction line record by its primary key equal to al.transaction_line.
                try:
                    tline = NetSuiteTransactionLine.objects.get(id=al.transaction_line, company_name=self.org)
                except NetSuiteTransactionLine.DoesNotExist:
                    logger.warning(f"Transaction line {al.transaction_line} not found for transaction {al.transaction}; skipping.")
                    continue

                # Lookup account info.
                account_str = str(al.account) if al.account is not None else None
                account_obj = None
                if account_str:
                    try:
                        account_obj = NetSuiteAccounts.objects.get(account_id=account_str, company_name=self.org)
                    except NetSuiteAccounts.DoesNotExist:
                        logger.debug(f"Account {account_str} not found.")

                # Lookup subsidiary info from the transaction line.
                subsidiary_obj = None
                if tline.subsidiary:
                    try:
                        subsidiary_obj = NetSuiteSubsidiaries.objects.get(subsidiary_id=tline.subsidiary, company_name=self.org)
                    except NetSuiteSubsidiaries.DoesNotExist:
                        logger.debug(f"Subsidiary {tline.subsidiary} not found.")

                # Derive YEARPERIOD from the transaction postingperiod.
                yearperiod = self.extract_yearperiod(txn.postingperiod or "")

                # Build the transformed data dictionary.
                transformed_data = {
                    # Header fields from transaction (T)
                    "company_name": txn.company_name,
                    "consolidation_key": self.consolidation_key,
                    "transactionid": txn.transactionid,
                    "abbrevtype": txn.abbrevtype,
                    "approvalstatus": txn.approvalstatus,
                    "number": txn.number,
                    "source": txn.source,
                    "status": txn.status,
                    "trandisplayname": txn.trandisplayname,
                    "tranid": txn.tranid,
                    "transactionnumber": txn.transactionnumber,
                    "type": txn.type,
                    "recordtype": txn.recordtype,
                    "createdby": txn.createdby,
                    "createddate": txn.createddate,
                    "lastmodifiedby": txn.lastmodifiedby,
                    "lastmodifieddate": txn.lastmodifieddate,
                    "postingperiod": txn.postingperiod,
                    "yearperiod": yearperiod,
                    "trandate": txn.trandate,
                    
                    # Subsidiary info
                    "subsidiary": subsidiary_obj.name if subsidiary_obj else txn.subsidiary,
                    "subsidiaryfullname": subsidiary_obj.full_name if subsidiary_obj else None,
                    "subsidiaryid": tline.subsidiary,  # from transaction line field
                    
                    # Department fields (if available on transaction line; adjust attribute names if needed)
                    "department": getattr(tline, "department", None),
                    "departmentid": getattr(tline, "departmentid", None),
                    
                    # From transaction line (L)
                    "linesequencenumber": tline.line_sequence_number,
                    "lineid": str(tline.id),
                    "location": tline.location,
                    "clas": getattr(tline, "class_field", None),
                    "linenmemo": tline.memo,
                    
                    # Common header/line fields
                    "memo": txn.memo,
                    "externalid": txn.externalid,
                    "entity": getattr(tline, "entity", None),
                    "entityid": getattr(tline, "entityid", None),
                    "terms": txn.terms,
                    "daysopen": txn.daysopen,
                    "daysoverduesearch": txn.daysoverduesearch,
                    "duedate": txn.duedate,
                    "closedate": txn.closedate,
                    
                    # From accounting line (AL)
                    "accountingbook": al.accountingbook,
                    "amount": al.amount,
                    "amountlinked": al.amountlinked,
                    "debit": al.debit,
                    "credit": al.credit,
                    "netamount": al.netamount,
                    "paymentamountunused": al.paymentamountunused,
                    "paymentamountused": al.paymentamountused,
                    "posting_field": al.posting,
                    "amountpaid": al.amountpaid,
                    "amountunpaid": al.amountunpaid,
                    
                    # From transaction line (if present, for line-level netamount)
                    "linenetamount": tline.net_amount,
                    
                    # From account lookup (A)
                    "account": account_str,
                    "acctnumber": account_obj.acctnumber if account_obj else None,
                    "accountsearchdisplayname": account_obj.accountsearchdisplayname if account_obj else None,
                    "accttype": account_obj.accttype if account_obj else None,
                    "displaynamewithhierarchy": account_obj.displaynamewithhierarchy if account_obj else None,
                    "fullname": account_obj.fullname if account_obj else None,
                    "sspecacct": account_obj.sspecacct if account_obj else None,
                    
                    # Additional fields from transaction header (if any)
                    "billingstatus": txn.billingstatus,
                    "custbody_report_timestamp": txn.custbody_report_timestamp,
                    "currency": txn.currency,
                    "exchangerate": Decimal(txn.exchangerate) if txn.exchangerate else None,
                    "foreignamountpaid": Decimal(txn.foreignamountpaid) if txn.foreignamountpaid else None,
                    "foreignamountunpaid": Decimal(txn.foreignamountunpaid) if txn.foreignamountunpaid else None,
                    "foreigntotal": Decimal(txn.foreigntotal) if txn.foreigntotal else None,
                    # If transaction line has a field for foreignlineamount:
                    "foreignlineamount": getattr(tline, "foreignlineamount", None),
                    "record_date": txn.record_date,
                    
                    # # Build a unique key based on transaction and line.
                    # # (You may adjust this formula if needed.)
                    # # Note: We assume tline.line_sequence_number is unique per transaction.
                    # "uniquekey": f"{txn.transactionid}-{tline.line_sequence_number}",
                }

                # Create (or update) the transformed record. The unique key is based on
                # (company_name, transactionid, linesequencenumber).
                NetSuiteTransformedTransaction.objects.update_or_create(
                    company_name=txn.company_name,
                    transactionid=txn.transactionid,
                    linesequencenumber=tline.line_sequence_number,
                    defaults=transformed_data
                )
                total_transformed += 1

            except Exception as e:
                logger.error(f"Error transforming transaction {txn.transactionid} for accounting line {al.pk}: {e}", exc_info=True)
                continue

        logger.info(f"Transformation complete: {total_transformed} entries processed.")
        self.log_import_event(module_name="netsuite_transformed_transaction", fetched_records=total_transformed)
    
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
                dt = datetime.strptime(datetime_str, fmt)
                return dt.replace(tzinfo=tz.tzutc())
            except ValueError:
                continue
        try:
            dt = dateutil_parse(datetime_str)
            return dt.astimezone(tz.tzutc())
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
            dt = datetime.strptime(d, "%Y-%m-%d")
            return timezone.make_aware(dt)
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
