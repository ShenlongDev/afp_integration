import logging
from django.db import transaction
from django.utils import timezone
from typing import Optional
from datetime import timezone as tz
from .client import NetSuiteClient
from integrations.models.models import Integration, Organisation
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

    def __init__(self, integration: Integration):
        self.integration = integration
        self.consolidation_key = integration.netsuite_account_id
        self.client = NetSuiteClient(self.consolidation_key, integration)
        self.org_name = integration.org
        self.now_ts = timezone.now()
        self.org = Organisation.objects.get(name=self.org_name)

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

        today = timezone.now().date()
        filtered_rows = [r for r in rows if self.parse_datetime(r.get("lastmodifieddate")).date() == today]


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

        while offset < 10000:
            query = f"""
            SELECT *
            FROM Account
            ORDER BY ID ASC
            OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY
            """
            rows = list(self.client.execute_suiteql(query))
            print(f"Importing {len(rows)} accounts at offset {offset}.")
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
                            "acctnumber": r.get("ACCTNUMBER"),
                            "accountsearchdisplaynamecopy": r.get("ACCOUNTSEARCHDISPLAYNAMECOPY"),
                            "fullname": r.get("FULLNAME"),
                            "accountsearchdisplayname": r.get("ACCOUNTSEARCHDISPLAYNAME"),
                            "displaynamewithhierarchy": r.get("DISPLAYNAMEWITHHIERARCHY"),
                            "parent": r.get("PARENT"),
                            "accttype": r.get("ACCTTYPE"),
                            "sspecacct": r.get("SSPECACCT"),
                            "description": r.get("DESCRIPTION"),
                            "eliminate": bool_from_str(r.get("ELIMINATE")),
                            "externalid": r.get("EXTERNALID"),
                            "include_children": bool_from_str(r.get("INCLUDECHILDREN")),
                            "inventory": bool_from_str(r.get("INVENTORY")),
                            "is_inactive": bool_from_str(r.get("ISINACTIVE")),
                            "is_summary": bool_from_str(r.get("ISSUMMARY")),
                            "last_modified_date": self.parse_datetime(r.get("LASTMODIFIEDDATE")),
                            "reconcile_with_matching": bool_from_str(r.get("RECONCILEWITHMATCHING")),
                            "revalue": bool_from_str(r.get("REVALUE")),
                            "subsidiary": r.get("SUBSIDIARY"),
                            "balance": decimal_or_none(r.get("BALANCE")),
                            "record_date": self.now_ts,
                        }
                    )
                except Exception as e:
                    logger.error(f"Error importing account row={r}: {e}", exc_info=True)

            total_imported += len(rows)
            offset += limit
            logger.debug(f"Imported {len(rows)} accounts at offset {offset}.")

        logger.info(f"Imported {total_imported} NetSuite Accounts (load_type={load_type}).")

    # ------------------------------------------------------------
    # 7) Import Transactions
    # ------------------------------------------------------------
    @transaction.atomic
    def import_transactions(self, min_id: Optional[str] = None):
        """
        Imports NetSuite Transactions into the NetSuiteTransactions model incrementally.
        Uses the table structure (with columns such as LINKS, ABBREVTYPE, etc.) as found
        in NetSuite.
        """
        logger.info("Importing NetSuite Transactions incrementally...")

        if not min_id:
            min_id = "0"

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
        WHERE ID > {min_id}
        ORDER BY ID ASC
        """
        rows = list(self.client.execute_suiteql(query))
        today = timezone.now().date()
        filtered_rows = [
            r for r in rows 
            # if self.parse_datetime(r.get("LASTMODIFIEDDATE")) and 
            # self.parse_datetime(r.get("LASTMODIFIEDDATE")).date() == today
        ]

        logger.info(f"Fetched {len(rows)} transactions, importing {len(filtered_rows)} modified today.")
        for r in filtered_rows:
            try:
                transaction_id = r.get("id")
                if not transaction_id:
                    logger.warning(f"Transaction row missing 'ID': {r}")
                    continue

                last_modified = self.parse_datetime(r.get("lastmodifieddate"))

                NetSuiteTransactions.objects.update_or_create(
                    transactionid=str(transaction_id),
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
                        "lastmodifieddate": last_modified,
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
                        "record_date": last_modified,
                    }
                )
            except Exception as e:
                logger.error(f"Error importing transaction row={r}: {e}", exc_info=True)

        logger.info(f"Imported {len(filtered_rows)} NetSuite Transactions (min_id={min_id}).")

    # ------------------------------------------------------------
    # 8) Import General Ledger
    # ------------------------------------------------------------
    @transaction.atomic
    def map_net_suite_general_ledger(self):
        """
        Map data from NetSuiteTransactions (header records) plus 
        NetSuiteTransactionAccountingLine (detail lines) into NetSuiteGeneralLedger.

        Because the NetSuiteTransactions model doesn't have an 'account' or 
        'acctnumber', we rely on line-level data from NetSuiteTransactionAccountingLine
        for debits, credits, accounts, etc.

        Steps:
        1) For each transaction:
            a) Retrieve all accounting lines from NetSuiteTransactionAccountingLine 
                where transaction=transactionid.
            b) For each line:
                - Convert line.account (BigInteger) to a string
                - If that exists in NetSuiteAccounts.account_id, fetch the account record 
                for additional details (like .acctnumber or .fullname).
                - Insert/Update a GL entry with line's monetary values.
            c) If no lines, optionally create a fallback GL entry with empty monetary fields.
        """
        logger.info("Mapping NetSuite General Ledger from Transactions + Accounting Lines...")
        total_mapped = 0

        # Retrieve all transactions
        transactions = NetSuiteTransactions.objects.all()

        for txn in transactions:
            # All lines that belong to this transaction
            lines = NetSuiteTransactionAccountingLine.objects.filter(transaction=txn.transactionid)

            if lines.exists():
                # -- Create/Update a GL record for each line
                for line in lines:
                    print(f"Processing line: {line.account}")
                    try:
                        # Convert numeric 'account' to string to match NetSuiteAccounts.account_id
                        account_str = str(line.account) if line.account is not None else None

                        # If we can find a matching account record, fetch it
                        account_obj = None
                        if account_str:
                            try:
                                account_obj = NetSuiteAccounts.objects.get(account_id=account_str)
                            except NetSuiteAccounts.DoesNotExist:
                                pass

                        # Decide what to store in the GL's "account" field
                        # - If you want to store the actual numeric ID, use account_str
                        # - If you prefer the 'account_id' from account_obj, also account_str (same thing)
                        # - If you want a more descriptive field, e.g. 'acctnumber' from the account record, see below
                        gl_account = account_str

                        # For the GL 'acctnumber' field, you could store the
                        # NetSuiteAccounts.acctnumber or the account_id. Choose whichever is meaningful.
                        acct_number = account_obj.acctnumber if (account_obj and account_obj.acctnumber) else account_str

                        # Monetary fields come directly from line-level data
                        defaults = {
                            'abbrevtype':        txn.abbrevtype,
                            'approvalstatus':    txn.approvalstatus,
                            'postingperiod':     txn.postingperiod,
                            # net date => if your transaction date is a DateField, 
                            # Django will convert automatically to DateTime for the GL.
                            'trandate':          txn.trandate,  
                            'subsidiary':        txn.subsidiary,

                            # The 'account' field in NetSuiteGeneralLedger is a CharField
                            'account':           gl_account,
                            'acctnumber':        acct_number,

                            'amount':            line.amount,
                            'debit':             line.debit,
                            'credit':            line.credit,
                            'netamount':         line.netamount,

                            # Pull currency and exchange rate from the transaction header
                            'currency':          txn.currency,
                            'exchangerate':      txn.exchangerate,

                            # record_date => prefer line.lastmodifieddate if present
                            'record_date':       line.lastmodifieddate if line.lastmodifieddate else txn.record_date,
                        }

                        # Use the line's transaction_line as the 'linesequencenumber'
                        line_seq = line.transaction_line if line.transaction_line else 0

                        # Insert/update the GL record
                        gl_obj, created = NetSuiteGeneralLedger.objects.update_or_create(
                            company_name=txn.company_name,
                            transactionid=txn.transactionid,
                            linesequencenumber=line_seq,
                            defaults=defaults
                        )
                        total_mapped += 1

                        if created:
                            logger.info(
                                f"Created GL entry: Txn={txn.transactionid}, line_seq={line_seq}"
                            )
                        else:
                            logger.info(
                                f"Updated GL entry: Txn={txn.transactionid}, line_seq={line_seq}"
                            )
                    except Exception as e:
                        logger.error(
                            f"Error mapping Txn={txn.transactionid}, line={line.transaction_line}: {e}",
                            exc_info=True
                        )

            else:
                # -- If no lines exist, create one fallback GL entry (optional)
                try:
                    # Because there's no line-level info, we can't get account from the transaction
                    # => store them as None or your chosen default
                    defaults = {
                        'abbrevtype':     txn.abbrevtype,
                        'approvalstatus': txn.approvalstatus,
                        'postingperiod':  txn.postingperiod,
                        'trandate':       txn.trandate,
                        'subsidiary':     txn.subsidiary,

                        'account':        None,
                        'acctnumber':     None,

                        'amount':         None,
                        'debit':          None,
                        'credit':         None,
                        'netamount':      None,

                        'currency':       txn.currency,
                        'exchangerate':   txn.exchangerate,
                        'record_date':    txn.record_date,
                    }

                    gl_obj, created = NetSuiteGeneralLedger.objects.update_or_create(
                        company_name=txn.company_name,
                        transactionid=txn.transactionid,
                        linesequencenumber=0,  # fallback line number
                        defaults=defaults
                    )
                    total_mapped += 1

                    if created:
                        logger.info(
                            f"Created fallback GL entry: Txn={txn.transactionid} (no lines)"
                        )
                    else:
                        logger.info(
                            f"Updated fallback GL entry: Txn={txn.transactionid} (no lines)"
                        )
                except Exception as e:
                    logger.error(
                        f"Error mapping fallback GL for Txn={txn.transactionid}: {e}",
                        exc_info=True
                    )

        logger.info(f"Completed mapping general ledger: {total_mapped} entries processed.")


    # ------------------------------------------------------------
    # 9) Import Transaction Lines
    # ------------------------------------------------------------
    @transaction.atomic
    def import_transaction_lines(self, min_id: Optional[str] = None):
        """
        Imports NetSuite Transaction Lines into the NetSuiteTransactionLine model.
        """
        logger.info("Importing NetSuite Transaction Lines...")
        if not min_id:
            min_id = "0"

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
        OFFSET 500 ROWS FETCH NEXT 500 ROWS ONLY
        """
        try:
            rows = list(self.client.execute_suiteql(query))
            logger.info(f"Fetched {len(rows)} transaction line records.")
        except Exception as e:
            logger.error(f"Error importing transaction_lines: {e}", exc_info=True)
            return

        for r in rows:
            try:
                netsuite_id = r.get("id")
                if not netsuite_id:
                    logger.warning(f"Transaction Line row missing 'id': {r}")
                    continue

                # Parse the LINELASTMODIFIEDDATE; assuming parse_datetime returns a datetime.
                last_modified = self.parse_datetime(r.get("LINELASTMODIFIEDDATE"))

                NetSuiteTransactionLine.objects.update_or_create(
                    netsuite_id=netsuite_id,
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

        logger.info("Transaction Line import complete.")


    # ------------------------------------------------------------
    # 10) Transaction Accounting Lines
    # ------------------------------------------------------------
    @transaction.atomic
    def import_transaction_accounting_lines(self, min_id: Optional[str] = None):
        """
        Imports NetSuite Transaction Accounting Lines (from the TransactionAccountingLine table)
        into the NetSuiteTransactionAccountingLine model using keyset pagination to avoid offset issues.
        """
        logger.info("Importing Transaction Accounting Lines...")
        if not min_id:
            min_id = "0"
        
        limit = 500
        offset = 500
        total_imported = 0

        while offset < 2000:
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
            WHERE L.Transaction > {min_id}
            ORDER BY L.Transaction ASC
            OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY
            """
            rows = list(self.client.execute_suiteql(query))
            logger.info(f"Fetched {len(rows)} transaction accounting line records with min_id > {min_id}.")

            if not rows:
                break

            for r in rows:
                try:
                    last_modified = self.parse_datetime(r.get("LASTMODIFIEDDATE"))
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
            offset += limit
        
        logger.info(f"Imported {total_imported} Transaction Accounting Lines successfully.")




    # 9) Import Budgets
    # ------------------------------------------------------------
    # @transaction.atomic
    # def import_budgets(self, load_type="drop_and_reload"):
    #     """Imports NetSuite Budgets into NetSuiteBudgetPeriodBalances model."""
    #     logger.info("Importing NetSuite Budgets...")

    #     if load_type == "drop_and_reload":
    #         NetSuiteBudgetPeriodBalances.objects.filter(
    #             company_name=self.org_name
    #         ).delete()

    #     # Import Budget
    #     budget_query = "SELECT * FROM Budget"
    #     budget_rows = list(self.client.execute_suiteql(budget_query))

    #     for r in budget_rows:
    #         try:
    #             NetSuiteBudgetPeriodBalances.objects.create(
    #                 company_name=self.org_name,
    #                 budget_id=r.get("id", ""),
    #                 budget_name=r.get("name", ""),
    #                 budget_status=r.get("status", ""),
    #                 budget_type=r.get("type", ""),
    #                 account_id=r.get("accountid", ""),
    #                 account_code=r.get("accountcode", ""),
    #                 account_name=r.get("accountname", ""),
    #                 account_class=r.get("accountclass", ""),
    #                 department=r.get("department"),
    #                 location=r.get("location"),
    #                 period=r.get("period"),
    #                 amount=decimal_or_none(r.get("amount")),
    #                 notes=r.get("notes"),
    #                 updated_date_utc=self.parse_datetime(r.get("updateddateutc")),
    #                 ingestion_timestamp=self.now_ts,
    #                 source_system='NETSUITE'
    #             )
    #         except Exception as e:
    #             logger.error(f"Error importing budget row={r}: {e}", exc_info=True)

    #     logger.info(f"Imported {len(budget_rows)} NetSuite Budgets.")

    # ------------------------------------------------------------
    # 10) Import Journals
    # ------------------------------------------------------------
    # @transaction.atomic
    # def import_journals(self, min_id: Optional[str] = None):
    #     """Imports NetSuite Journal Entries into NetSuiteJournals model incrementally."""
    #     logger.info("Importing NetSuite Journal Entries incrementally...")

    #     if not min_id:
    #         min_id = "0"

    #     query = f"""
    #     SELECT
    #         j.id,
    #         j.date,
    #         j.memo,
    #         jl.account,
    #         jl.debit,
    #         jl.credit,
    #         j.currency,
    #         j.exchangerate
    #     FROM journalentry j
    #     JOIN journalline jl ON j.id = jl.journalentry
    #     WHERE j.id > {min_id}
    #     ORDER BY j.id ASC
    #     """
    #     rows = list(self.client.execute_suiteql(query))

    #     for r in rows:
    #         try:
    #             j_id = r.get("id")
    #             if not j_id:
    #                 logger.warning(f"Journal row missing 'id': {r}")
    #                 continue

    #             journal_date = self.parse_date(r.get("date"))

    #             NetSuiteJournals.objects.create(
    #                 company_name=self.org_name,
    #                 journal_id=str(j_id),
    #                 date=journal_date,
    #                 memo=r.get("memo"),
    #                 account=r.get("account"),
    #                 debit=decimal_or_none(r.get("debit")),
    #                 credit=decimal_or_none(r.get("credit")),
    #                 currency=r.get("currency"),
    #                 exchangerate=decimal_or_none(r.get("exchangerate")),
    #                 record_date=self.now_ts
    #             )
    #         except Exception as e:
    #             logger.error(f"Error importing journal row={r}: {e}", exc_info=True)

    #     logger.info(f"Imported {len(rows)} NetSuite Journals (min_id={min_id}).")

    # ------------------------------------------------------------
    # Helper Methods
    # ------------------------------------------------------------
    def parse_date(self, date_str: Optional[str]) -> Optional[timezone.datetime]:
        """Parses a date string into a date object."""
        if not date_str:
            return None
        try:
            return timezone.datetime.strptime(date_str, "%d/%m/%Y").date()
        except ValueError:
            logger.warning(f"Failed to parse date: {date_str}")
            return None

    def parse_datetime(self, datetime_str: Optional[str]) -> Optional[timezone.datetime]:
        """Parses a datetime string into a datetime object.
        
        Attempts several formats in order to capture timestamps with/without time information.
        If no format matches, it falls back to dateutil's parser.
        """
        if not datetime_str:
            return None

        # Define possible datetime formats.
        formats = [
            "%d/%m/%Y %H:%M:%S",
            "%d/%m/%Y %H:%M:%S.%f",  # With microseconds if present.
            "%d/%m/%Y"
        ]
        
        # Try each format
        for fmt in formats:
            try:
                dt = timezone.datetime.strptime(datetime_str, fmt)
                return dt.replace(tzinfo=tz.tzutc())
            except ValueError:
                continue

        # Fallback: try using dateutil parser if none of the above formats matched.
        try:
            dt = dateutil_parse(datetime_str)
            return dt.astimezone(tz.tzutc())
        except Exception as e:
            logger.warning(f"Failed to parse datetime with fallback: {datetime_str} - {e}")
            return None

    def get_quarter(self, month: Optional[int]) -> Optional[int]:
        """Returns the quarter for a given month."""
        if month is None:
            return None
        return (month - 1) // 3 + 1
