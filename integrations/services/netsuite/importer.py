import logging
import re
from decimal import Decimal, InvalidOperation
from datetime import datetime, date, timezone as dt_timezone
from typing import Optional

from django.db import transaction, close_old_connections
from django.utils import timezone
from dateutil import tz
from dateutil.parser import parse as dateutil_parse

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
    A robust importer for NetSuite data. All import/transform methods support optional date filtering.
    The since_date and until_date (format "YYYY-MM-DD HH:MI:SS") are passed in at initialization.
    If not provided, since_date defaults to today at midnight.
    The code uses keyset pagination and calls close_old_connections() each batch so that long‐running processes
    can recover from dropped connections.
    """

    def __init__(self, integration: Integration, since_date: Optional[str] = None, until_date: Optional[str] = None):
        self.integration = integration
        self.consolidation_key = int(integration.netsuite_account_id)
        self.client = NetSuiteClient(self.consolidation_key, integration)
        self.org_name = integration.org
        self.now_ts = timezone.now()
        self.org = Organisation.objects.get(name=self.org_name)
        # since_date and until_date should be in "YYYY-MM-DD HH:MI:SS" format.
        # If since_date is not provided, default to today's date at midnight.
        self.since_date = since_date or timezone.datetime.combine(date.today(), datetime.min.time()).strftime("%Y-%m-%d %H:%M:%S")
        self.until_date = until_date  # Can be None

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
        Build a SuiteQL date filtering clause for the given field.
        Both since and until are expected in the format "YYYY-MM-DD HH:MI:SS".
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
        logger.info("Importing NetSuite Vendors...")
        if load_type == "drop_and_reload":
            NetSuiteVendors.objects.filter(company_name=self.org_name).delete()

        date_clause = self.build_date_clause("LASTMODIFIEDDATE", self.since_date, self.until_date)
        query = f"SELECT * FROM Vendor WHERE 1=1 {date_clause}"
        rows = list(self.client.execute_suiteql(query))
        count = 0
        for r in rows:
            vendor_id = r.get("id")
            if not vendor_id:
                continue  # Required key missing; skip silently.
            subsidiary = r.get("subsidiaryedition") or "Unknown"
            try:
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
                count += 1
            except Exception as e:
                logger.error(f"Error importing vendor row: {e}", exc_info=True)
        self.log_import_event(module_name="netsuite_vendors", fetched_records=len(rows))
        logger.info(f"Imported {count} NetSuite Vendors.")

    # ------------------------------------------------------------
    # 2) Import Subsidiaries
    # ------------------------------------------------------------
    @transaction.atomic
    def import_subsidiaries(self, load_type="drop_and_reload"):
        logger.info("Importing NetSuite Subsidiaries...")
        if load_type == "drop_and_reload":
            NetSuiteSubsidiaries.objects.filter(company_name=self.org_name).delete()

        date_clause = self.build_date_clause("lastmodifieddate", self.since_date, self.until_date)
        query = f"""
            SELECT id, name, fullname, legalname, iselimination, currency, country, lastmodifieddate
            FROM subsidiary
            WHERE 1=1 {date_clause}
            ORDER BY id
        """
        rows = list(self.client.execute_suiteql(query))
        count = 0
        for r in rows:
            sub_id = r.get("id")
            if not sub_id:
                continue
            try:
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
                count += 1
            except Exception as e:
                logger.error(f"Error importing subsidiary row: {e}", exc_info=True)
        self.log_import_event(module_name="netsuite_subsidiaries", fetched_records=len(rows))
        logger.info(f"Imported {count} NetSuite Subsidiaries.")

    # ------------------------------------------------------------
    # 3) Import Departments
    # ------------------------------------------------------------
    @transaction.atomic
    def import_departments(self, load_type="drop_and_reload"):
        logger.info("Importing NetSuite Departments...")
        if load_type == "drop_and_reload":
            NetSuiteDepartments.objects.filter(company_name=self.org_name).delete()

        # Assuming no date filter is needed here.
        query = "SELECT id, name, fullname, subsidiary, isinactive FROM department ORDER BY id"
        rows = list(self.client.execute_suiteql(query))
        count = 0
        for r in rows:
            dept_id = r.get("id")
            if not dept_id:
                continue
            try:
                NetSuiteDepartments.objects.update_or_create(
                    department_id=dept_id,
                    defaults={
                        "company_name": self.org_name,
                        "name": r.get("name"),
                        "full_name": r.get("fullname"),
                        "subsidiary": r.get("subsidiary"),
                        "is_inactive": bool_from_str(r.get("isinactive")),
                        "record_date": self.now_ts,
                    }
                )
                count += 1
            except Exception as e:
                logger.error(f"Error importing department row: {e}", exc_info=True)
        self.log_import_event(module_name="netsuite_departments", fetched_records=len(rows))
        logger.info(f"Imported {count} NetSuite Departments.")

    # ------------------------------------------------------------
    # 4) Import Entities
    # ------------------------------------------------------------
    @transaction.atomic
    def import_entities(self, load_type="drop_and_reload"):
        logger.info("Importing NetSuite Entities...")
        if load_type == "drop_and_reload":
            NetSuiteEntity.objects.filter(company_name=self.org_name).delete()

        date_clause = self.build_date_clause("lastmodifieddate", self.since_date, self.until_date)
        query = f"SELECT * FROM entity WHERE 1=1 {date_clause}"
        rows = list(self.client.execute_suiteql(query))
        count = 0
        for r in rows:
            ent_id = r.get("entityid") or r.get("id")
            if not ent_id:
                continue
            subsidiary = r.get("subsidiaryedition") or "Unknown"
            try:
                NetSuiteEntity.objects.update_or_create(
                    entity_id=ent_id,
                    id=r.get("id"),
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
                        "record_date": self.now_ts,
                    }
                )
                count += 1
            except Exception as e:
                logger.error(f"Error importing entity row: {e}", exc_info=True)
        self.log_import_event(module_name="netsuite_entities", fetched_records=len(rows))
        logger.info(f"Imported {count} NetSuite Entities.")

    # ------------------------------------------------------------
    # 5) Import Accounting Periods
    # ------------------------------------------------------------
    @transaction.atomic
    def import_accounting_periods(self, load_type="drop_and_reload"):
        logger.info("Importing NetSuite Accounting Periods...")
        if load_type == "drop_and_reload":
            NetSuiteAccountingPeriods.objects.filter(company_name=self.org_name).delete()

        date_clause = self.build_date_clause("lastmodifieddate", self.since_date, self.until_date)
        query = f"SELECT * FROM accountingperiod WHERE 1=1 {date_clause}"
        rows = list(self.client.execute_suiteql(query))
        count = 0
        for r in rows:
            period_id = r.get("id")
            if not period_id:
                continue
            start_date_obj = self.parse_date(r.get("startdate"))
            end_date_obj = self.parse_date(r.get("enddate"))
            year_val = start_date_obj.year if start_date_obj else None
            quarter_val = self.get_quarter(start_date_obj.month) if start_date_obj else None
            period_val = start_date_obj.month if start_date_obj else None
            try:
                NetSuiteAccountingPeriods.objects.update_or_create(
                    period_id=period_id,
                    defaults={
                        "company_name": self.org_name,
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
                count += 1
            except Exception as e:
                logger.error(f"Error importing accounting period row: {e}", exc_info=True)
        self.log_import_event(module_name="netsuite_accounting_periods", fetched_records=len(rows))
        logger.info(f"Imported {count} NetSuite Accounting Periods.")

    # ------------------------------------------------------------
    # 6) Import Accounts (with pagination)
    # ------------------------------------------------------------
    @transaction.atomic
    def import_accounts(self, load_type="drop_and_reload"):
        logger.info("Importing NetSuite Accounts...")
        offset = 0
        limit = 1000
        total_imported = 0
        date_clause = self.build_date_clause("lastmodifieddate", self.since_date, self.until_date)
        while True:
            close_old_connections()  # refresh DB connection
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
            for r in rows:
                account_id = r.get("id")
                if not account_id:
                    continue
                try:
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
                    logger.error(f"Error importing account row: {e}", exc_info=True)
            total_imported += len(rows)
            offset += limit
            if len(rows) < limit:
                break
            logger.debug(f"Imported {len(rows)} accounts at offset {offset}.")
        self.log_import_event(module_name="netsuite_accounts", fetched_records=total_imported)
        logger.info(f"Imported {total_imported} NetSuite Accounts.")

    # ------------------------------------------------------------
    # 7) Import Transactions (using keyset pagination and date filtering)
    # ------------------------------------------------------------
    @transaction.atomic
    def import_transactions(self, last_import_date: Optional[str] = None):
        logger.info("Importing NetSuite Transactions " +
                    ("incrementally..." if (last_import_date or self.since_date) else "(full import)..."))
        filter_since = last_import_date or self.since_date
        date_filter_clause = ""
        if filter_since:
            date_filter_clause = f"AND LASTMODIFIEDDATE > TO_DATE('{filter_since}', 'YYYY-MM-DD HH24:MI:SS')"
        limit = 500
        total_imported = 0
        marker = None

        while True:
            close_old_connections()
            marker_clause = ""
            if marker:
                marker_clause = f"AND (LASTMODIFIEDDATE, ID) > (TO_DATE('{marker[0]}', 'YYYY-MM-DD HH24:MI:SS'), {marker[1]})"
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
                txn_id = r.get("id")
                if not txn_id:
                    continue
                last_mod = self.parse_datetime(r.get("lastmodifieddate"))
                if not last_mod:
                    continue
                try:
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
                    logger.error(f"Error importing transaction row: {e}", exc_info=True)
            total_imported += len(rows)
            last_row = rows[-1]
            new_marker_date_raw = last_row.get("LASTMODIFIEDDATE")
            new_marker_id = last_row.get("ID")
            if new_marker_date_raw:
                new_marker_date = self.parse_datetime(new_marker_date_raw)
                new_marker_date_str = new_marker_date.strftime("%Y-%m-%d %H:%M:%S") if new_marker_date else "1970-01-01 00:00:00"
            else:
                new_marker_date_str = "1970-01-01 00:00:00"
            marker = (new_marker_date_str, new_marker_id)
            logger.info(f"Processed batch. New marker: LASTMODIFIEDDATE={new_marker_date_str}, ID={new_marker_id}.")
            if len(rows) < limit:
                break
        self.log_import_event(module_name="netsuite_transactions", fetched_records=total_imported)
        logger.info(f"Imported {total_imported} Transaction records successfully.")

    # ------------------------------------------------------------
    # 8) Transform General Ledger (from transformed transactions)
    # ------------------------------------------------------------
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

    @transaction.atomic
    def transform_general_ledger(self):
        logger.info("Starting General Ledger transformation from transformed transactions...")
        total_mapped = 0
        from integrations.models.netsuite.analytics import NetSuiteTransformedTransaction

        transformed_records = NetSuiteTransformedTransaction.objects.all().order_by("transactionid", "linesequencenumber")
        for rec in transformed_records:
            try:
                trandate_dt = rec.trandate
                gl_defaults = {
                    "tenant_name": rec.company_name,
                    "subsidiary_name": rec.subsidiary,
                    "account_name": NetSuiteAccounts.objects.get(account_id=rec.account, company_name=rec.company_name).name,
                    "abbrevtype": rec.abbrevtype,
                    "uniquekey": f"{rec.transactionid}-{rec.linesequencenumber}",
                    "linesequencenumber": rec.linesequencenumber,
                    "lineid": rec.lineid,
                    "approvalstatus": rec.approvalstatus,
                    "postingperiod": rec.postingperiod,
                    "yearperiod": rec.yearperiod,
                    "trandate": trandate_dt,
                    "subsidiary": rec.subsidiary,
                    "account_id": rec.account,
                    "acctnumber": rec.acctnumber,
                    "amount": rec.amount,
                    "debit": rec.debit,
                    "credit": rec.credit,
                    "netamount": rec.netamount,
                    "currency": rec.currency,
                    "exchangerate": rec.exchangerate,
                    "record_date": rec.record_date,
                }
                NetSuiteGeneralLedger.objects.update_or_create(
                    tenant_name=rec.company_name,
                    transactionid=rec.transactionid,
                    linesequencenumber=rec.linesequencenumber,
                    defaults=gl_defaults
                )
                total_mapped += 1
            except Exception as e:
                logger.error(f"Error mapping transformed transaction {rec.transactionid} line {rec.linesequencenumber}: {e}", exc_info=True)
                continue
        logger.info(f"General Ledger transformation complete: {total_mapped} entries processed.")
        self.log_import_event(module_name="netsuite_general_ledger", fetched_records=total_mapped)

    # ------------------------------------------------------------
    # 9) Import Transaction Lines (with date filtering)
    # ------------------------------------------------------------
    @transaction.atomic
    def import_transaction_lines(self, min_id: Optional[str] = None, last_modified_after: Optional[str] = None,
                                 start_date: Optional[str] = None, end_date: Optional[str] = None):
        logger.info("Importing NetSuite Transaction Lines...")
        batch_size = 500
        if not min_id:
            min_id = "0"
        if not start_date:
            start_date = self.since_date
        total_fetched = 0
        date_filter_clause = self.build_date_clause("LINELASTMODIFIEDDATE", since=last_modified_after or start_date, until=end_date)
        while True:
            close_old_connections()
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
                    {date_filter_clause}
                ORDER BY id ASC
                FETCH NEXT {batch_size} ROWS ONLY
            """
            try:
                rows = list(self.client.execute_suiteql(query))
                logger.info(f"Fetched {len(rows)} transaction line records with id > {min_id}{date_filter_clause}.")
            except Exception as e:
                logger.error(f"Error importing transaction lines: {e}", exc_info=True)
                return

            if not rows:
                break

            for r in rows:
                netsuite_id = r.get("id")
                if not netsuite_id:
                    continue
                try:
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
                    logger.error(f"Error importing transaction line row: {e}", exc_info=True)
            total_fetched += len(rows)
            min_id = rows[-1].get("id")
            logger.info(f"Processed batch. New min_id: {min_id}. Total imported: {total_fetched}.")
            if len(rows) < batch_size:
                break
        self.log_import_event(module_name="netsuite_transaction_lines", fetched_records=total_fetched)
        logger.info("Transaction Line import complete.")

    # ------------------------------------------------------------
    # 10) Import Transaction Accounting Lines (with date filtering and keyset pagination)
    # ------------------------------------------------------------
    @transaction.atomic
    def import_transaction_accounting_lines(self, min_id: Optional[str] = None,
                                            last_modified_after: Optional[str] = None,
                                            start_date: Optional[str] = None,
                                            end_date: Optional[str] = None):
        logger.info("Importing Transaction Accounting Lines...")
        if not min_id:
            min_id = "0"
        limit = 500
        total_imported = 0
        if not start_date:
            start_date = self.since_date or "2024-12-01"

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
                FROM TransactionAccountingLine
                WHERE TRANSACTION > {min_id}
                    {date_filter_clause}
                ORDER BY TRANSACTION ASC
                FETCH NEXT {limit} ROWS ONLY
            """
            try:
                rows = list(self.client.execute_suiteql(query))
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
                        transaction=r.get("transaction").lower(),
                        transaction_line=r.get("transactionline").lower(),
                        defaults={
                            "links": r.get("links"),
                            "account": r.get("account").lower(),
                            "accountingbook": r.get("accountingbook").lower(),
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
                    logger.error(f"Error importing transaction accounting line row: {e}", exc_info=True)
            total_imported += len(rows)
            max_transaction = max(r.get("transaction") for r in rows)
            min_id = str(max_transaction)
            logger.info(f"Processed batch. New min_id: {min_id}. Total imported: {total_imported}.")
            if len(rows) < limit or total_imported > 5000:
                break
        self.log_import_event(module_name="netsuite_transaction_accounting_lines", fetched_records=total_imported)
        logger.info(f"Imported {total_imported} Transaction Accounting Lines successfully.")

    # ------------------------------------------------------------
    # 11) Transform Transaction (into unified transformed records)
    # ------------------------------------------------------------
    @transaction.atomic
    def transform_transactions(self):
        logger.info("Starting transformation of NetSuite transactions...")
        total_transformed = 0
        from integrations.models.netsuite.analytics import NetSuiteTransformedTransaction
        accounting_lines = NetSuiteTransactionAccountingLine.objects.filter(org=self.org).order_by("transaction", "transaction_line")
        for al in accounting_lines:
            try:
                try:
                    txn = NetSuiteTransactions.objects.get(transactionid=al.transaction, company_name=self.org)
                except NetSuiteTransactions.DoesNotExist:
                    continue
                try:
                    tline = NetSuiteTransactionLine.objects.get(id=al.transaction_line, company_name=self.org)
                except NetSuiteTransactionLine.DoesNotExist:
                    continue

                account_str = str(al.account) if al.account is not None else None
                account_obj = None
                if account_str:
                    try:
                        account_obj = NetSuiteAccounts.objects.get(account_id=account_str, company_name=self.org)
                    except NetSuiteAccounts.DoesNotExist:
                        pass

                subsidiary_obj = None
                if tline.subsidiary:
                    try:
                        subsidiary_obj = NetSuiteSubsidiaries.objects.get(subsidiary_id=tline.subsidiary, company_name=self.org)
                    except NetSuiteSubsidiaries.DoesNotExist:
                        pass

                entity_obj = None
                if txn.entity:
                    try:
                        entity_obj = NetSuiteEntity.objects.get(id=txn.entity, company_name=self.org)
                    except NetSuiteEntity.DoesNotExist:
                        pass

                yearperiod = self.extract_yearperiod(txn.postingperiod or "")
                transformed_data = {
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
                    "subsidiary": subsidiary_obj.name if subsidiary_obj else txn.subsidiary,
                    "subsidiaryfullname": subsidiary_obj.full_name if subsidiary_obj else None,
                    "subsidiaryid": tline.subsidiary,
                    "department": getattr(tline, "department", None),
                    "departmentid": getattr(tline, "departmentid", None),
                    "linesequencenumber": tline.line_sequence_number,
                    "lineid": str(tline.id),
                    "location": tline.location,
                    "clas": getattr(tline, "class_field", None),
                    "linenmemo": tline.memo,
                    "memo": txn.memo,
                    "externalid": txn.externalid,
                    "entity_id": entity_obj.entity_id if entity_obj else txn.entity,
                    "entity_title": entity_obj.entity_title if entity_obj else None,
                    "legal_name": entity_obj.legal_name if entity_obj else None,
                    "parent_entity": entity_obj.parent_entity if entity_obj else None,
                    "entity_email": entity_obj.email if entity_obj else None,
                    "entity_phone": entity_obj.phone if entity_obj else None,
                    "terms": txn.terms,
                    "daysopen": txn.daysopen,
                    "daysoverduesearch": txn.daysoverduesearch,
                    "duedate": txn.duedate,
                    "closedate": txn.closedate,
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
                    "linenetamount": tline.net_amount,
                    "account": account_str,
                    "acctnumber": account_obj.acctnumber if account_obj else None,
                    "accountsearchdisplayname": account_obj.accountsearchdisplayname if account_obj else None,
                    "accttype": account_obj.accttype if account_obj else None,
                    "displaynamewithhierarchy": account_obj.displaynamewithhierarchy if account_obj else None,
                    "fullname": account_obj.fullname if account_obj else None,
                    "sspecacct": account_obj.sspecacct if account_obj else None,
                    "billingstatus": txn.billingstatus,
                    "custbody_report_timestamp": txn.custbody_report_timestamp,
                    "currency": txn.currency,
                    "exchangerate": Decimal(txn.exchangerate) if txn.exchangerate else None,
                    "foreignamountpaid": Decimal(txn.foreignamountpaid) if txn.foreignamountpaid else None,
                    "foreignamountunpaid": Decimal(txn.foreignamountunpaid) if txn.foreignamountunpaid else None,
                    "foreigntotal": Decimal(txn.foreigntotal) if txn.foreigntotal else None,
                    "foreignlineamount": getattr(tline, "foreignlineamount", None),
                    "record_date": txn.record_date,
                    "uniquekey": f"{txn.transactionid}-{tline.line_sequence_number}",
                }
                NetSuiteTransformedTransaction.objects.update_or_create(
                    company_name=txn.company_name,
                    transactionid=txn.transactionid,
                    linesequencenumber=tline.line_sequence_number,
                    defaults=transformed_data
                )
                total_transformed += 1
            except Exception as e:
                logger.error(f"Error transforming transaction {txn.transactionid} for AL {al.pk}: {e}", exc_info=True)
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
