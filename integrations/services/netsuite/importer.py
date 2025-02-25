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
    NetSuiteTransformedTransaction,
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
    A robust importer for NetSuite data using batch processing.
    All import/transform methods support optional date filtering.
    """
    def __init__(self, integration: Integration, since_date: Optional[str] = None, until_date: Optional[str] = None):
        self.integration = integration
        self.client = NetSuiteClient(self.integration.netsuite_account_id, integration)
        self.org = integration.org
        self.now_ts = timezone.now()
        self.tenant_id = integration.org.id
        self.since_date = since_date or timezone.datetime.combine(date.today(), datetime.min.time()).strftime("%Y-%m-%d %H:%M:%S")
        self.until_date = until_date  # May be None

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
            ent_id = r.get("entityid") or r.get("id")
            if not ent_id:
                return
            subsidiary = r.get("subsidiaryedition") or "Unknown"
            try:
                NetSuiteEntity.objects.update_or_create(
                    entity_id=ent_id,
                    id=r.get("id"),
                    defaults={
                        "tenant_id": self.org.id,
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
                            "consolidation_key": self.integration.netsuite_account_id,
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
        logger.info("Importing NetSuite Transactions " +
                    ("incrementally..." if (last_import_date or self.since_date) else "(full import)..."))
        
        batch_size = 500
        bulk_create_size = 1000
        total_imported = 0
        min_id = "0"
        marker = None

        from integrations.models.netsuite.temp import NetSuiteTransactions1

        while True:
            close_old_connections()
            
            try:
                with transaction.atomic():
                    marker_clause = ""
                    if marker and marker[1] is not None:
                        marker_clause = (
                            f"AND (LASTMODIFIEDDATE > TO_DATE('{marker[0]}', 'YYYY-MM-DD HH24:MI:SS') OR "
                            f"(LASTMODIFIEDDATE = TO_DATE('{marker[0]}', 'YYYY-MM-DD HH24:MI:SS') AND ID > {marker[1]}))"
                        )

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
                            Transaction.Type as type,
                            Transaction.CreatedDate as createddate,
                            BUILTIN.DF(Transaction.Currency) AS currency,
                            Transaction.AbbrevType as abbrevtype,
                            BUILTIN.DF(Transaction.ApprovalStatus) AS approvalstatus,
                            BUILTIN.DF(Transaction.BalSegStatus) AS balsegstatus,
                            Transaction.BillingStatus as billingstatus,
                            Transaction.CloseDate as closedate,
                            Transaction.CustomType as customtype,
                            Transaction.DaysOpen as daysopen,
                            Transaction.DaysOverdueSearch as daysoverduesearch,
                            Transaction.DueDate as duedate,
                            Transaction.ExchangeRate as exchangerate,
                            Transaction.ExternalId as externalid,
                            Transaction.ForeignAmountPaid as foreignamountpaid,
                            Transaction.ForeignAmountUnpaid as foreignamountunpaid,
                            Transaction.ForeignTotal as foreigntotal,
                            Transaction.IsFinChrg as isfinchrg,
                            Transaction.IsReversal as isreversal,
                            BUILTIN.DF(Transaction.LastModifiedBy) AS lastmodifiedby,
                            Transaction.LastModifiedDate as lastmodifieddate,
                            Transaction.Nexus as nexus,
                            Transaction.Number as number,
                            Transaction.OrdPicked as ordpicked,
                            Transaction.PaymentHold as paymenthold,
                            Transaction.PrintedPickingTicket as printedpickingticket,
                            Transaction.RecordType as recordtype,
                            Transaction.Source as source,
                            Transaction.ToBePrinted as tobeprinted,
                            Transaction.TranDate as trandate,
                            Transaction.TranDisplayName as trandisplayname,
                            Transaction.TranId as tranid,
                            Transaction.TransactionNumber as transactionnumber,
                            Transaction.Void as void,
                            Transaction.Voided as voided,
                            BUILTIN.DF(Transaction.Terms) AS terms
                        FROM 
                            Transaction 
                        WHERE 
                            ID > {min_id}
                            {marker_clause}
                        ORDER BY 
                            ID ASC
                        FETCH NEXT {batch_size} ROWS ONLY
                    """

                    rows = list(self.client.execute_suiteql(query))
                    if not rows:
                        break
                    
                    logger.info(f"Fetched {len(rows)} transaction records (min_id: {min_id}).")
                    print(f"Fetched {len(rows)} transaction records (min_id: {min_id}).")
                    
                    # Accumulate model instances for bulk insertion.
                    transactions_to_create = []
                    for r in rows:
                        txn_id = r.get("id")
                        if not txn_id:
                            continue
                        last_mod = self.parse_datetime(r.get("lastmodifieddate"))
                        if not last_mod:
                            continue
                        try:
                            instance = NetSuiteTransactions1(
                                transactionid=str(txn_id),
                                tenant_id=self.org.id,
                                abbrevtype=r.get("abbrevtype"),
                                approvalstatus=r.get("approvalstatus"),
                                balsegstatus=r.get("balsegstatus"),
                                billingstatus=r.get("billingstatus"),
                                closedate=self.parse_date(r.get("closedate")),
                                createdby=r.get("createdBy"),
                                createddate=self.parse_date(r.get("createddate")),
                                currency=r.get("currency"),
                                customtype=r.get("customtype"),
                                daysopen=r.get("daysopen"),
                                daysoverduesearch=r.get("daysoverduesearch"),
                                duedate=self.parse_date(r.get("duedate")),
                                entity=r.get("Entity"),
                                exchangerate=decimal_or_none(r.get("exchangerate")),
                                externalid=r.get("externalid"),
                                foreignamountpaid=decimal_or_none(r.get("foreignamountpaid")),
                                foreignamountunpaid=decimal_or_none(r.get("foreignamountunpaid")),
                                foreigntotal=decimal_or_none(r.get("foreigntotal")),
                                number=decimal_or_none(r.get("number")),
                                isfinchrg=r.get("isfinchrg"),
                                isreversal=r.get("isreversal"),
                                lastmodifiedby=r.get("lastmodifiedby"),
                                lastmodifieddate=last_mod,
                                nexus=r.get("nexus"),
                                ordpicked=r.get("ordpicked"),
                                paymenthold=r.get("paymenthold"),
                                posting=r.get("posting"),
                                postingperiod=r.get("postingperiod"),
                                printedpickingticket=r.get("printedpickingticket"),
                                recordtype=r.get("recordtype"),
                                source=r.get("source"),
                                status=r.get("status"),
                                terms=r.get("terms"),
                                tobeprinted=r.get("tobeprinted"),
                                trandate=self.parse_date(r.get("trandate")),
                                trandisplayname=r.get("trandisplayname"),
                                tranid=r.get("tranid"),
                                transactionnumber=r.get("transactionnumber"),
                                type=r.get("type"),
                                userevenuearrangement=r.get("userevenuearrangement"),
                                visibletocustomer=r.get("visibletocustomer"),
                                void_field=r.get("void"),
                                voided=r.get("voided"),
                                memo=r.get("memo"),
                                record_date=last_mod,
                                consolidation_key=self.integration.netsuite_account_id,
                            )
                            transactions_to_create.append(instance)
                        except Exception as e:
                            logger.error(f"Error processing transaction row {txn_id}: {e}", exc_info=True)

                    if transactions_to_create:
                        # Bulk create in chunks of bulk_create_size.
                        NetSuiteTransactions1.objects.bulk_create(
                            transactions_to_create, 
                            batch_size=bulk_create_size, 
                            ignore_conflicts=True
                        )
                        total_imported += len(transactions_to_create)

                    # Update marker based on the last row in the batch.
                    last_row = rows[-1]
                    new_marker_date_raw = last_row.get("lastmodifieddate")
                    new_marker_id = last_row.get("id")  # Ensure the correct key is used.

                    if new_marker_date_raw:
                        new_marker_date = self.parse_datetime(new_marker_date_raw)
                        new_marker_date_str = new_marker_date.strftime("%Y-%m-%d %H:%M:%S") if new_marker_date else "1970-01-01 00:00:00"
                    else:
                        new_marker_date_str = "1970-01-01 00:00:00"
                    
                    marker = (new_marker_date_str, new_marker_id)
                    min_id = new_marker_id
                    
                    logger.info(f"Processed batch. New marker: LASTMODIFIEDDATE={new_marker_date_str}, ID={new_marker_id}. Total imported: {total_imported}")
                    
                    # Log the import event within the same transaction.
                    self.log_import_event(
                        module_name="netsuite_transactions",
                        fetched_records=total_imported
                    )
                    
                    if len(rows) < batch_size:
                        break

            except Exception as e:
                logger.error(f"Error importing transactions batch: {e}", exc_info=True)
                close_old_connections()
                continue

        logger.info(f"Imported Transactions: {total_imported} records processed.")

        
    
    # ------------------------------------------------------------
    # 8) Transform General Ledger (from transformed transactions)
    # ------------------------------------------------------------
    def transform_general_ledger(self):
        logger.info("Starting General Ledger transformation from transformed transactions...")
        total_mapped = 0
        transformed_records = NetSuiteTransformedTransaction.objects.all().order_by("transactionid", "linesequencenumber")

        def process_gl(rec):
            nonlocal total_mapped
            try:
                trandate_dt = rec.trandate
                gl_defaults = {
                    "tenant_id": rec.tenant_id,
                    "subsidiary_name": rec.subsidiary,
                    "account_name": NetSuiteAccounts.objects.get(account_id=rec.account, tenant_id=rec.tenant_id).name,
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
                    tenant_id=rec.tenant_id,
                    transactionid=rec.transactionid,
                    linesequencenumber=rec.linesequencenumber,
                    defaults=gl_defaults
                )
                total_mapped += 1
            except Exception as e:
                logger.error(f"Error mapping transformed transaction {rec.transactionid} line {rec.linesequencenumber}: {e}", exc_info=True)

        BatchUtils.process_in_batches(list(transformed_records), process_gl, batch_size=1000)
        logger.info(f"General Ledger transformation complete: {total_mapped} entries processed.")
        self.log_import_event(module_name="netsuite_general_ledger", fetched_records=total_mapped)

    # ------------------------------------------------------------
    # 9) Import Transaction Lines (with date filtering)
    # ------------------------------------------------------------
    def import_transaction_lines(self, min_id: Optional[str] = None, last_modified_after: Optional[str] = None,
                                 start_date: Optional[str] = None, end_date: Optional[str] = None):
        logger.info("Importing NetSuite Transaction Lines...")
        batch_size = 500
        min_id = min_id or "0"
        start_date = start_date or self.since_date
        print(f"start_date: {start_date}")
        total_fetched = 0
        date_filter_clause = self.build_date_clause("LINELASTMODIFIEDDATE", since=last_modified_after or start_date, until=end_date)

        line_counter = 0
        while True:
            close_old_connections()
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
                    L.taxline, L.transaction, L.transactiondiscount, L.uniquekey, L.location, L.class 
                FROM TransactionLine L 
                WHERE L.transaction > {min_id}
                    {date_filter_clause}
                ORDER BY L.transaction, L.uniquekey ASC
                FETCH FIRST {batch_size} ROWS ONLY
            """
            
            try:
                rows = list(self.client.execute_suiteql(query))
                if not rows:
                    break
                
                print(f"fetched {len(rows)} transaction accounting line records at {min_id}")
                logger.info(f"Fetched {len(rows)} transaction line records with transaction > {min_id}{date_filter_clause}.")
                
                # Get the last transaction ID for the next iteration
                min_id = rows[-1].get("transaction")
                
            except Exception as e:
                logger.error(f"Error importing transaction lines: {e}", exc_info=True)
                return

            def process_line(r):
                nonlocal line_counter
                line_counter += 1
                unique_key = f"{self.integration.netsuite_account_id}_{line_counter}"
                
                try:
                    last_modified = self.parse_datetime(r.get("linelastmodifieddate"))
                    from integrations.models.netsuite.temp import NetSuiteTransactionLine1
                    NetSuiteTransactionLine1.objects.update_or_create(
                        unique_key=unique_key,  # Using consolidation_key plus counter as our unique field
                        defaults={
                            "transaction_line_id": r.get("id"),
                            "company_name": self.org_name,
                            "is_billable": r.get("isbillable") == 'T',  # Convert 'T'/'F' to boolean
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
                            "class_field": r.get("class"),  # Changed from class_field to class
                            "uniquekey": r.get("uniquekey"),
                            "consolidation_key": self.integration.netsuite_account_id,
                        }
                    )
                except Exception as e:
                    logger.error(f"Error importing transaction line row: {e}", exc_info=True)
        
            BatchUtils.process_in_batches(rows, process_line, batch_size=batch_size)
            total_fetched += len(rows)
            logger.info(f"Processed batch. New min_id (transaction): {min_id}. Total imported: {total_fetched}.")
            
            # Break if we got less than the batch size (meaning we're at the end)
            if len(rows) < batch_size:
                break
        

        self.log_import_event(module_name="netsuite_transaction_lines", fetched_records=total_fetched)
        logger.info("Transaction Line import complete.")

    # ------------------------------------------------------------
    # 10) Import Transaction Accounting Lines (with date filtering and keyset pagination)
    # ------------------------------------------------------------
    def import_transaction_accounting_lines(self, min_id: Optional[str] = None,
                                        last_modified_after: Optional[str] = None,
                                        start_date: Optional[str] = None,
                                        end_date: Optional[str] = None):
        logger.info("Importing Transaction Accounting Lines...")
        min_id = min_id or "0"
        limit = 500
        total_imported = 0
        start_date = start_date or self.since_date or "2024-12-01"
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
                    BUILTIN.DF (ACCOUNTINGBOOK) AS ACCOUNTINGBOOK,
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
                print(f"fetched {len(rows)} transaction accounting line records at {min_id}")
                logger.info(f"Fetched {len(rows)} transaction accounting line records with TRANSACTION > {min_id}{date_filter_clause}.")
            except Exception as e:
                logger.error(f"Error importing transaction accounting lines: {e}", exc_info=True)
                return

            if not rows:
                break

            def process_accounting_line(r):
                try:
                    if str(decimal_or_none(r.get("credit"))) == "3.75":
                        logger.info(f"r: {r}")
                        print(f"r: {r}")
                    last_modified = self.parse_datetime(r.get("lastmodifieddate"))
                    from integrations.models.netsuite.temp import NetSuiteTransactionAccountingLine1
                    NetSuiteTransactionAccountingLine1.objects.update_or_create(
                        transaction=r.get("transaction").lower(),
                        transaction_line=r.get("transactionline").lower(),
                        defaults={
                            "links": r.get("links"),
                            "accountingbook": r.get("accountingbook").lower() if r.get("accountingbook") else None,
                            "account": r.get("account").lower() if r.get("account") else None,
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
                            "consolidation_key": self.integration.netsuite_account_id,
                            "source_uri": r.get("source_uri"),
                        }
                    )
                except Exception as e:
                    logger.error(f"Error importing transaction accounting line row: {e}", exc_info=True)

            BatchUtils.process_in_batches(rows, process_accounting_line, batch_size=limit)
            total_imported += len(rows)
            max_transaction = max(r.get("transaction") for r in rows)
            min_id = str(max_transaction)
            logger.info(f"Processed batch. New min_id: {min_id}. Total imported: {total_imported}.")
            if len(rows) < limit:
                break
        self.log_import_event(module_name="netsuite_transaction_accounting_lines", fetched_records=total_imported)
        logger.info(f"Imported Transaction Accounting Lines: {total_imported} records processed.")
    
    
    def transform_transactions(self):
        logger.info("Starting transformation of NetSuite transactions...")
        total_transformed = 0
        # 1 Accounting Line
        # 2 transaction Line
        # 3 Transactions
        # after accounting line, then transaction using fields transaction, (org_id condition to join)
        accounting_lines = NetSuiteTransactionAccountingLine.objects.filter(org=self.org).order_by("transaction", "transaction_line")

        def process_transformation(al):
            nonlocal total_transformed
            try:
                # Lookup the transaction record using the accounting line's transaction value.
                txn = NetSuiteTransactions.objects.filter(transactionid=str(al.transaction), company_name=self.org).first()
                if not txn:
                    return 

                # Use the proper field for transaction line lookup:
                tline = NetSuiteTransactionLine.objects.filter(transaction_line_id=al.transaction_line, company_name=self.org).first()
                if not tline:
                    return 

                # Lookup the account (if available) using its proper key.
                account_str = str(al.account) if al.account is not None else None
                account_obj = None
                if account_str:
                    account_obj = NetSuiteAccounts.objects.filter(account_id=account_str, company_name=self.org).first()

                # Lookup subsidiary info from the transaction line.
                subsidiary_obj = None
                if tline.subsidiary:
                    subsidiary_obj = NetSuiteSubsidiaries.objects.filter(subsidiary_id=tline.subsidiary, company_name=self.org).first()

                # Lookup entity if provided.
                entity_obj = None
                if txn.entity:
                    entity_obj = NetSuiteEntity.objects.filter(id=txn.entity, company_name=self.org).first()

                # Calculate yearperiod using your helper method.
                yearperiod = self.extract_yearperiod(txn.postingperiod or "")

                transformed_data = {
                    "company_name": txn.company_name,
                    "consolidation_key": self.integration.netsuite_account_id,
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
                }

                NetSuiteTransformedTransaction.objects.update_or_create(
                    company_name=txn.company_name,
                    transactionid=txn.transactionid,
                    linesequencenumber=tline.line_sequence_number,
                    defaults=transformed_data
                )
                total_transformed += 1

            except Exception as e:
                logger.error(f"Error transforming transaction {txn.transactionid if txn else 'N/A'} for AL {al.pk}: {e}", exc_info=True)

        BatchUtils.process_in_batches(list(accounting_lines), process_transformation, batch_size=1000)
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
