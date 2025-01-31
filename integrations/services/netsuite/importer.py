import logging
from django.db import transaction
from django.utils import timezone
from typing import Optional, Dict, Iterator
from datetime import timezone as tz
from .client import NetSuiteClient
from integrations.models.models import Integration
from integrations.models.netsuite.analytics import (
    NetSuiteVendors,
    NetSuiteSubsidiaries,
    NetSuiteDepartments,
    NetSuiteEntity,
    NetSuiteAccountingPeriods,
    NetSuiteAccounts,
    NetSuiteTransactions,
    NetSuiteGeneralLedger,
    NetSuiteBudgetPeriodBalances,
    NetSuiteJournals,
)

logger = logging.getLogger(__name__)


def bool_from_str(val: Optional[str]) -> bool:
    """Convert 'T'/'F' or similar strings to boolean."""
    if val is None:
        return False
    val_upper = val.strip().upper()
    return val_upper in ('T', 'TRUE', 'YES')


def decimal_or_none(val: Optional[str]) -> Optional[float]:
    """Convert string to float, return None if invalid."""
    if val is None or val == '':
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
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

        for r in rows:
            try:
                ent_id = r.get("id")
                if not ent_id:
                    logger.warning(f"Entity row missing 'id': {r}")
                    continue

                subsidiary = r.get("subsidiaryedition") or "Unknown"  # Adjust as needed

                NetSuiteEntity.objects.update_or_create(
                    entity_id=ent_id,
                    defaults={
                        "company_name": r.get("companyname") or self.org_name,
                        "entity_number": r.get("entitynumber"),
                        "company_display_name": r.get("companyname"),  # Adjust if needed
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

        logger.info(f"Imported {len(rows)} NetSuite Entities (load_type={load_type}).")

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
        """Imports NetSuite Accounts into NetSuiteAccounts model with pagination."""
        logger.info("Importing NetSuite Accounts...")

        # if load_type == "drop_and_reload":
        #     NetSuiteAccounts.objects.filter(
        #         company_name=self.org_name
        #     ).delete()

        offset = 0
        limit = 1000
        total_imported = 0

        while offset < 2000:
            print(f"Importing accounts at offset {offset}.")
            query = f"SELECT * FROM Account ORDER BY id ASC OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY"
            rows = list(self.client.execute_suiteql(query))
            if not rows:
                break
                
            print(f"Importing {len(rows)} accounts at offset {offset}.")

            for r in rows:
                try:
                    acct_id = r.get("id")
                    if not acct_id:
                        logger.warning(f"Account row missing 'id': {r}")
                        continue

                    subsidiary = r.get("subsidiaryedition") or "Unknown"  # Adjust field as needed

                    NetSuiteAccounts.objects.update_or_create(
                        account_id=acct_id,
                        defaults={
                            "company_name": self.org_name,
                            "account_number": r.get("acctnumber"),
                            "account_name": r.get("accountsearchdisplaynamecopy"),
                            "account_hierarchy": r.get("fullname"),
                            "account_display_name": r.get("accountsearchdisplayname"),
                            "account_display_hierarchy": r.get("displaynamewithhierarchy"),
                            "parent_id": r.get("parent"),  # Adjust if needed
                            "parent_account": r.get("parent_account"),  # Ensure correct field
                            "account_type": r.get("accttype"),
                            "sspecacct": r.get("sspecacct"),
                            "description": r.get("description"),
                            "eliminate": bool_from_str(r.get("eliminate")),
                            "external_id": r.get("externalid"),
                            "include_children": bool_from_str(r.get("includechildren")),
                            "inventory": bool_from_str(r.get("inventory")),
                            "is_inactive": bool_from_str(r.get("isinactive")),
                            "is_summary": bool_from_str(r.get("issummary")),
                            "last_modified_date": self.parse_datetime(r.get("lastmodifieddate")),
                            "reconcile_with_matching": bool_from_str(r.get("reconcilewithmatching")),
                            "revalue": bool_from_str(r.get("revalue")),
                            "subsidiary": subsidiary,
                            "balance": decimal_or_none(r.get("balance")),
                            "record_date": self.now_ts
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
        """Imports NetSuite Transactions into NetSuiteTransactions model incrementally."""
        logger.info("Importing NetSuite Transactions incrementally...")

        if not min_id:
            min_id = "0"

        query = f"""
        SELECT 
            id,
            abbrevtype,
            approvalstatus,
            number,
            source,
            status,
            trandisplayname,
            tranid,
            transactionnumber,
            type,
            recordtype,
            createdby,
            createddate,
            lastmodifiedby,
            lastmodifieddate,
            postingperiod,
            trandate,
            memo,
            externalid,
            entity,
            currency,
            exchangerate,
        FROM Transaction
        WHERE id > {min_id}
        ORDER BY id ASC
        """
        rows = list(self.client.execute_suiteql(query))

        for r in rows:
            try:
                trans_id = r.get("id")
                if not trans_id:
                    logger.warning(f"Transaction row missing 'id': {r}")
                    continue

                subsidiary = r.get("subsidiaryedition") or "Unknown"  # Adjust field as needed

                trandate = self.parse_date(r.get("trandate"))

                NetSuiteTransactions.objects.update_or_create(
                    transactionid=str(trans_id),
                    defaults={
                        "company_name": self.org_name,
                        "abbrevtype": r.get("abbrevtype"),
                        "uniquekey": r.get("uniquekey"),
                        "linesequencenumber": int(r.get("linesequencenumber", 0)),
                        "lineid": r.get("lineid"),
                        "approvalstatus": r.get("approvalstatus"),
                        "number": r.get("number"),
                        "source": r.get("source"),
                        "status": r.get("status"),
                        "trandisplayname": r.get("trandisplayname"),
                        "tranid": r.get("tranid"),
                        "transactionnumber": r.get("transactionnumber"),
                        "type": r.get("type"),
                        "recordtype": r.get("recordtype"),
                        "createdby": r.get("createdby"),
                        "createddate": self.parse_datetime(r.get("createddate")),
                        "lastmodifiedby": r.get("lastmodifiedby"),
                        "lastmodifieddate": self.parse_datetime(r.get("lastmodifieddate")),
                        "postingperiod": r.get("postingperiod"),
                        "yearperiod": int(r.get("yearperiod", 0)),
                        "trandate": trandate,
                        "subsidiary": subsidiary,
                        "subsidiaryfullname": r.get("subsidiaryfullname"),
                        "subsidiaryid": r.get("subsidiaryid"),
                        "department": r.get("department"),
                        "departmentid": r.get("departmentid"),
                        "location": r.get("location"),
                        "class_field": r.get("class"),
                        "memo": r.get("memo"),
                        "linememo": r.get("linememo"),
                        "externalid": r.get("externalid"),
                        "entity": r.get("entity"),
                        "entityid": r.get("entityid"),
                        "account": r.get("account"),
                        "acctnumber": r.get("acctnumber"),
                        "accountsearchdisplayname": r.get("accountsearchdisplayname"),
                        "amount": decimal_or_none(r.get("amount")),
                        "debit": decimal_or_none(r.get("debit")),
                        "credit": decimal_or_none(r.get("credit")),
                        "netamount": decimal_or_none(r.get("netamount")),
                        "currency": r.get("currency"),
                        "exchangerate": decimal_or_none(r.get("exchangerate")),
                        "record_date": self.now_ts,
                        "duplicate_check": 1  # Adjust based on logic
                    }
                )
            except Exception as e:
                logger.error(f"Error importing transaction row={r}: {e}", exc_info=True)

        logger.info(f"Imported {len(rows)} NetSuite Transactions (min_id={min_id}).")

    # ------------------------------------------------------------
    # 8) Import General Ledger
    # ------------------------------------------------------------
    @transaction.atomic
    def import_general_ledger(self, min_id: Optional[str] = None):
        """Imports NetSuite General Ledger data into NetSuiteGeneralLedger model."""
        logger.info("Importing NetSuite General Ledger data...")

        if not min_id:
            min_id = "0"

        limit = 1000  # Number of records to process per batch
        total_rows_imported = 0

        while True:
            query = f"""
            SELECT
                T.TRANSACTIONID AS TRANSACTIONID,
                T.TRANDATE AS TRANDATE,
                T.POSTINGPERIOD AS POSTINGPERIOD,
                T.ABBREVTYPE AS ABBREVTYPE,
                L.SUBSIDIARY AS SUBSIDIARY,
                L.ACCOUNT AS ACCOUNT,
                A.ACCTNUMBER AS ACCTNUMBER,
                L.AMOUNT AS AMOUNT,
                L.NETAMOUNT AS NETAMOUNT,
                T.CURRENCY AS CURRENCY,
                T.EXCHANGERATE AS EXCHANGERATE,
                L.UNIQUEKEY AS UNIQUEKEY,
                L.LINESEQUENCENUMBER AS LINESEQUENCENUMBER,
                L.ID AS LINEID,
                T.APPROVALSTATUS AS APPROVALSTATUS
            FROM PROD.NETSUITE.TRANSACTION T
            JOIN PROD.NETSUITE.TRANSACTIONLINE L ON L.TRANSACTIONID = T.TRANSACTIONID
            JOIN PROD.NETSUITE.ACCOUNT A ON A.ID = L.ACCOUNT
            WHERE T.TRANSACTIONID > {min_id}
            ORDER BY T.TRANSACTIONID ASC, L.UNIQUEKEY ASC
            """

            try:
                logger.debug(f"Executing SuiteQL Query: {query}")
                rows = list(self.client.execute_suiteql(query))
            except Exception as e:
                logger.error(f"SuiteQL Query Failed: {e}", exc_info=True)
                return  # Exit the function if the query fails

            if not rows:
                break  # No more rows to process

            ledger_entries = []
            max_transactionid = min_id  # Initialize to current min_id

            for r in rows:
                try:
                    trans_id = r.get("TRANSACTIONID")
                    if not trans_id:
                        logger.warning(f"General Ledger row missing 'TRANSACTIONID': {r}")
                        continue

                    # Update max_transactionid for the next iteration
                    if trans_id > max_transactionid:
                        max_transactionid = trans_id

                    trandate = self.parse_date(r.get("TRANDATE"))
                    posting_period = r.get("POSTINGPERIOD", "")

                    # Calculate YEARPERIOD in Python
                    year_period = None
                    if 'FY' in posting_period:
                        fy_index = posting_period.find('FY')
                        if fy_index != -1 and fy_index + 4 <= len(posting_period):
                            year_part = posting_period[fy_index + 2: fy_index + 4]
                            try:
                                year_int = int(year_part)
                            except ValueError:
                                year_int = 0
                        else:
                            year_int = 0

                        if 'ADJUSTMENT' in posting_period.upper():
                            year_period = 200000 + (year_int * 100) + 13
                        else:
                            try:
                                month_part = int(posting_period[1:3])
                                quarter = (month_part - 1) // 3 + 1
                                year_period = 200000 + (year_int * 100) + quarter
                            except (ValueError, IndexError):
                                year_period = 200000 + (year_int * 100)

                    ledger_entry = NetSuiteGeneralLedger(
                        company_name=self.org_name,
                        abbrevtype=r.get("ABBREVTYPE"),
                        transactionid=str(trans_id),
                        uniquekey=str(r.get("UNIQUEKEY")),
                        linesequencenumber=int(r.get("LINESEQUENCENUMBER", 0)),
                        lineid=str(r.get("LINEID")),
                        approvalstatus=r.get("APPROVALSTATUS"),
                        postingperiod=posting_period,
                        yearperiod=year_period,
                        trandate=trandate,
                        subsidiary=r.get("SUBSIDIARY"),
                        account=r.get("ACCOUNT"),
                        acctnumber=r.get("ACCTNUMBER"),
                        amount=decimal_or_none(r.get("AMOUNT")),
                        debit=decimal_or_none(r.get("DEBIT")),
                        credit=decimal_or_none(r.get("CREDIT")),
                        netamount=decimal_or_none(r.get("NETAMOUNT")),
                        currency=r.get("CURRENCY"),
                        exchangerate=decimal_or_none(r.get("EXCHANGERATE")),
                        record_date=self.now_ts
                    )

                    ledger_entries.append(ledger_entry)
                except Exception as e:
                    logger.error(f"Error importing general ledger row={r}: {e}", exc_info=True)

            if not ledger_entries:
                break  # No entries to add

            # Bulk create ledger entries
            try:
                NetSuiteGeneralLedger.objects.bulk_create(ledger_entries, ignore_conflicts=True)
                total_rows_imported += len(ledger_entries)
                logger.info(f"Imported {len(ledger_entries)} NetSuite General Ledger records (min_id={min_id}).")
            except Exception as e:
                logger.error(f"Bulk create failed: {e}", exc_info=True)

            # Update the min_id to the highest TRANSACTIONID fetched for the next batch
            min_id = max_transactionid

        logger.info(f"Total records imported: {total_rows_imported}")


    # ------------------------------------------------------------
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
        """Parses a datetime string into a datetime object."""
        if not datetime_str:
            return None
        try:
            return timezone.datetime.strptime(datetime_str, "%d/%m/%Y").replace(tzinfo=tz.utc)
        except ValueError:
            logger.warning(f"Failed to parse datetime: {datetime_str}")
            return None

    def get_quarter(self, month: Optional[int]) -> Optional[int]:
        """Returns the quarter for a given month."""
        if month is None:
            return None
        return (month - 1) // 3 + 1
