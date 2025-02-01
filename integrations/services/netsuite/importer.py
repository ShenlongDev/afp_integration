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
        print(f"Importing {len(rows)} vendors...")
        for r in rows:
            print(r)
            break

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

        # Filter rows based on today's last modified date
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
        """Imports NetSuite Transactions into NetSuiteTransactions model incrementally based on lastmodifieddate."""
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
            exchangerate
        FROM Transaction
        WHERE id > {min_id}
        ORDER BY id ASC
        """
        rows = list(self.client.execute_suiteql(query))
        today = timezone.now().date()
        filtered_rows = [
            r for r in rows 
            if self.parse_datetime(r.get("lastmodifieddate")) 
               and self.parse_datetime(r.get("lastmodifieddate")).date() == today
        ]

        logger.info(f"Fetched {len(rows)} transactions, importing {len(filtered_rows)} modified today.")

        for r in filtered_rows:
            try:
                trans_id = r.get("id")
                if not trans_id:
                    logger.warning(f"Transaction row missing 'id': {r}")
                    continue

                subsidiary = r.get("subsidiaryedition") or "Unknown"

                trandate = self.parse_date(r.get("trandate"))
                last_modified = self.parse_datetime(r.get("lastmodifieddate"))

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
                        "lastmodifieddate": last_modified,
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
                        "record_date": last_modified,
                        "duplicate_check": 1  # Adjust based on logic
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
        Map data from NetSuiteTransactions (and, if available, NetSuiteAccounts) into
        the NetSuiteGeneralLedger model.

        For each transaction record, we:
        1. Attempt to fetch additional account details (such as acctnumber) from
            NetSuiteAccounts using txn.account.
        2. Build a dictionary of fields for the general ledger. (If a field is missing,
            it will simply remain None.)
        3. Upsert (update_or_create) a NetSuiteGeneralLedger record using a unique
            combination (company_name, transactionid, linesequencenumber).

        Any fields missing in the source will be ignored.
        """
        transactions = NetSuiteTransactions.objects.all()
        total_mapped = 0

        for txn in transactions:
            try:
                # Try to retrieve the corresponding account record to enhance mapping.
                account_obj = None
                if txn.account:
                    try:
                        account_obj = NetSuiteAccounts.objects.get(account_id=txn.account)
                    except NetSuiteAccounts.DoesNotExist:
                        account_obj = None

                # Build the defaults for the general ledger entry.
                # Note: if a field is missing in txn, it will be stored as None.
                defaults = {
                    'abbrevtype': txn.abbrevtype,
                    'uniquekey': txn.uniquekey,
                    'linesequencenumber': txn.linesequencenumber,
                    'lineid': txn.lineid,
                    'approvalstatus': txn.approvalstatus,
                    'postingperiod': txn.postingperiod,
                    'yearperiod': txn.yearperiod,
                    'trandate': txn.trandate,
                    'subsidiary': txn.subsidiary,
                    'account': txn.account,
                    # Prefer the account's acctnumber if found; otherwise, use the one in txn.
                    'acctnumber': account_obj.acctnumber if account_obj and account_obj.acctnumber else txn.acctnumber,
                    'amount': txn.amount,
                    'debit': txn.debit,
                    'credit': txn.credit,
                    'netamount': txn.netamount,
                    'currency': txn.currency,
                    'exchangerate': txn.exchangerate,
                    'record_date': txn.record_date,
                }

                # The unique key is defined by the combination of the company, transaction ID, and line sequence.
                gl_obj, created = NetSuiteGeneralLedger.objects.update_or_create(
                    company_name=txn.company_name,  # Note: company_name is a ForeignKey to Organisation.
                    transactionid=txn.transactionid,
                    linesequencenumber=txn.linesequencenumber,
                    defaults=defaults
                )
                total_mapped += 1

                if created:
                    logger.info(f"Created GL entry for transaction {txn.transactionid}, line {txn.linesequencenumber}")
                else:
                    logger.info(f"Updated GL entry for transaction {txn.transactionid}, line {txn.linesequencenumber}")

            except Exception as e:
                logger.error(f"Error mapping transaction {txn.transactionid}: {e}", exc_info=True)

        logger.info(f"Completed mapping general ledger: {total_mapped} entries processed.")


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
