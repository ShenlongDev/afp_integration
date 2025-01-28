from typing import Optional
from django.db import transaction
from django.utils import timezone
from ...models.netsuite.raw import *
from .client import NetSuiteClient


class NetSuiteImporter:
    def __init__(self, consolidation_key: str):
        self.consolidation_key = consolidation_key
        self.client = NetSuiteClient(consolidation_key)


    @transaction.atomic
    def import_vendors(self):
        """Import vendors with PostgreSQL syntax"""
        query = """
        SELECT
            id,
            entityid,
            companyname,
            isperson,
            isinactive,
            email,
            phone,
            currency,
            subsidiary,
            terms,
            lastmodifieddate
        FROM vendor
        """




    @transaction.atomic
    def import_subsidiary(self):
        """Import subsidiary with PostgreSQL syntax"""
        query = """
        SELECT
            id,
            name,
            namenohierarchy,
            fullname,
            legalname,
            federalnumber,
            iselimination,
            currency,
            country,
            lastmodifieddate
        FROM subsidiary
            """

    @transaction.atomic
    def import_transactions(self, min_id: Optional[str] = None):
        """Import transactions with PostgreSQL syntax"""
        query = """
        SELECT
            t.id,
            t.tranid,
            t.trandate,
            t.postingperiod,
            t.memo,
            t.posting,
            t.status,
            t.createdby,
            t.subsidiary,
            t.entity,
            t.type,
            t.createddate,
            t.currency,
            t.abbrevtype,
            t.approvalstatus,
            t.balsegstatus,
            t.billingstatus,
            t.closedate,
            t.customtype,
            t.daysopen,
            t.daysoverduesearch,
            t.duedate,
            t.exchangerate,
            t.externalid,
            COALESCE(t.foreignamountpaid, 0) as foreignamountpaid,
            COALESCE(t.foreignamountunpaid, 0) as foreignamountunpaid,
            COALESCE(t.foreigntotal, 0) as foreigntotal,
            t.isfinchrg,
            t.isreversal,
            t.lastmodifiedby,
            t.lastmodifieddate,
            t.nexus,
            t.number,
            t.ordpicked,
            t.paymenthold,
            t.printedpickingticket,
            t.recordtype,
            t.source,
            t.tobeprinted,
            t.userevenuearrangement,
            t.visibletocustomer,
            t.void,
            t.voided,
            t.terms,
            t.custbody_report_timestamp,
            t.custbody_nexus_notc,
            t.custbody_rpc_payment_approved,
            t.custbody_rpc_same_reference_number,
            t.custbody_cash_register,
            t.custbody_nondeductible_processed,
            t.custbody_rpc_duplicate_bill_created,
            t.custbody_rpc_duplicate_bill_credit_crt,
            t.custbody_rpc_lightyear_bill,
            t.custbody_rpc_lightyear_bill_credit,
            t.custbody_wrong_subs
        FROM transaction t
        WHERE t.id > %s
        ORDER BY t.id ASC
        """


    
        for transaction in self.client.execute_suiteql(query, min_id=min_id):
            NetSuiteRawTransactions.objects.create(
                consolidation_key=self.consolidation_key,
                raw_payload=transaction,
                ingestion_timestamp=timezone.now()
            )


    @transaction.atomic
    def import_transaction_lines(self, min_id: Optional[str] = None):
        """Import transaction lines with PostgreSQL syntax"""
        query = """
        SELECT
            l.memo,
            l.accountinglinetype,
            l.cleared,
            l.closedate,
            l.commitmentfirm,
            COALESCE(l.creditforeignamount, 0) as creditforeignamount,
            l.department,
            l.departmentid,
            l.documentnumber,
            l.donotdisplayline,
            l.eliminate,
            l.entity,
            l.entityid,
            l.expenseaccountid,
            l.expenseaccount,
            COALESCE(l.foreignamount, 0) as foreignamount,
            COALESCE(l.foreignamountpaid, 0) as foreignamountpaid,
            COALESCE(l.foreignamountunpaid, 0) as foreignamountunpaid,
            l.id,
            l.isbillable,
            l.isclosed,
            l.iscogs,
            l.iscustomglline,
            l.isfullyshipped,
            l.isfxvariance,
            l.isinventoryaffecting,
            l.isrevrectransaction,
            l.linelastmodifieddate,
            l.linesequencenumber,
            l.mainline,
            l.matchbilltoreceipt,
            COALESCE(l.netamount, 0) as netamount,
            l.oldcommitmentfirm,
            COALESCE(l.quantitybilled, 0) as quantitybilled,
            COALESCE(l.quantityrejected, 0) as quantityrejected,
            COALESCE(l.quantityshiprecv, 0) as quantityshiprecv,
            l.subsidiary,
            l.subsidiaryid,
            l.taxline,
            l.transactiondiscount,
            l.uniquekey,
            l.location,
            l.class,
            l.custcol_rpc_model_cat_group,
            l.custcol_rpc_model_category_sub
        FROM transactionline l
        WHERE l.transaction > %s
        ORDER BY l.transaction, l.uniquekey ASC
        """

        for line in self.client.execute_suiteql(query, min_id=min_id):
            NetSuiteRawTransactionLine.objects.create(
                consolidation_key=self.consolidation_key,
                raw_payload=line,
                ingestion_timestamp=timezone.now()
            )

    @transaction.atomic
    def import_transaction_accounting_lines(self, min_id: Optional[str] = None):
        """Import transaction accounting lines with full field list"""
        query = """
        SELECT
            L.Transaction as transaction,
            L.TransactionLine as transactionline,
            L.Account as account,
            L.AccountingBook as accountingbook,
            L.Amount as amount,
            L.AmountForeign as amountforeign,
            L.AmountLinked as amountlinked,
            L.Credit as credit,
            L.CreditForeign as creditforeign,
            L.Debit as debit,
            L.DebitForeign as debitforeign,
            L.Eliminate as eliminate,
            L.NetAmount as netamount,
            L.Posting as posting,
            L.Subsidiary as subsidiary,
            L.SegmentAccount as segmentaccount,
            L.AmountPaid as amountpaid,
            L.AmountUnpaid as amountunpaid
        FROM TransactionAccountingLine L
        WHERE Transaction > $min
        ORDER BY L.transaction ASC, transactionline ASC
        """
       
        for line in self.client.execute_suiteql(query, min_id=min_id):
            NetSuiteRawTransactionAccountingLine.objects.create(
                consolidation_key=self.consolidation_key,
                raw_payload=line,
                ingestion_timestamp=timezone.now()
            )


    @transaction.atomic
    def import_entity(self):
        """Import entity with PostgreSQL syntax"""
        query = """
        SELECT
            id,
            entityid,
            entitynumber,
            companyname,
            legalname,
            isperson,
            isinactive,
            parententity,
            email,
            phone,
            currency,
            subsidiary,
            terms,
            lastmodifieddate
        FROM entity
        """
        for entity in self.client.execute_suiteql(query):
            NetSuiteRawEntity.objects.create(
                consolidation_key=self.consolidation_key,
                raw_payload=entity,
                ingestion_timestamp=timezone.now()
            )


    @transaction.atomic
    def import_accounting_periods(self):
        """Import accounting periods with PostgreSQL syntax"""
        query = """
        SELECT
            id,
            periodname,
            startdate,
            enddate,
            closed,
            alllocked,
            aplocked,
            arlocked,
            payrolllocked,
            fiscalcalendar,
            EXTRACT(YEAR FROM startdate) as year,
            CASE
                WHEN EXTRACT(MONTH FROM startdate) BETWEEN 1 AND 3 THEN 1
                WHEN EXTRACT(MONTH FROM startdate) BETWEEN 4 AND 6 THEN 2
                WHEN EXTRACT(MONTH FROM startdate) BETWEEN 7 AND 9 THEN 3
                ELSE 4
            END as quarter,
            EXTRACT(MONTH FROM startdate) as period,
            isadjust,
            parent
        FROM accountingperiod
        ORDER BY startdate
        """

        for period in self.client.execute_suiteql(query):
            NetSuiteRawAccountingPeriod.objects.create(
                consolidation_key=self.consolidation_key,
                raw_payload=period,
                ingestion_timestamp=timezone.now()
            )

    @transaction.atomic
    def import_transaction_accounting_lines(self, min_id: Optional[str] = None):
        """Import transaction accounting lines with PostgreSQL syntax"""
        query = """
        SELECT
            l.transaction,
            l.transactionline,
            l.account,
            l.accountingbook,
            COALESCE(l.amount, 0) as amount,
            COALESCE(l.amountforeign, 0) as amountforeign,
            COALESCE(l.amountlinked, 0) as amountlinked,
            COALESCE(l.credit, 0) as credit,
            COALESCE(l.creditforeign, 0) as creditforeign,
            COALESCE(l.debit, 0) as debit,
            COALESCE(l.debitforeign, 0) as debitforeign,
            l.eliminate,
            COALESCE(l.netamount, 0) as netamount,
            l.posting,
            l.subsidiary,
            l.segmentaccount,
            COALESCE(l.amountpaid, 0) as amountpaid,
            COALESCE(l.amountunpaid, 0) as amountunpaid,
            l.processedbyrevcommit,
            l.custcol_rpc_model_cat_group,
            l.custcol_rpc_model_category_sub
        FROM transactionaccountingline l
        WHERE l.transaction > %s
        ORDER BY l.transaction ASC, l.transactionline ASC
        """

        for line in self.client.execute_suiteql(query, min_id=min_id):
            NetSuiteRawTransactionLine.objects.create(
                consolidation_key=self.consolidation_key,
                raw_payload=line,
                ingestion_timestamp=timezone.now()
            )


    @transaction.atomic
    def import_transaction_accounting_lines(self, min_id: Optional[str] = None):
        """Import transaction accounting lines with complete field list"""
        query = """
        SELECT
            L.Transaction as transaction,
            L.TransactionLine as transactionline,
            L.Account as account,
            L.AccountingBook as accountingbook,
            L.Amount as amount,
            L.AmountForeign as amountforeign,
            L.AmountLinked as amountlinked,
            L.Credit as credit,
            L.CreditForeign as creditforeign,
            L.Debit as debit,
            L.DebitForeign as debitforeign,
            L.Eliminate as eliminate,
            L.NetAmount as netamount,
            L.Posting as posting,
            L.Subsidiary as subsidiary,
            L.SegmentAccount as segmentaccount,
            L.AmountPaid as amountpaid,
            L.AmountUnpaid as amountunpaid,
            L.ProcessedByRevCommit as processedbyrevcommit,
            L.custcol_rpc_model_cat_group,
            L.custcol_rpc_model_category_sub
        FROM TransactionAccountingLine L
        WHERE Transaction > $min
        ORDER BY L.transaction ASC, transactionline ASC
        """
    
        for line in self.client.execute_suiteql(query, min_id=min_id):
            NetSuiteRawTransactionAccountingLine.objects.create(
                consolidation_key=self.consolidation_key,
                raw_payload=line,
                ingestion_timestamp=timezone.now()
            )


    @transaction.atomic
    def import_budgets(self):
        """Import budgets from NetSuite"""
        query = "SELECT * FROM Budget"
    
        for budget in self.client.execute_suiteql(query):
            NetSuiteRawBudget.objects.create(
                consolidation_key=self.consolidation_key,
                raw_payload=budget,
                ingestion_timestamp=timezone.now()
            )


    @transaction.atomic
    def import_budget_lines(self):
        """Import budget lines"""
        query = """
        SELECT * FROM BudgetLine
        """
    
        for line in self.client.execute_suiteql(query):
            NetSuiteRawBudgetLine.objects.create(
                consolidation_key=self.consolidation_key,
                raw_payload=line,
                ingestion_timestamp=timezone.now()
            )


    @transaction.atomic
    def import_departments(self):
        """Import departments with PostgreSQL syntax"""
        query = """
        SELECT
            id,
            name,
            fullname,
            subsidiary,
            isinactive,
            lastmodifieddate
        FROM department
        """


    @transaction.atomic
    def import_accounts(self):
        """Import accounts with PostgreSQL syntax"""
        query = """
        SELECT
            id,
            acctnumber,
            acctname,
            fullname,
            accountsearchdisplayname,
            displaynamewithhierarchy,
            parent,
            accttype,
            description,
            eliminate,
            externalid,
            includechildren,
            inventory,
            isinactive,
            issummary,
            lastmodifieddate,
            reconcilewithmatching,
            revalue,
            subsidiary,
            COALESCE(balance, 0) as balance
        FROM account
        ORDER BY acctnumber
        """


    
        for account in self.client.execute_suiteql(query):
            NetSuiteRawAccounts.objects.create(
                consolidation_key=self.consolidation_key,
                raw_payload=account,
                ingestion_timestamp=timezone.now()
            )
