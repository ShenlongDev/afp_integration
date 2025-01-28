from typing import Optional
from django.db import transaction
from django.utils import timezone
from ...models.netsuite.raw import NetSuiteRawVendors, NetSuiteRawSubsidiary, NetSuiteRawTransactions, NetSuiteRawTransactionLine, NetSuiteRawTransactionAccountingLine, NetSuiteRawEntity, NetSuiteRawAccountingPeriod, NetSuiteRawBudget, NetSuiteRawBudgetLine, NetSuiteRawDepartment, NetSuiteRawAccounts
from .client import NetSuiteClient


class NetSuiteImporter:
    def __init__(self, consolidation_key: str):
        self.consolidation_key = consolidation_key
        self.client = NetSuiteClient(consolidation_key)


    @transaction.atomic
    def import_vendors(self):
        """Import vendors from NetSuite"""
        query = "SELECT * FROM Vendor"
       
        for vendor in self.client.execute_suiteql(query):
            NetSuiteRawVendors.objects.create(
                consolidation_key=self.consolidation_key,
                raw_payload=vendor,
                ingestion_timestamp=timezone.now()
            )


    @transaction.atomic
    def import_subsidiary(self):
        """Import subsidiary data"""
        query = "SELECT * FROM subsidiary"
       
        for subsidiary in self.client.execute_suiteql(query):
            NetSuiteRawSubsidiary.objects.create(
                consolidation_key=self.consolidation_key,
                raw_payload=subsidiary,
                ingestion_timestamp=timezone.now()
            )


    @transaction.atomic
    def import_transactions(self, min_id: Optional[str] = None):
        """Import transactions incrementally"""
        query = """
        SELECT Transaction.ID, Transaction.TranID, Transaction.TranDate,
               BUILTIN.DF(Transaction.PostingPeriod) AS PostingPeriod,
               Transaction.Memo, Transaction.Posting,
               BUILTIN.DF(Transaction.Status) AS Status,
               BUILTIN.DF(Transaction.CreatedBy) AS CreatedBy,
               BUILTIN.DF(Transaction.Subsidiary) AS Subsidiary,
               BUILTIN.DF(Transaction.Entity) AS Entity,
               type, createddate,
               BUILTIN.DF(Transaction.currency) AS currency,
               -- ... other fields ...
        FROM Transaction
        WHERE ID > $min
        ORDER BY id ASC
        """
       
        for transaction in self.client.execute_suiteql(query, min_id=min_id):
            NetSuiteRawTransactions.objects.create(
                consolidation_key=self.consolidation_key,
                raw_payload=transaction,
                ingestion_timestamp=timezone.now()
            )


    @transaction.atomic
    def import_transaction_lines(self, min_id: Optional[str] = None):
        """Import transaction lines"""
        query = """
        SELECT L.memo, L.accountinglinetype, L.cleared,
               L.closedate, L.commitmentfirm, L.creditforeignamount,
               BUILTIN.DF(L.department) AS department,
               L.department AS departmentid,
               -- ... other fields ...
        FROM TransactionLine L
        WHERE L.transaction > $min
        ORDER BY L.transaction, L.uniquekey ASC
        """
       
        for line in self.client.execute_suiteql(query, min_id=min_id):
            NetSuiteRawTransactionLine.objects.create(
                consolidation_key=self.consolidation_key,
                raw_payload=line,
                ingestion_timestamp=timezone.now()
            )


class NetSuiteImporter:
    # ... existing methods ...


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
        """Import entity data"""
        query = """
        SELECT
            ID,
            EntityId,
            EntityNumber,
            CompanyName,
            LegalName,
            IsPerson,
            IsInactive,
            ParentEntity,
            Email,
            Phone,
            Currency,
            Subsidiary,
            Terms,
            LastModifiedDate
        FROM Entity
        """
       
        for entity in self.client.execute_suiteql(query):
            NetSuiteRawEntity.objects.create(
                consolidation_key=self.consolidation_key,
                raw_payload=entity,
                ingestion_timestamp=timezone.now()
            )


    @transaction.atomic
    def import_accounting_periods(self):
        """Import accounting periods"""
        query = """
        SELECT
            ID,
            PeriodName,
            StartDate,
            EndDate,
            Closed,
            AllLocked,
            ApLocked,
            ArLocked,
            PayrollLocked,
            FiscalCalendar,
            Year,
            Quarter,
            Period,
            IsAdjust,
            Parent
        FROM accountingperiod
        """
       
        for period in self.client.execute_suiteql(query):
            NetSuiteRawAccountingPeriod.objects.create(
                consolidation_key=self.consolidation_key,
                raw_payload=period,
                ingestion_timestamp=timezone.now()
            )


class NetSuiteImporter:
    @transaction.atomic
    def import_transaction_lines(self, min_id: Optional[str] = None):
        """Import transaction lines with complete field list"""
        query = """
        SELECT
            L.memo,
            L.accountinglinetype,
            L.cleared,
            L.closedate,
            L.commitmentfirm,
            L.creditforeignamount,
            BUILTIN.DF(L.department) AS department,
            L.department AS departmentid,
            L.documentnumber,
            L.donotdisplayline,
            L.eliminate,
            BUILTIN.DF(L.entity) AS entity,
            L.entity AS entityid,
            L.expenseaccount AS expenseaccountid,
            BUILTIN.DF(L.expenseaccount) AS expenseaccount,
            L.foreignamount,
            L.foreignamountpaid,
            L.foreignamountunpaid,
            L.id,
            L.isbillable,
            L.isclosed,
            L.iscogs,
            L.iscustomglline,
            L.isfullyshipped,
            L.isfxvariance,
            L.isinventoryaffecting,
            L.isrevrectransaction,
            L.linelastmodifieddate,
            L.linesequencenumber,
            L.mainline,
            L.matchbilltoreceipt,
            L.netamount,
            L.oldcommitmentfirm,
            L.quantitybilled,
            L.quantityrejected,
            L.quantityshiprecv,
            BUILTIN.DF(L.subsidiary) AS subsidiary,
            L.subsidiary AS subsidiaryid,
            L.taxline,
            L.transactiondiscount,
            L.uniquekey,
            L.location,
            L.class,
            L.custcol_rpc_model_cat_group,
            L.custcol_rpc_model_category_sub
        FROM TransactionLine L
        WHERE L.transaction > $min
        ORDER BY L.transaction, L.uniquekey ASC
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
def import_transactions(self, min_id: Optional[str] = None):
    """Import transactions with custom fields"""
    query = """
    SELECT
        ... existing fields ...,
        custbody_report_timestamp,
        custbody_nexus_notc,
        custbody_rpc_payment_approved,
        custbody_rpc_same_reference_number,
        custbody_cash_register,
        custbody_nondeductible_processed,
        custbody_rpc_duplicate_bill_created,
        custbody_rpc_duplicate_bill_credit_crt,
        custbody_rpc_lightyear_bill,
        custbody_rpc_lightyear_bill_credit,
        custbody_wrong_subs
    FROM Transaction
    WHERE ID > $min
    ORDER BY id ASC
    """


@transaction.atomic
def import_departments(self):
    """Import departments from NetSuite"""
    query = "SELECT * FROM department"
   
    for department in self.client.execute_suiteql(query):
        NetSuiteRawDepartment.objects.create(
            consolidation_key=self.consolidation_key,
            raw_payload=department,
            ingestion_timestamp=timezone.now()
        )




@transaction.atomic
def import_accounts(self):
    """Import accounts from NetSuite"""
    query = """
    SELECT
        ID,
        AcctNumber,
        AcctName,
        FullName,
        AccountSearchDisplayName,
        DisplayNameWithHierarchy,
        Parent,
        AcctType,
        Description,
        Eliminate,
        ExternalId,
        IncludeChildren,
        Inventory,
        IsInactive,
        IsSummary,
        LastModifiedDate,
        ReconcileWithMatching,
        Revalue,
        Subsidiary,
        Balance
    FROM Account
    """
   
    for account in self.client.execute_suiteql(query):
        NetSuiteRawAccounts.objects.create(
            consolidation_key=self.consolidation_key,
            raw_payload=account,
            ingestion_timestamp=timezone.now()
        )
