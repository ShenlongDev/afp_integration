from django.db.models import (
    F, Value, Case, When, Window, Q, Func,
    CharField, IntegerField, DecimalField
)
from django.db.models.functions import (
    Substr, Cast, RowNumber, Coalesce, ExtractYear,
    Concat, StrIndex
)
from django.contrib.postgres.aggregates import JSONBAgg
from django.db import transaction
from ..models.netsuite.raw import *
from ..models.netsuite.analytics import *
# from ..models.netsuite.raw import NetSuiteRawTransactions
import logging

logger = logging.getLogger(__name__)

class NetSuiteTransformer:
    
    
    # @transaction.atomic
    # def transform_transactions(self):
    #     """Transform transactions matching the original SQL"""
    #     transactions_data = (
    #         NetSuiteRawTransactions.objects
    #         .select_related(
    #             'companies',
    #             'transaction_line',
    #             'transaction_accounting_line',
    #             'subsidiary'
    #         )
    #         .annotate(
    #             company_name=F('companies__company_name'),
    #             abbrevtype=JSONBAgg('raw_payload', 'AbbrevType'),
    #             transactionid=JSONBAgg('raw_payload', 'ID'),
    #             uniquekey=JSONBAgg('transaction_line__raw_payload', 'UniqueKey'),
    #             linesequencenumber=JSONBAgg('transaction_line__raw_payload', 'LineSequenceNumber'),
    #             lineid=JSONBAgg('transaction_line__raw_payload', 'ID'),
    #             approvalstatus=JSONBAgg('raw_payload', 'ApprovalStatus'),
    #             number=JSONBAgg('raw_payload', 'Number'),
    #             source=JSONBAgg('raw_payload', 'Source'),
    #             status=JSONBAgg('raw_payload', 'Status'),
    #             trandisplayname=JSONBAgg('raw_payload', 'TranDisplayName'),
    #             tranid=JSONBAgg('raw_payload', 'TranID'),
    #             transactionnumber=JSONBAgg('raw_payload', 'TransactionNumber'),
    #             type=JSONBAgg('raw_payload', 'Type'),
    #             recordtype=JSONBAgg('raw_payload', 'RecordType'),
    #             createdby=JSONBAgg('raw_payload', 'CreatedBy'),
    #             createddate=JSONBAgg('raw_payload', 'CreatedDate'),
    #             lastmodifiedby=JSONBAgg('raw_payload', 'LastModifiedBy'),
    #             lastmodifieddate=JSONBAgg('raw_payload', 'LastModifiedDate'),
    #             postingperiod=JSONBAgg('raw_payload', 'PostingPeriod'),
               
    #             # Complex year period calculation for PostgreSQL
    #             yearperiod=Case(
    #                 When(
    #                     raw_payload__PostingPeriod__contains='FY',
    #                     then=Value(200000) +
    #                          Cast(
    #                              Substr(
    #                                  JSONBAgg('raw_payload', 'PostingPeriod'),
    #                                  StrIndex('FY', JSONBAgg('raw_payload', 'PostingPeriod')) + 2,
    #                                  2
    #                              ),
    #                              output_field=IntegerField()
    #                          ) * 100 +
    #                          Case(
    #                              When(
    #                                  raw_payload__PostingPeriod__icontains='ADJUSTMENT',
    #                                  then=Value(13)
    #                              ),
    #                              default=Cast(
    #                                  Substr(
    #                                      JSONBAgg('raw_payload', 'PostingPeriod'),
    #                                      2,
    #                                      2
    #                                  ),
    #                                  output_field=IntegerField()
    #                              )
    #                          )
    #                 )
    #             ),
               
    #             trandate=JSONBAgg('raw_payload', 'TranDate'),
    #             subsidiary=JSONBAgg('subsidiary__raw_payload', 'Name'),
    #             subsidiaryfullname=JSONBAgg('subsidiary__raw_payload', 'FullName'),
    #             subsidiaryid=JSONBAgg('transaction_line__raw_payload', 'SubsidiaryId'),
    #             department=JSONBAgg('transaction_line__raw_payload', 'Department'),
    #             departmentid=JSONBAgg('transaction_line__raw_payload', 'DepartmentId'),
    #             location=JSONBAgg('transaction_line__raw_payload', 'Location'),
    #             class_field=JSONBAgg('transaction_line__raw_payload', 'Class'),
               
    #             memo=JSONBAgg('raw_payload', 'Memo'),
    #             linememo=JSONBAgg('transaction_line__raw_payload', 'Memo'),
    #             externalid=JSONBAgg('raw_payload', 'ExternalId'),
    #             entity=JSONBAgg('transaction_line__raw_payload', 'Entity'),
    #             entityid=JSONBAgg('transaction_line__raw_payload', 'EntityId'),
               
    #             account=JSONBAgg('transaction_accounting_line__raw_payload', 'Account'),
    #             acctnumber=JSONBAgg('accounts__raw_payload', 'AcctNumber'),
    #             accountsearchdisplayname=JSONBAgg('accounts__raw_payload', 'AccountSearchDisplayName'),
               
    #             amount=Cast(
    #                 JSONBAgg('transaction_accounting_line__raw_payload', 'Amount'),
    #                 output_field=DecimalField()
    #             ),
    #             debit=Cast(
    #                 JSONBAgg('transaction_accounting_line__raw_payload', 'Debit'),
    #                 output_field=DecimalField()
    #             ),
    #             credit=Cast(
    #                 JSONBAgg('transaction_accounting_line__raw_payload', 'Credit'),
    #                 output_field=DecimalField()
    #             ),
    #             netamount=Cast(
    #                 JSONBAgg('transaction_accounting_line__raw_payload', 'NetAmount'),
    #                 output_field=DecimalField()
    #             ),
               
    #             currency=JSONBAgg('raw_payload', 'Currency'),
    #             exchangerate=Cast(
    #                 JSONBAgg('raw_payload', 'ExchangeRate'),
    #                 output_field=DecimalField()
    #             ),
               
    #             record_date=F('ingestion_timestamp'),
               
    #             duplicate_check=Window(
    #                 expression=RowNumber(),
    #                 partition_by=['transaction_line__raw_payload__UniqueKey'],
    #                 order_by='-ingestion_timestamp'
    #             )
    #         )
    #         .filter(duplicate_check=1)
    #         .order_by('transactionid', 'linesequencenumber')
    #     )


    #     # Bulk create with PostgreSQL upsert
    #     NetSuiteTransactions.objects.bulk_create(
    #     (NetSuiteTransactions(**transaction) for transaction in transactions_data),
    #     update_conflicts=True,
    #     unique_fields=['company_name', 'transactionid', 'linesequencenumber'],
    #     update_fields=['raw_payload', 'ingestion_timestamp']
    # )


    @transaction.atomic
    def transform_general_ledger(self, integration):
        """Transform to general ledger matching original SQL"""
        gl_data = (
            NetSuiteTransactions.objects
            .filter(
                Q(approvalstatus='Approved') | Q(approvalstatus__isnull=True)
            )
            .values(
                'abbrevtype',
                'transactionid', 
                'uniquekey',
                'linesequencenumber',
                'lineid',
                'approvalstatus',
                'postingperiod',
                'yearperiod',
                'trandate',
                'subsidiary',
                'account',
                'acctnumber',
                'amount',
                'debit',
                'credit', 
                'netamount',
                'currency',
                'exchangerate',
                'record_date'
            )
            .order_by('transactionid', 'linesequencenumber')
        )

        # Add company_name to each entry
        gl_entries = []
        for entry in gl_data:
            entry['company_name'] = integration.org
            gl_entries.append(NetSuiteGeneralLedger(**entry))

        # Bulk create with PostgreSQL upsert
        NetSuiteGeneralLedger.objects.bulk_create(
            gl_entries,
            update_conflicts=True,
            unique_fields=['company_name', 'transactionid', 'linesequencenumber'],
            update_fields=['amount', 'debit', 'credit', 'netamount', 'record_date']
        )


    # @transaction.atomic
    # def transform_accounts(self):
    #     """Transform raw accounts into analytical accounts"""
    #     logger.info("Transforming NetSuite Accounts...")
    #     raw_accounts = NetSuiteRawAccounts.objects.filter(
    #         consolidation_key=self.consolidation_key
    #     ).select_related('companies')

    #     for raw in raw_accounts:
    #         data = {
    #             "company_name": raw.companies.company_name,
    #             "account_id": raw.raw_payload.get("id"),
    #             "account_number": raw.raw_payload.get("acctnumber"),
    #             "account_name": raw.raw_payload.get("accountsearchdisplayname"),
    #             "amount": raw.raw_payload.get("amount"),
    #             "debit": raw.raw_payload.get("debit"),
    #             "credit": raw.raw_payload.get("credit"),
    #             "net_amount": raw.raw_payload.get("netamount"),
    #             "currency": raw.raw_payload.get("currency"),
    #             "exchange_rate": raw.raw_payload.get("exchangerate"),
    #             "record_date": raw.raw_payload.get("record_date"),
    #         }
    #         NetSuiteAccounts.objects.update_or_create(
    #             company_name=data["company_name"],
    #             account_id=data["account_id"],
    #             defaults=data
    #         )
    #     logger.info("Completed transforming NetSuite Accounts.")


    # @transaction.atomic
    # def transform_accounting_periods(self):
    #     """Transform accounting periods"""
    #     periods_data = (
    #         NetSuiteRawAccountingPeriod.objects
    #         .select_related('companies')
    #         .annotate(
    #             company_name=F('companies__company_name'),
    #             period_id=JSONBAgg('raw_payload', 'ID'),
    #             period_name=JSONBAgg('raw_payload', 'PeriodName'),
    #             start_date=Cast(
    #                 JSONBAgg('raw_payload', 'StartDate'),
    #                 output_field=CharField()
    #             ),
    #             end_date=Cast(
    #                 JSONBAgg('raw_payload', 'EndDate'),
    #                 output_field=CharField()
    #             ),
    #             closed=Cast(
    #                 JSONBAgg('raw_payload', 'Closed'),
    #                 output_field=CharField()
    #             ),
    #             all_locked=Cast(
    #                 JSONBAgg('raw_payload', 'AllLocked'),
    #                 output_field=CharField()
    #             ),
    #             fiscal_calendar=JSONBAgg('raw_payload', 'FiscalCalendar'),
    #             year=Cast(
    #                 JSONBAgg('raw_payload', 'Year'),
    #                 output_field=IntegerField()
    #             ),
    #             quarter=Cast(
    #                 JSONBAgg('raw_payload', 'Quarter'),
    #                 output_field=IntegerField()
    #             ),
    #             period=Cast(
    #                 JSONBAgg('raw_payload', 'Period'),
    #                 output_field=IntegerField()
    #             ),
    #             record_date=F('ingestion_timestamp')
    #         )
    #     )


    #     # Bulk create with PostgreSQL upsert
    #     NetSuiteAccountingPeriods.objects.bulk_create(
    #         (NetSuiteAccountingPeriods(**period) for period in periods_data),
    #         update_conflicts=True,
    #         unique_fields=['company_name', 'period_id'],
    #         update_fields=['closed', 'all_locked', 'record_date']
    #     )


    # @transaction.atomic
    # def transform_departments(self):
    #     """Transform departments"""
    #     departments_data = (
    #         NetSuiteRawDepartment.objects
    #         .select_related('companies')
    #         .annotate(
    #             company_name=F('companies__company_name'),
    #             department_id=JSONBAgg('raw_payload', 'ID'),
    #             name=JSONBAgg('raw_payload', 'Name'),
    #             full_name=JSONBAgg('raw_payload', 'FullName'),
    #             subsidiary=JSONBAgg('raw_payload', 'Subsidiary'),
    #             is_inactive=Cast(
    #                 JSONBAgg('raw_payload', 'IsInactive'),
    #                 output_field=CharField()
    #             ),
    #             record_date=F('ingestion_timestamp')
    #         )
    #     )


    #     # Bulk create with PostgreSQL upsert
    #     NetSuiteDepartments.objects.bulk_create(
    #         (NetSuiteDepartments(**dept) for dept in departments_data),
    #         update_conflicts=True,
    #         unique_fields=['company_name', 'department_id'],
    #         update_fields=['name', 'is_inactive', 'record_date']
    #     )


    # @transaction.atomic
    # def transform_subsidiaries(self):
    #     """Transform subsidiaries"""
    #     subsidiaries_data = (
    #         NetSuiteRawSubsidiary.objects
    #         .select_related('companies')
    #         .annotate(
    #             company_name=F('companies__company_name'),
    #             subsidiary_id=JSONBAgg('raw_payload', 'ID'),
    #             name=JSONBAgg('raw_payload', 'Name'),
    #             name_nohi=JSONBAgg('raw_payload', 'NameNoHierarchy'),
    #             full_name=JSONBAgg('raw_payload', 'FullName'),
    #             legal_name=JSONBAgg('raw_payload', 'LegalName'),
    #             federal_number=JSONBAgg('raw_payload', 'FederalNumber'),
    #             is_elimination=Cast(
    #                 JSONBAgg('raw_payload', 'IsElimination'),
    #                 output_field=CharField()
    #             ),
    #             currency=JSONBAgg('raw_payload', 'Currency'),
    #             country=JSONBAgg('raw_payload', 'Country'),
    #             record_date=F('ingestion_timestamp')
    #         )
    #     )


    #     # Bulk create with PostgreSQL upsert
    #     NetSuiteSubsidiaries.objects.bulk_create(
    #         (NetSuiteSubsidiaries(**sub) for sub in subsidiaries_data),
    #         update_conflicts=True,
    #         unique_fields=['company_name', 'subsidiary_id'],
    #         update_fields=['name', 'legal_name', 'record_date']
    #     )


    # @transaction.atomic
    # def transform_vendors(self):
    #     """Transform vendors"""
    #     vendors_data = (
    #         NetSuiteRawVendors.objects
    #         .select_related('companies')
    #         .annotate(
    #             company_name=F('companies__company_name'),
    #             vendor_id=JSONBAgg('raw_payload', 'ID'),
    #             entity_id=JSONBAgg('raw_payload', 'EntityId'),
    #             vendor_name=JSONBAgg('raw_payload', 'CompanyName'),
    #             is_person=Cast(
    #                 JSONBAgg('raw_payload', 'IsPerson'),
    #                 output_field=CharField()
    #             ),
    #             is_inactive=Cast(
    #                 JSONBAgg('raw_payload', 'IsInactive'),
    #                 output_field=CharField()
    #             ),
    #             email=JSONBAgg('raw_payload', 'Email'),
    #             phone=JSONBAgg('raw_payload', 'Phone'),
    #             currency=JSONBAgg('raw_payload', 'Currency'),
    #             subsidiary=JSONBAgg('raw_payload', 'Subsidiary'),
    #             terms=JSONBAgg('raw_payload', 'Terms'),
    #             record_date=F('ingestion_timestamp')
    #         )
    #     )


    #     # Bulk create with PostgreSQL upsert
    #     NetSuiteVendors.objects.bulk_create(
    #         (NetSuiteVendors(**vendor) for vendor in vendors_data),
    #         update_conflicts=True,
    #         unique_fields=['company_name', 'vendor_id'],
    #         update_fields=['vendor_name', 'email', 'phone', 'record_date']
    #     )


    # @transaction.atomic
    # def transform_budget_period_balances(self):
    #     """Transform budget data into period balances"""
    #     budget_data = (
    #         NetSuiteRawBudget.objects
    #         .select_related('companies', 'budget_lines', 'accounts', 'departments')
    #         .annotate(
    #             company_name=F('companies__company_name'),
    #             budget_id=JSONBAgg('raw_payload', 'ID'),
    #             budget_name=JSONBAgg('raw_payload', 'Name'),
    #             budget_status=JSONBAgg('raw_payload', 'Status'),
    #             budget_type=JSONBAgg('raw_payload', 'Type'),
    #             account_id=JSONBAgg('budget_lines__raw_payload', 'Account'),
    #             account_code=JSONBAgg('accounts__raw_payload', 'AcctNumber'),
    #             account_name=JSONBAgg('accounts__raw_payload', 'AccountSearchDisplayName'),
    #             account_class=JSONBAgg('accounts__raw_payload', 'AccountClass'),
    #             department=JSONBAgg('departments__raw_payload', 'Name'),
    #             location=JSONBAgg('budget_lines__raw_payload', 'Location'),
    #             period=JSONBAgg('budget_lines__raw_payload', 'Period'),
    #             amount=Cast(
    #                 Coalesce(JSONBAgg('budget_lines__raw_payload', 'Amount'), 0),
    #                 output_field=DecimalField(max_digits=19, decimal_places=2)
    #             ),
    #             notes=JSONBAgg('budget_lines__raw_payload', 'Notes'),
    #             updated_date_utc=JSONBAgg('budget_lines__raw_payload', 'LastModifiedDate'),
    #             source_system=Value('NETSUITE', output_field=CharField())
    #         )
    #         .order_by('company_name', 'budget_id', 'account_id', 'period')
    #     )


    #     # Bulk create with PostgreSQL upsert
    #     NetSuiteBudgetPeriodBalances.objects.bulk_create(
    #         (NetSuiteBudgetPeriodBalances(**budget) for budget in budget_data),
    #         update_conflicts=True,
    #         unique_fields=['company_name', 'budget_id', 'account_id', 'period'],
    #         update_fields=['amount', 'updated_date_utc', 'record_date']
    #     )




    # @transaction.atomic
    # def transform_transaction_accounting_lines(self):
    #     """Transform transaction accounting lines"""
    #     lines_data = (
    #         NetSuiteRawTransactionAccountingLine.objects
    #         .select_related('companies', 'transaction', 'account')
    #         .annotate(
    #             company_name=F('companies__company_name'),
    #             transaction_id=JSONBAgg('transaction__raw_payload', 'ID'),
    #             account_id=JSONBAgg('account__raw_payload', 'ID'),
    #             amount=Cast(
    #                 Coalesce(JSONBAgg('raw_payload', 'Amount'), 0),
    #                 output_field=DecimalField(max_digits=19, decimal_places=2)
    #             ),
    #             amount_foreign=Cast(
    #                 Coalesce(JSONBAgg('raw_payload', 'AmountForeign'), 0),
    #                 output_field=DecimalField(max_digits=19, decimal_places=2)
    #             ),
    #             credit=Cast(
    #                 Coalesce(JSONBAgg('raw_payload', 'Credit'), 0),
    #                 output_field=DecimalField(max_digits=19, decimal_places=2)
    #             ),
    #             debit=Cast(
    #                 Coalesce(JSONBAgg('raw_payload', 'Debit'), 0),
    #                 output_field=DecimalField(max_digits=19, decimal_places=2)
    #             ),
    #             eliminate=Cast(
    #                 JSONBAgg('raw_payload', 'Eliminate'),
    #                 output_field=CharField()
    #             ),
    #             net_amount=Cast(
    #                 Coalesce(JSONBAgg('raw_payload', 'NetAmount'), 0),
    #                 output_field=DecimalField(max_digits=19, decimal_places=2)
    #             ),
    #             posting=Cast(
    #                 JSONBAgg('raw_payload', 'Posting'),
    #                 output_field=CharField()
    #             ),
    #             subsidiary=JSONBAgg('raw_payload', 'Subsidiary'),
    #             amount_paid=Cast(
    #                 Coalesce(JSONBAgg('raw_payload', 'AmountPaid'), 0),
    #                 output_field=DecimalField(max_digits=19, decimal_places=2)
    #             ),
    #             amount_unpaid=Cast(
    #                 Coalesce(JSONBAgg('raw_payload', 'AmountUnpaid'), 0),
    #                 output_field=DecimalField(max_digits=19, decimal_places=2)
    #             ),
    #             record_date=F('ingestion_timestamp')
    #         )
    #         .order_by('transaction_id', 'account_id')
    #     )


    #     # Bulk create with PostgreSQL upsert
    #     NetSuiteTransactionAccountingLines.objects.bulk_create(
    #         (NetSuiteTransactionAccountingLines(**line) for line in lines_data),
    #         update_conflicts=True,
    #         unique_fields=['company_name', 'transaction_id', 'account_id'],
    #         update_fields=['amount', 'credit', 'debit', 'net_amount', 'record_date']
    #     )


    # @transaction.atomic
    # def transform_entity(self):
    #     """Transform entity data"""
    #     entity_data = (
    #         NetSuiteRawEntity.objects
    #         .select_related('companies')
    #         .annotate(
    #             company_name=F('companies__company_name'),
    #             entity_id=JSONBAgg('raw_payload', 'ID'),
    #             entity_number=JSONBAgg('raw_payload', 'EntityNumber'),
    #             entity_name=JSONBAgg('raw_payload', 'CompanyName'),
    #             legal_name=JSONBAgg('raw_payload', 'LegalName'),
    #             is_person=Cast(
    #                 JSONBAgg('raw_payload', 'IsPerson'),
    #                 output_field=CharField()
    #             ),
    #             is_inactive=Cast(
    #                 JSONBAgg('raw_payload', 'IsInactive'),
    #                 output_field=CharField()
    #             ),
    #             parent_entity=JSONBAgg('raw_payload', 'ParentEntity'),
    #             email=JSONBAgg('raw_payload', 'Email'),
    #             phone=JSONBAgg('raw_payload', 'Phone'),
    #             currency=JSONBAgg('raw_payload', 'Currency'),
    #             subsidiary=JSONBAgg('raw_payload', 'Subsidiary'),
    #             terms=JSONBAgg('raw_payload', 'Terms'),
    #             last_modified_date=JSONBAgg('raw_payload', 'LastModifiedDate'),
    #             record_date=F('ingestion_timestamp')
    #         )
    #     )


    #     # Bulk create with PostgreSQL upsert
    #     NetSuiteEntity.objects.bulk_create(
    #         (NetSuiteEntity(**entity) for entity in entity_data),
    #         update_conflicts=True,
    #         unique_fields=['company_name', 'entity_id'],
    #         update_fields=['entity_name', 'email', 'phone', 'last_modified_date', 'record_date']
    #     )


    # def transform_all(self):
    #     """Run all transformations in the correct order"""
    #     # First transform reference data
    #     self.transform_subsidiaries()
    #     self.transform_departments()
    #     self.transform_accounting_periods()
    #     self.transform_accounts()
    #     # self.transform_vendors()
    #     self.transform_entity()
    #     self.transform_budget_period_balances()
       
    #     # Then transform transactions and related data
    #     self.transform_transactions()
    #     # self.transform_transaction_lines()
    #     # self.transform_transaction_accounting_lines()
       
    #     # Transform journals
    #     self.transform_journals()
       
    #     # Finally transform general ledger
    #     self.transform_general_ledger()
