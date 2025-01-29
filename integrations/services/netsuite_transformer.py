from django.db.models import (
    F, Value, Case, When, Window, Q, Func,
    CharField, IntegerField, DecimalField
)
from django.db.models.functions import (
    Substr, Cast, RowNumber, Coalesce, ExtractYear,
    Concat, Position
)
from django.contrib.postgres.functions import JsonbExtractPath
from django.db import transaction
from ..models.netsuite.raw import *
from ..models.netsuite.analytics import *



class NetSuiteTransformer:
    
    
    @transaction.atomic
    def transform_transactions(self):
        """Transform transactions matching the original SQL"""
        transactions_data = (
            NetSuiteRawTransactions.objects
            .select_related(
                'companies',
                'transaction_line',
                'transaction_accounting_line',
                'subsidiary'
            )
            .annotate(
                company_name=F('companies__company_name'),
                abbrevtype=JsonbExtractPath('raw_payload', 'AbbrevType'),
                transactionid=JsonbExtractPath('raw_payload', 'ID'),
                uniquekey=JsonbExtractPath('transaction_line__raw_payload', 'UniqueKey'),
                linesequencenumber=JsonbExtractPath('transaction_line__raw_payload', 'LineSequenceNumber'),
                lineid=JsonbExtractPath('transaction_line__raw_payload', 'ID'),
                approvalstatus=JsonbExtractPath('raw_payload', 'ApprovalStatus'),
                number=JsonbExtractPath('raw_payload', 'Number'),
                source=JsonbExtractPath('raw_payload', 'Source'),
                status=JsonbExtractPath('raw_payload', 'Status'),
                trandisplayname=JsonbExtractPath('raw_payload', 'TranDisplayName'),
                tranid=JsonbExtractPath('raw_payload', 'TranID'),
                transactionnumber=JsonbExtractPath('raw_payload', 'TransactionNumber'),
                type=JsonbExtractPath('raw_payload', 'Type'),
                recordtype=JsonbExtractPath('raw_payload', 'RecordType'),
                createdby=JsonbExtractPath('raw_payload', 'CreatedBy'),
                createddate=JsonbExtractPath('raw_payload', 'CreatedDate'),
                lastmodifiedby=JsonbExtractPath('raw_payload', 'LastModifiedBy'),
                lastmodifieddate=JsonbExtractPath('raw_payload', 'LastModifiedDate'),
                postingperiod=JsonbExtractPath('raw_payload', 'PostingPeriod'),
               
                # Complex year period calculation for PostgreSQL
                yearperiod=Case(
                    When(
                        raw_payload__PostingPeriod__contains='FY',
                        then=Value(200000) +
                             Cast(
                                 Substr(
                                     JsonbExtractPath('raw_payload', 'PostingPeriod'),
                                     Position('FY', JsonbExtractPath('raw_payload', 'PostingPeriod')) + 2,
                                     2
                                 ),
                                 output_field=IntegerField()
                             ) * 100 +
                             Case(
                                 When(
                                     raw_payload__PostingPeriod__icontains='ADJUSTMENT',
                                     then=Value(13)
                                 ),
                                 default=Cast(
                                     Substr(
                                         JsonbExtractPath('raw_payload', 'PostingPeriod'),
                                         2,
                                         2
                                     ),
                                     output_field=IntegerField()
                                 )
                             )
                    )
                ),
               
                trandate=JsonbExtractPath('raw_payload', 'TranDate'),
                subsidiary=JsonbExtractPath('subsidiary__raw_payload', 'Name'),
                subsidiaryfullname=JsonbExtractPath('subsidiary__raw_payload', 'FullName'),
                subsidiaryid=JsonbExtractPath('transaction_line__raw_payload', 'SubsidiaryId'),
                department=JsonbExtractPath('transaction_line__raw_payload', 'Department'),
                departmentid=JsonbExtractPath('transaction_line__raw_payload', 'DepartmentId'),
                location=JsonbExtractPath('transaction_line__raw_payload', 'Location'),
                class_field=JsonbExtractPath('transaction_line__raw_payload', 'Class'),
               
                memo=JsonbExtractPath('raw_payload', 'Memo'),
                linememo=JsonbExtractPath('transaction_line__raw_payload', 'Memo'),
                externalid=JsonbExtractPath('raw_payload', 'ExternalId'),
                entity=JsonbExtractPath('transaction_line__raw_payload', 'Entity'),
                entityid=JsonbExtractPath('transaction_line__raw_payload', 'EntityId'),
               
                account=JsonbExtractPath('transaction_accounting_line__raw_payload', 'Account'),
                acctnumber=JsonbExtractPath('accounts__raw_payload', 'AcctNumber'),
                accountsearchdisplayname=JsonbExtractPath('accounts__raw_payload', 'AccountSearchDisplayName'),
               
                amount=Cast(
                    JsonbExtractPath('transaction_accounting_line__raw_payload', 'Amount'),
                    output_field=DecimalField()
                ),
                debit=Cast(
                    JsonbExtractPath('transaction_accounting_line__raw_payload', 'Debit'),
                    output_field=DecimalField()
                ),
                credit=Cast(
                    JsonbExtractPath('transaction_accounting_line__raw_payload', 'Credit'),
                    output_field=DecimalField()
                ),
                netamount=Cast(
                    JsonbExtractPath('transaction_accounting_line__raw_payload', 'NetAmount'),
                    output_field=DecimalField()
                ),
               
                currency=JsonbExtractPath('raw_payload', 'Currency'),
                exchangerate=Cast(
                    JsonbExtractPath('raw_payload', 'ExchangeRate'),
                    output_field=DecimalField()
                ),
               
                record_date=F('ingestion_timestamp'),
               
                duplicate_check=Window(
                    expression=RowNumber(),
                    partition_by=['transaction_line__raw_payload__UniqueKey'],
                    order_by='-ingestion_timestamp'
                )
            )
            .filter(duplicate_check=1)
            .order_by('transactionid', 'linesequencenumber')
        )


        # Bulk create with PostgreSQL upsert
        NetSuiteTransactions.objects.bulk_create(
        (NetSuiteTransactions(**transaction) for transaction in transactions_data),
        update_conflicts=True,
        unique_fields=['company_name', 'transactionid', 'linesequencenumber'],
        update_fields=['raw_payload', 'ingestion_timestamp']
    )


    @transaction.atomic
    def transform_general_ledger(self):
        """Transform to general ledger matching original SQL"""
        gl_data = (
            NetSuiteTransactions.objects
            .filter(
                Q(approvalstatus='Approved') | Q(approvalstatus__isnull=True)
            )
            .annotate(
                company_name=F('company_name'),
                abbrevtype=F('abbrevtype'),
                transactionid=F('transactionid'),
                uniquekey=F('uniquekey'),
                linesequencenumber=Cast('linesequencenumber', output_field=IntegerField()),
                lineid=F('lineid'),
                approvalstatus=F('approvalstatus'),
                postingperiod=F('postingperiod'),
                yearperiod=Cast('yearperiod', output_field=IntegerField()),
                trandate=F('trandate'),
                subsidiary=F('subsidiary'),
                account=F('account'),
                acctnumber=F('acctnumber'),
                amount=Cast('amount', output_field=DecimalField(max_digits=19, decimal_places=2)),
                debit=Cast('debit', output_field=DecimalField(max_digits=19, decimal_places=2)),
                credit=Cast('credit', output_field=DecimalField(max_digits=19, decimal_places=2)),
                netamount=Cast('netamount', output_field=DecimalField(max_digits=19, decimal_places=2)),
                currency=F('currency'),
                exchangerate=Cast('exchangerate', output_field=DecimalField(max_digits=19, decimal_places=6)),
                record_date=F('record_date')
            )
            .order_by('transactionid', 'linesequencenumber')
        )


        # Bulk create with PostgreSQL upsert
        NetSuiteGeneralLedger.objects.bulk_create(
            (NetSuiteGeneralLedger(**entry) for entry in gl_data),
            update_conflicts=True,
            unique_fields=['company_name', 'transactionid', 'linesequencenumber'],
            update_fields=['amount', 'debit', 'credit', 'netamount', 'record_date']
        )


    @transaction.atomic
    def transform_accounts(self):
        """Transform accounts matching original SQL"""
        accounts_data = (
            NetSuiteRawAccounts.objects
            .select_related('companies')
            .annotate(
                company_name=F('companies__company_name'),
                account_id=JsonbExtractPath('raw_payload', 'ID'),
                account_number=JsonbExtractPath('raw_payload', 'AcctNumber'),
                account_name=JsonbExtractPath('raw_payload', 'AccountSearchDisplayNameCopy'),
                account_hierarchy=JsonbExtractPath('raw_payload', 'FullName'),
                account_display_name=JsonbExtractPath('raw_payload', 'AccountSearchDisplayName'),
                account_display_hierarchy=JsonbExtractPath('raw_payload', 'DisplayNameWithHierarchy'),
                parent_id=JsonbExtractPath('raw_payload', 'Parent'),
                parent_account=JsonbExtractPath('parent__raw_payload', 'AccountSearchDisplayName'),
                account_type=JsonbExtractPath('raw_payload', 'AcctType'),
                sspecacct=JsonbExtractPath('raw_payload', 'SSPECAcct'),
                description=JsonbExtractPath('raw_payload', 'Description'),
                eliminate=Cast(
                    JsonbExtractPath('raw_payload', 'Eliminate'),
                    output_field=CharField()
                ),
                external_id=JsonbExtractPath('raw_payload', 'ExternalId'),
                include_children=Cast(
                    JsonbExtractPath('raw_payload', 'IncludeChildren'),
                    output_field=CharField()
                ),
                inventory=Cast(
                    JsonbExtractPath('raw_payload', 'Inventory'),
                    output_field=CharField()
                ),
                is_inactive=Cast(
                    JsonbExtractPath('raw_payload', 'IsInactive'),
                    output_field=CharField()
                ),
                is_summary=Cast(
                    JsonbExtractPath('raw_payload', 'IsSummary'),
                    output_field=CharField()
                ),
                last_modified_date=JsonbExtractPath('raw_payload', 'LastModifiedDate'),
                reconcile_with_matching=Cast(
                    JsonbExtractPath('raw_payload', 'ReconcileWithMatching'),
                    output_field=CharField()
                ),
                revalue=Cast(
                    JsonbExtractPath('raw_payload', 'Revalue'),
                    output_field=CharField()
                ),
                subsidiary=JsonbExtractPath('raw_payload', 'Subsidiary'),
                balance=Cast(
                    Coalesce(JsonbExtractPath('raw_payload', 'Balance'), 0),
                    output_field=DecimalField(max_digits=19, decimal_places=2)
                ),
                record_date=F('ingestion_timestamp')
            )
        )


        # Bulk create with PostgreSQL upsert
        NetSuiteAccounts.objects.bulk_create(
            (NetSuiteAccounts(**account) for account in accounts_data),
            update_conflicts=True,
            unique_fields=['company_name', 'account_id'],
            update_fields=['account_name', 'balance', 'last_modified_date', 'record_date']
        )


    @transaction.atomic
    def transform_accounting_periods(self):
        """Transform accounting periods"""
        periods_data = (
            NetSuiteRawAccountingPeriod.objects
            .select_related('companies')
            .annotate(
                company_name=F('companies__company_name'),
                period_id=JsonbExtractPath('raw_payload', 'ID'),
                period_name=JsonbExtractPath('raw_payload', 'PeriodName'),
                start_date=Cast(
                    JsonbExtractPath('raw_payload', 'StartDate'),
                    output_field=CharField()
                ),
                end_date=Cast(
                    JsonbExtractPath('raw_payload', 'EndDate'),
                    output_field=CharField()
                ),
                closed=Cast(
                    JsonbExtractPath('raw_payload', 'Closed'),
                    output_field=CharField()
                ),
                all_locked=Cast(
                    JsonbExtractPath('raw_payload', 'AllLocked'),
                    output_field=CharField()
                ),
                fiscal_calendar=JsonbExtractPath('raw_payload', 'FiscalCalendar'),
                year=Cast(
                    JsonbExtractPath('raw_payload', 'Year'),
                    output_field=IntegerField()
                ),
                quarter=Cast(
                    JsonbExtractPath('raw_payload', 'Quarter'),
                    output_field=IntegerField()
                ),
                period=Cast(
                    JsonbExtractPath('raw_payload', 'Period'),
                    output_field=IntegerField()
                ),
                record_date=F('ingestion_timestamp')
            )
        )


        # Bulk create with PostgreSQL upsert
        NetSuiteAccountingPeriods.objects.bulk_create(
            (NetSuiteAccountingPeriods(**period) for period in periods_data),
            update_conflicts=True,
            unique_fields=['company_name', 'period_id'],
            update_fields=['closed', 'all_locked', 'record_date']
        )


    @transaction.atomic
    def transform_departments(self):
        """Transform departments"""
        departments_data = (
            NetSuiteRawDepartment.objects
            .select_related('companies')
            .annotate(
                company_name=F('companies__company_name'),
                department_id=JsonbExtractPath('raw_payload', 'ID'),
                name=JsonbExtractPath('raw_payload', 'Name'),
                full_name=JsonbExtractPath('raw_payload', 'FullName'),
                subsidiary=JsonbExtractPath('raw_payload', 'Subsidiary'),
                is_inactive=Cast(
                    JsonbExtractPath('raw_payload', 'IsInactive'),
                    output_field=CharField()
                ),
                record_date=F('ingestion_timestamp')
            )
        )


        # Bulk create with PostgreSQL upsert
        NetSuiteDepartments.objects.bulk_create(
            (NetSuiteDepartments(**dept) for dept in departments_data),
            update_conflicts=True,
            unique_fields=['company_name', 'department_id'],
            update_fields=['name', 'is_inactive', 'record_date']
        )


    @transaction.atomic
    def transform_subsidiaries(self):
        """Transform subsidiaries"""
        subsidiaries_data = (
            NetSuiteRawSubsidiary.objects
            .select_related('companies')
            .annotate(
                company_name=F('companies__company_name'),
                subsidiary_id=JsonbExtractPath('raw_payload', 'ID'),
                name=JsonbExtractPath('raw_payload', 'Name'),
                name_nohi=JsonbExtractPath('raw_payload', 'NameNoHierarchy'),
                full_name=JsonbExtractPath('raw_payload', 'FullName'),
                legal_name=JsonbExtractPath('raw_payload', 'LegalName'),
                federal_number=JsonbExtractPath('raw_payload', 'FederalNumber'),
                is_elimination=Cast(
                    JsonbExtractPath('raw_payload', 'IsElimination'),
                    output_field=CharField()
                ),
                currency=JsonbExtractPath('raw_payload', 'Currency'),
                country=JsonbExtractPath('raw_payload', 'Country'),
                record_date=F('ingestion_timestamp')
            )
        )


        # Bulk create with PostgreSQL upsert
        NetSuiteSubsidiaries.objects.bulk_create(
            (NetSuiteSubsidiaries(**sub) for sub in subsidiaries_data),
            update_conflicts=True,
            unique_fields=['company_name', 'subsidiary_id'],
            update_fields=['name', 'legal_name', 'record_date']
        )


    # @transaction.atomic
    # def transform_vendors(self):
    #     """Transform vendors"""
    #     vendors_data = (
    #         NetSuiteRawVendors.objects
    #         .select_related('companies')
    #         .annotate(
    #             company_name=F('companies__company_name'),
    #             vendor_id=JsonbExtractPath('raw_payload', 'ID'),
    #             entity_id=JsonbExtractPath('raw_payload', 'EntityId'),
    #             vendor_name=JsonbExtractPath('raw_payload', 'CompanyName'),
    #             is_person=Cast(
    #                 JsonbExtractPath('raw_payload', 'IsPerson'),
    #                 output_field=CharField()
    #             ),
    #             is_inactive=Cast(
    #                 JsonbExtractPath('raw_payload', 'IsInactive'),
    #                 output_field=CharField()
    #             ),
    #             email=JsonbExtractPath('raw_payload', 'Email'),
    #             phone=JsonbExtractPath('raw_payload', 'Phone'),
    #             currency=JsonbExtractPath('raw_payload', 'Currency'),
    #             subsidiary=JsonbExtractPath('raw_payload', 'Subsidiary'),
    #             terms=JsonbExtractPath('raw_payload', 'Terms'),
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


    @transaction.atomic
    def transform_budget_period_balances(self):
        """Transform budget data into period balances"""
        budget_data = (
            NetSuiteRawBudget.objects
            .select_related('companies', 'budget_lines', 'accounts', 'departments')
            .annotate(
                company_name=F('companies__company_name'),
                budget_id=JsonbExtractPath('raw_payload', 'ID'),
                budget_name=JsonbExtractPath('raw_payload', 'Name'),
                budget_status=JsonbExtractPath('raw_payload', 'Status'),
                budget_type=JsonbExtractPath('raw_payload', 'Type'),
                account_id=JsonbExtractPath('budget_lines__raw_payload', 'Account'),
                account_code=JsonbExtractPath('accounts__raw_payload', 'AcctNumber'),
                account_name=JsonbExtractPath('accounts__raw_payload', 'AccountSearchDisplayName'),
                account_class=JsonbExtractPath('accounts__raw_payload', 'AccountClass'),
                department=JsonbExtractPath('departments__raw_payload', 'Name'),
                location=JsonbExtractPath('budget_lines__raw_payload', 'Location'),
                period=JsonbExtractPath('budget_lines__raw_payload', 'Period'),
                amount=Cast(
                    Coalesce(JsonbExtractPath('budget_lines__raw_payload', 'Amount'), 0),
                    output_field=DecimalField(max_digits=19, decimal_places=2)
                ),
                notes=JsonbExtractPath('budget_lines__raw_payload', 'Notes'),
                updated_date_utc=JsonbExtractPath('budget_lines__raw_payload', 'LastModifiedDate'),
                source_system=Value('NETSUITE', output_field=CharField())
            )
            .order_by('company_name', 'budget_id', 'account_id', 'period')
        )


        # Bulk create with PostgreSQL upsert
        NetSuiteBudgetPeriodBalances.objects.bulk_create(
            (NetSuiteBudgetPeriodBalances(**budget) for budget in budget_data),
            update_conflicts=True,
            unique_fields=['company_name', 'budget_id', 'account_id', 'period'],
            update_fields=['amount', 'updated_date_utc', 'record_date']
        )




    # @transaction.atomic
    # def transform_transaction_accounting_lines(self):
    #     """Transform transaction accounting lines"""
    #     lines_data = (
    #         NetSuiteRawTransactionAccountingLine.objects
    #         .select_related('companies', 'transaction', 'account')
    #         .annotate(
    #             company_name=F('companies__company_name'),
    #             transaction_id=JsonbExtractPath('transaction__raw_payload', 'ID'),
    #             account_id=JsonbExtractPath('account__raw_payload', 'ID'),
    #             amount=Cast(
    #                 Coalesce(JsonbExtractPath('raw_payload', 'Amount'), 0),
    #                 output_field=DecimalField(max_digits=19, decimal_places=2)
    #             ),
    #             amount_foreign=Cast(
    #                 Coalesce(JsonbExtractPath('raw_payload', 'AmountForeign'), 0),
    #                 output_field=DecimalField(max_digits=19, decimal_places=2)
    #             ),
    #             credit=Cast(
    #                 Coalesce(JsonbExtractPath('raw_payload', 'Credit'), 0),
    #                 output_field=DecimalField(max_digits=19, decimal_places=2)
    #             ),
    #             debit=Cast(
    #                 Coalesce(JsonbExtractPath('raw_payload', 'Debit'), 0),
    #                 output_field=DecimalField(max_digits=19, decimal_places=2)
    #             ),
    #             eliminate=Cast(
    #                 JsonbExtractPath('raw_payload', 'Eliminate'),
    #                 output_field=CharField()
    #             ),
    #             net_amount=Cast(
    #                 Coalesce(JsonbExtractPath('raw_payload', 'NetAmount'), 0),
    #                 output_field=DecimalField(max_digits=19, decimal_places=2)
    #             ),
    #             posting=Cast(
    #                 JsonbExtractPath('raw_payload', 'Posting'),
    #                 output_field=CharField()
    #             ),
    #             subsidiary=JsonbExtractPath('raw_payload', 'Subsidiary'),
    #             amount_paid=Cast(
    #                 Coalesce(JsonbExtractPath('raw_payload', 'AmountPaid'), 0),
    #                 output_field=DecimalField(max_digits=19, decimal_places=2)
    #             ),
    #             amount_unpaid=Cast(
    #                 Coalesce(JsonbExtractPath('raw_payload', 'AmountUnpaid'), 0),
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


    @transaction.atomic
    def transform_entity(self):
        """Transform entity data"""
        entity_data = (
            NetSuiteRawEntity.objects
            .select_related('companies')
            .annotate(
                company_name=F('companies__company_name'),
                entity_id=JsonbExtractPath('raw_payload', 'ID'),
                entity_number=JsonbExtractPath('raw_payload', 'EntityNumber'),
                entity_name=JsonbExtractPath('raw_payload', 'CompanyName'),
                legal_name=JsonbExtractPath('raw_payload', 'LegalName'),
                is_person=Cast(
                    JsonbExtractPath('raw_payload', 'IsPerson'),
                    output_field=CharField()
                ),
                is_inactive=Cast(
                    JsonbExtractPath('raw_payload', 'IsInactive'),
                    output_field=CharField()
                ),
                parent_entity=JsonbExtractPath('raw_payload', 'ParentEntity'),
                email=JsonbExtractPath('raw_payload', 'Email'),
                phone=JsonbExtractPath('raw_payload', 'Phone'),
                currency=JsonbExtractPath('raw_payload', 'Currency'),
                subsidiary=JsonbExtractPath('raw_payload', 'Subsidiary'),
                terms=JsonbExtractPath('raw_payload', 'Terms'),
                last_modified_date=JsonbExtractPath('raw_payload', 'LastModifiedDate'),
                record_date=F('ingestion_timestamp')
            )
        )


        # Bulk create with PostgreSQL upsert
        NetSuiteEntity.objects.bulk_create(
            (NetSuiteEntity(**entity) for entity in entity_data),
            update_conflicts=True,
            unique_fields=['company_name', 'entity_id'],
            update_fields=['entity_name', 'email', 'phone', 'last_modified_date', 'record_date']
        )


    @transaction.atomic
    def transform_journals(self):
        """Transform journals matching the original SQL"""
        journals_data = (
            NetSuiteRawJournal.objects
            .select_related('companies')
            .annotate(
                company_name=F('companies__company_name'),
                journal_id=JsonbExtractPath('raw_payload', 'id'),
                date=JsonbExtractPath('raw_payload', 'date'),
                memo=JsonbExtractPath('raw_payload', 'memo'),
                account=JsonbExtractPath('raw_payload__journalline', 'account'),
                debit=Cast(
                    JsonbExtractPath('raw_payload__journalline', 'debit'),
                    output_field=DecimalField()
                ),
                credit=Cast(
                    JsonbExtractPath('raw_payload__journalline', 'credit'),
                    output_field=DecimalField()
                ),
                currency=JsonbExtractPath('raw_payload', 'currency'),
                exchangerate=Cast(
                    JsonbExtractPath('raw_payload', 'exchangerate'),
                    output_field=DecimalField()
                ),
                record_date=F('ingestion_timestamp'),
            )
            .order_by('journal_id')
        )


        # Bulk create with PostgreSQL upsert
        NetSuiteJournals.objects.bulk_create(
            (NetSuiteJournals(**journal) for journal in journals_data),
            update_conflicts=True,
            unique_fields=['company_name', 'journal_id'],
            update_fields=['memo', 'record_date']
        )


    def transform_all(self):
        """Run all transformations in the correct order"""
        # First transform reference data
        self.transform_subsidiaries()
        self.transform_departments()
        self.transform_accounting_periods()
        self.transform_accounts()
        # self.transform_vendors()
        self.transform_entity()
        self.transform_budget_period_balances()
       
        # Then transform transactions and related data
        self.transform_transactions()
        # self.transform_transaction_lines()
        # self.transform_transaction_accounting_lines()
       
        # Transform journals
        self.transform_journals()
       
        # Finally transform general ledger
        self.transform_general_ledger()
