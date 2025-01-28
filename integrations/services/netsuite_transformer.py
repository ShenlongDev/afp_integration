from django.db.models import (
    F, Value, Case, When, Window, Q, Func,
    CharField, IntegerField, DecimalField
)
from django.db.models.functions import (
    Substr, Cast, RowNumber, Coalesce, ExtractYear,
    Concat, Position
)
from django.db import transaction
from ..models.netsuite.raw import NetSuiteRawVendors, NetSuiteRawAccounts, NetSuiteRawSubsidiary, NetSuiteRawTransactions, NetSuiteRawTransactionAccountingLine, NetSuiteRawEntity, NetSuiteRawBudget, NetSuiteRawAccountingPeriod, NetSuiteRawDepartment, NetSuiteTransactionAccountingLines
from ..models.netsuite.analytics import *
from typing import Optional


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
                abbrevtype=F('raw_payload__AbbrevType'),
                transactionid=F('raw_payload__ID'),
                uniquekey=F('transaction_line__raw_payload__UniqueKey'),
                linesequencenumber=F('transaction_line__raw_payload__LineSequenceNumber'),
                lineid=F('transaction_line__raw_payload__ID'),
                approvalstatus=F('raw_payload__ApprovalStatus'),
                number=F('raw_payload__Number'),
                source=F('raw_payload__Source'),
                status=F('raw_payload__Status'),
                trandisplayname=F('raw_payload__TranDisplayName'),
                tranid=F('raw_payload__TranID'),
                transactionnumber=F('raw_payload__TransactionNumber'),
                type=F('raw_payload__Type'),
                recordtype=F('raw_payload__RecordType'),
                createdby=F('raw_payload__CreatedBy'),
                createddate=F('raw_payload__CreatedDate'),
                lastmodifiedby=F('raw_payload__LastModifiedBy'),
                lastmodifieddate=F('raw_payload__LastModifiedDate'),
                postingperiod=F('raw_payload__PostingPeriod'),
               
                # Complex year period calculation
                yearperiod=Case(
                    When(
                        raw_payload__PostingPeriod__contains='FY',
                        then=Value(200000) +
                             Cast(
                                 Substr(
                                     'raw_payload__PostingPeriod',
                                     Position('FY', 'raw_payload__PostingPeriod') + 2,
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
                                         'raw_payload__PostingPeriod',
                                         2,
                                         2
                                     ),
                                     output_field=IntegerField()
                                 )
                             )
                    )
                ),
               
                trandate=F('raw_payload__TranDate'),
                subsidiary=F('subsidiary__raw_payload__Name'),
                subsidiaryfullname=F('subsidiary__raw_payload__FullName'),
                subsidiaryid=F('transaction_line__raw_payload__SubsidiaryId'),
                department=F('transaction_line__raw_payload__Department'),
                departmentid=F('transaction_line__raw_payload__DepartmentId'),
                location=F('transaction_line__raw_payload__Location'),
                class_field=F('transaction_line__raw_payload__Class'),
               
                memo=F('raw_payload__Memo'),
                linememo=F('transaction_line__raw_payload__Memo'),
                externalid=F('raw_payload__ExternalId'),
                entity=F('transaction_line__raw_payload__Entity'),
                entityid=F('transaction_line__raw_payload__EntityId'),
               
                account=F('transaction_accounting_line__raw_payload__Account'),
                acctnumber=F('accounts__raw_payload__AcctNumber'),
                accountsearchdisplayname=F('accounts__raw_payload__AccountSearchDisplayName'),
                expenseaccount=F('transaction_line__raw_payload__ExpenseAccount'),
                expenseaccountid=F('transaction_line__raw_payload__ExpenseAccountId'),
               
                amount=F('transaction_accounting_line__raw_payload__Amount'),
                amountlinked=F('transaction_accounting_line__raw_payload__AmountLinked'),
                debit=F('transaction_accounting_line__raw_payload__Debit'),
                credit=F('transaction_accounting_line__raw_payload__Credit'),
                netamount=F('transaction_accounting_line__raw_payload__NetAmount'),
                linenetamount=F('transaction_line__raw_payload__NetAmount'),
               
                currency=F('raw_payload__Currency'),
                exchangerate=F('raw_payload__ExchangeRate'),
                foreignamountpaid=F('raw_payload__ForeignAmountPaid'),
                foreignamountunpaid=F('raw_payload__ForeignAmountUnpaid'),
                foreigntotal=F('raw_payload__ForeignTotal'),
                foreignlineamount=F('transaction_line__raw_payload__ForeignAmount'),
               
                record_date=F('ingestion_timestamp'),
               
                # Deduplication check
                duplicate_check=Window(
                    expression=RowNumber(),
                    partition_by=[F('transaction_line__raw_payload__UniqueKey')],
                    order_by=F('ingestion_timestamp').desc()
                )
            )
            .filter(duplicate_check=1)
            .order_by('transactionid', 'linesequencenumber')
        )


        # Bulk create transformed transactions
        NetSuiteTransactions.objects.bulk_create(
            NetSuiteTransactions(**transaction) for transaction in transactions_data
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
                # Select only needed fields for GL
                company_name=F('company_name'),
                abbrevtype=F('abbrevtype'),
                transactionid=F('transactionid'),
                uniquekey=F('uniquekey'),
                linesequencenumber=F('linesequencenumber'),
                lineid=F('lineid'),
                approvalstatus=F('approvalstatus'),
                postingperiod=F('postingperiod'),
                yearperiod=F('yearperiod'),
                trandate=F('trandate'),
                subsidiary=F('subsidiary'),
                account=F('account'),
                acctnumber=F('acctnumber'),
                amount=F('amount'),
                debit=F('debit'),
                credit=F('credit'),
                netamount=F('netamount'),
                currency=F('currency'),
                exchangerate=F('exchangerate'),
                record_date=F('record_date')
            )
            .order_by('transactionid', 'linesequencenumber')
        )


        # Bulk create GL entries
        NetSuiteGeneralLedger.objects.bulk_create(
            NetSuiteGeneralLedger(**entry) for entry in gl_data
        )


    @transaction.atomic
    def transform_accounts(self):
        """Transform accounts matching original SQL"""
        accounts_data = (
            NetSuiteRawAccounts.objects
            .select_related('companies')
            .annotate(
                company_name=F('companies__company_name'),
                account_id=F('raw_payload__ID'),
                account_number=F('raw_payload__AcctNumber'),
                account_name=F('raw_payload__AccountSearchDisplayNameCopy'),
                account_hierarchy=F('raw_payload__FullName'),
                account_display_name=F('raw_payload__AccountSearchDisplayName'),
                account_display_hierarchy=F('raw_payload__DisplayNameWithHierarchy'),
                parent_id=F('raw_payload__Parent'),
                parent_account=F('parent__raw_payload__AccountSearchDisplayName'),
                account_type=F('raw_payload__AcctType'),
                sspecacct=F('raw_payload__SSPECAcct'),
                description=F('raw_payload__Description'),
                eliminate=F('raw_payload__Eliminate'),
                external_id=F('raw_payload__ExternalId'),
                include_children=F('raw_payload__IncludeChildren'),
                inventory=F('raw_payload__Inventory'),
                is_inactive=F('raw_payload__IsInactive'),
                is_summary=F('raw_payload__IsSummary'),
                last_modified_date=F('raw_payload__LastModifiedDate'),
                reconcile_with_matching=F('raw_payload__ReconcileWithMatching'),
                revalue=F('raw_payload__Revalue'),
                subsidiary=F('raw_payload__Subsidiary'),
                balance=F('raw_payload__Balance'),
                record_date=F('ingestion_timestamp')
            )
        )


        # Bulk create transformed accounts
        NetSuiteAccounts.objects.bulk_create(
            NetSuiteAccounts(**account) for account in accounts_data
        )


    @transaction.atomic
    def transform_accounting_periods(self):
        """Transform accounting periods"""
        periods_data = (
            NetSuiteRawAccountingPeriod.objects
            .select_related('companies')
            .annotate(
                company_name=F('companies__company_name'),
                period_id=F('raw_payload__ID'),
                period_name=F('raw_payload__PeriodName'),
                start_date=F('raw_payload__StartDate'),
                end_date=F('raw_payload__EndDate'),
                closed=F('raw_payload__Closed'),
                all_locked=F('raw_payload__AllLocked'),
                fiscal_calendar=F('raw_payload__FiscalCalendar'),
                year=F('raw_payload__Year'),
                quarter=F('raw_payload__Quarter'),
                period=F('raw_payload__Period'),
                record_date=F('ingestion_timestamp')
            )
        )


        # Bulk create transformed accounting periods
        NetSuiteAccountingPeriods.objects.bulk_create(
            NetSuiteAccountingPeriods(**period) for period in periods_data
        )


    @transaction.atomic
    def transform_departments(self):
        """Transform departments"""
        departments_data = (
            NetSuiteRawDepartment.objects
            .select_related('companies')
            .annotate(
                company_name=F('companies__company_name'),
                department_id=F('raw_payload__ID'),
                name=F('raw_payload__Name'),
                full_name=F('raw_payload__FullName'),
                subsidiary=F('raw_payload__Subsidiary'),
                is_inactive=F('raw_payload__IsInactive'),
                record_date=F('ingestion_timestamp')
            )
        )


        # Bulk create transformed departments
        NetSuiteDepartments.objects.bulk_create(
            NetSuiteDepartments(**dept) for dept in departments_data
        )


    @transaction.atomic
    def transform_subsidiaries(self):
        """Transform subsidiaries"""
        subsidiaries_data = (
            NetSuiteRawSubsidiary.objects
            .select_related('companies')
            .annotate(
                company_name=F('companies__company_name'),
                subsidiary_id=F('raw_payload__ID'),
                name=F('raw_payload__Name'),
                name_nohi=F('raw_payload__NameNoHierarchy'),
                full_name=F('raw_payload__FullName'),
                legal_name=F('raw_payload__LegalName'),
                federal_number=F('raw_payload__FederalNumber'),
                is_elimination=F('raw_payload__IsElimination'),
                currency=F('raw_payload__Currency'),
                country=F('raw_payload__Country'),
                record_date=F('ingestion_timestamp')
            )
        )


        # Bulk create transformed subsidiaries
        NetSuiteSubsidiaries.objects.bulk_create(
            NetSuiteSubsidiaries(**sub) for sub in subsidiaries_data
        )


    @transaction.atomic
    def transform_vendors(self):
        """Transform vendors"""
        vendors_data = (
            NetSuiteRawVendors.objects
            .select_related('companies')
            .annotate(
                company_name=F('companies__company_name'),
                vendor_id=F('raw_payload__ID'),
                entity_id=F('raw_payload__EntityId'),
                company_name=F('raw_payload__CompanyName'),
                is_person=F('raw_payload__IsPerson'),
                is_inactive=F('raw_payload__IsInactive'),
                email=F('raw_payload__Email'),
                phone=F('raw_payload__Phone'),
                currency=F('raw_payload__Currency'),
                subsidiary=F('raw_payload__Subsidiary'),
                terms=F('raw_payload__Terms'),
                record_date=F('ingestion_timestamp')
            )
        )


        # Bulk create transformed vendors
        NetSuiteVendors.objects.bulk_create(
            NetSuiteVendors(**vendor) for vendor in vendors_data
        )


    def transform_all(self):
        """Run all transformations in the correct order"""
        # First transform reference data
        self.transform_subsidiaries()
        self.transform_departments()
        self.transform_accounting_periods()
        self.transform_accounts()
        self.transform_vendors()
        self.transform_entity()
        self.transform_budget_period_balances()  # Add this
       
        # Then transform transactions and related data
        self.transform_transactions()
        self.transform_transaction_lines()
        self.transform_transaction_accounting_lines()
       
        # Finally transform general ledger
        self.transform_general_ledger()


    @transaction.atomic
    def transform_transaction_accounting_lines(self):
        """Transform transaction accounting lines"""
        lines_data = (
            NetSuiteRawTransactionAccountingLine.objects
            .select_related('companies', 'transaction', 'account')
            .annotate(
                company_name=F('companies__company_name'),
                transaction_id=F('transaction__raw_payload__ID'),
                account_id=F('account__raw_payload__ID'),
                amount=F('raw_payload__Amount'),
                amount_foreign=F('raw_payload__AmountForeign'),
                credit=F('raw_payload__Credit'),
                debit=F('raw_payload__Debit'),
                eliminate=F('raw_payload__Eliminate'),
                net_amount=F('raw_payload__NetAmount'),
                posting=F('raw_payload__Posting'),
                subsidiary=F('raw_payload__Subsidiary'),
                amount_paid=F('raw_payload__AmountPaid'),
                amount_unpaid=F('raw_payload__AmountUnpaid'),
                record_date=F('ingestion_timestamp')
            )
        )


        # Bulk create transformed lines
        NetSuiteTransactionAccountingLines.objects.bulk_create(
            NetSuiteTransactionAccountingLines(**line) for line in lines_data
        )


    @transaction.atomic
    def transform_entity(self):
        """Transform entity data"""
        entity_data = (
            NetSuiteRawEntity.objects
            .select_related('companies')
            .annotate(
                company_name=F('companies__company_name'),
                entity_id=F('raw_payload__ID'),
                entity_number=F('raw_payload__EntityNumber'),
                company_name=F('raw_payload__CompanyName'),
                legal_name=F('raw_payload__LegalName'),
                is_person=F('raw_payload__IsPerson'),
                is_inactive=F('raw_payload__IsInactive'),
                parent_entity=F('raw_payload__ParentEntity'),
                email=F('raw_payload__Email'),
                phone=F('raw_payload__Phone'),
                currency=F('raw_payload__Currency'),
                subsidiary=F('raw_payload__Subsidiary'),
                terms=F('raw_payload__Terms'),
                last_modified_date=F('raw_payload__LastModifiedDate'),
                record_date=F('ingestion_timestamp')
            )
        )


        NetSuiteEntity.objects.bulk_create(
            NetSuiteEntity(**entity) for entity in entity_data
        )


    @transaction.atomic
    def transform_budget_period_balances(self):
        """Transform budget data into period balances with full field list"""
        budget_data = (
            NetSuiteRawBudget.objects
            .select_related(
                'companies',
                'budget_lines',
                'accounts',
                'departments'
            )
            .annotate(
                company_name=F('companies__company_name'),
                budget_id=F('raw_payload__ID'),
                budget_name=F('raw_payload__Name'),
                budget_status=F('raw_payload__Status'),
                budget_type=F('raw_payload__Type'),
                account_id=F('budget_lines__raw_payload__Account'),
                account_code=F('accounts__raw_payload__AcctNumber'),
                account_name=F('accounts__raw_payload__AccountSearchDisplayName'),
                account_class=F('accounts__raw_payload__AccountClass'),
                department=F('departments__raw_payload__Name'),
                location=F('budget_lines__raw_payload__Location'),
                period=F('budget_lines__raw_payload__Period'),
                amount=F('budget_lines__raw_payload__Amount'),
                notes=F('budget_lines__raw_payload__Notes'),
                updated_date_utc=F('budget_lines__raw_payload__LastModifiedDate'),
                source_system=Value('NETSUITE', output_field=CharField())
            )
            .order_by(
                'company_name',
                'budget_id',
                'account_id',
                'period'
            )
        )


        # Bulk create with unique constraint handling
        NetSuiteBudgetPeriodBalances.objects.bulk_create(
            NetSuiteBudgetPeriodBalances(**budget)
            for budget in budget_data
        )


    @transaction.atomic
    def import_transactions(self, min_id: Optional[str] = None):
        """Import transactions with all custom fields"""
        query = """
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
            type,
            createddate,
            BUILTIN.DF(Transaction.currency) AS currency,
            abbrevtype,
            BUILTIN.DF(Transaction.approvalstatus) AS approvalstatus,
            BUILTIN.DF(Transaction.balsegstatus) AS balsegstatus,
            billingstatus,
            closedate,
            customtype,
            daysopen,
            daysoverduesearch,
            duedate,
            exchangerate,
            externalid,
            foreignamountpaid,
            foreignamountunpaid,
            foreigntotal,
            isfinchrg,
            isreversal,
            BUILTIN.DF(Transaction.lastmodifiedby) AS lastmodifiedby,
            lastmodifieddate,
            nexus,
            number,
            ordpicked,
            paymenthold,
            printedpickingticket,
            recordtype,
            source,
            tobeprinted,
            userevenuearrangement,
            visibletocustomer,
            void,
            voided,
            BUILTIN.DF(Transaction.terms) AS terms,
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
