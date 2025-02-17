from django.contrib import admin
from .models.models import (
    Integration,
    IntegrationAccessToken,
    ChartOfAccounts,
    OrphanBankTransaction,
    SyncTableLogs
)

from integrations.models.xero.transformations import (
    XeroAccounts,
    XeroBankTransactionLineItems,
    XeroJournalLines,
    XeroJournalLineTrackingCategories,
    XeroInvoiceLineItems,
    XeroBudgetPeriodBalances,
)
from integrations.models.xero.analytics import (
    XeroGeneralLedger,
    XeroBudgetPeriodBalancesAnalytics,
)
from integrations.models.xero.raw import (
    XeroAccountsRaw,
    XeroBudgetPeriodBalancesRaw,
    XeroBudgetsRaw,
    XeroJournalsRaw,
    XeroBankTransactionsRaw,
    XeroConnectionsRaw,
    XeroContactsRaw,
    XeroInvoicesRaw,
)

from integrations.models.netsuite.analytics import (
    NetSuiteVendors,
    NetSuiteAccounts,
    NetSuiteTransactions,
    NetSuiteSubsidiaries,
    NetSuiteDepartments,
    NetSuiteGeneralLedger,
    NetSuiteJournals,
    NetSuiteEntity,
    NetSuiteAccountingPeriods,
    NetSuiteTransactionLine,
    NetSuiteTransactionAccountingLine,
    NetSuiteTransformedTransaction
)


class NetSuiteTransformedTransactionAdmin(admin.ModelAdmin):
    list_display = ('consolidation_key', 'transactionid', 'createdby', 'createddate', 'lastmodifieddate', 'entity_id')
    search_fields = ('consolidation_key', 'transactionid', 'createdby', 'createddate', 'lastmodifieddate', 'entity_id')
    

class SyncTableLogsAdmin(admin.ModelAdmin):
    list_display = ('module_name', 'integration', 'organization', 'fetched_records', 'last_updated_time', 'last_updated_date')
    search_fields = ('module_name', 'integration__org__name', 'organization__name', 'organization')

class NetSuiteTransactionAccountingLineAdmin(admin.ModelAdmin):
    list_display = ('org', 'transaction', 'account', 'amount', 'lastmodifieddate', 'transaction_line')
    search_fields = ('transaction', 'account', 'amount')
    
class NetSuiteTransactionLinesAdmin(admin.ModelAdmin):
    list_display = ('transaction_line_id',  'subsidiary', 'transactionid')
    search_fields = ('transaction_line_id', 'transactionid', 'subsidiary')



class NetSuiteAccountingPeriodsAdmin(admin.ModelAdmin):

    list_display = ('company_name', 'period_name', 'start_date', 'end_date', 'closed', 'all_locked', 'fiscal_calendar', 'record_date')
    search_fields = ( 'period_name', 'start_date', 'end_date', 'closed', 'all_locked', 'fiscal_calendar', 'record_date')

class NetSuiteEntityAdmin(admin.ModelAdmin):
    list_display = ('id','company_name', 'entity_id', 'legal_name', 'is_person', 'is_inactive', 'parent_entity', 'email', 'phone', 'currency', 'terms', 'last_modified_date', 'record_date')
    search_fields = ('id', 'entity_id', 'email', 'phone', 'currency', 'terms')  # Removed entity_number and subsidiary to avoid ForeignKey issues

class NetSuiteSubsidiariesAdmin(admin.ModelAdmin):
    list_display = ('subsidiary_id', 'name', 'name_nohi', 'full_name', 'legal_name', 'federal_number', 'is_elimination', 'currency', 'country', 'record_date')
    search_fields = ('subsidiary_id', 'name', 'name_nohi', 'full_name', 'legal_name', 'federal_number', 'is_elimination', 'currency', 'country', 'record_date')


class NetSuiteDepartmentsAdmin(admin.ModelAdmin):
    list_display = ('department_id', 'company_name', 'name', 'full_name', 'is_inactive', 'record_date')
    search_fields = ('department_id', 'name', 'full_name', 'is_inactive', 'record_date')


class NetSuiteGeneralLedgerAdmin(admin.ModelAdmin):
    list_display = ('tenant_name', 'record_date', 'trandate', 'subsidiary')  
    search_fields = ('tenant_name', 'is_inactive', 'record_date')


class NetSuiteJournalsAdmin(admin.ModelAdmin):
    list_display = ('journal_id', 'company_name', 'record_date')
    search_fields = ('journal_id', 'record_date')


class NetSuiteTransactionsAdmin(admin.ModelAdmin):
    list_display = ('company_name', 'approvalstatus', 'createdby', 'transactionid', 'createddate', 'memo')
    search_fields = ('abbrevtype', 'createdby', 'transactionid', 'memo', 'transactionnumber')


class NetSuiteVendorsAdmin(admin.ModelAdmin):
    list_display = ('vendor_id', 'company_name', 'entity_id', 'is_person', 'is_inactive', 'email', 'phone', 'currency', 'subsidiary', 'terms', 'record_date')
    search_fields = ('vendor_id', 'entity_id', 'is_person', 'is_inactive', 'email', 'phone', 'currency', 'subsidiary', 'terms', 'record_date')


class NetSuiteAccountsAdmin(admin.ModelAdmin):
    list_display = ('account_id', 'is_inactive', 'accountsearchdisplayname', 'accountsearchdisplaynamecopy')
    search_fields = ('account_id', 'is_inactive', 'accountsearchdisplayname', 'accountsearchdisplaynamecopy')


class IntegrationAdmin(admin.ModelAdmin):
    list_display = ('org', 'created_at', 'updated_at')
    search_fields = ('org__name', 'xero_client_id', 'xero_client_secret', 'xero_tenant_id', 'created_at', 'updated_at')


class IntegrationAccessTokenAdmin(admin.ModelAdmin):
    list_display = ('integration', 'integration_type', 'expires_at', 'created_at')
    search_fields = ('integration__org__name', 'integration_type', 'token', 'refresh_token', 'expires_at', 'created_at')


class ChartOfAccountsAdmin(admin.ModelAdmin):
    list_display = ('integration', 'account_id', 'code', 'name', 'status', 'account_type', 'tax_type')
    search_fields = ('integration__org__name', 'account_id', 'code', 'name', 'status', 'account_type', 'tax_type')


@admin.register(OrphanBankTransaction)
class OrphanBankTransactionAdmin(admin.ModelAdmin):
    list_display = ('integration', 'created_at', 'mapped')
    search_fields = ('integration__org__name',)
    list_filter = ('mapped', 'integration__org')
    readonly_fields = ('raw_data', 'integration', 'created_at')

    actions = ['map_to_account']

    def map_to_account(self, request, queryset):
        """
        Admin action to map selected orphan transactions to an existing account.
        """
        # Implementation depends on your UI framework; may require a custom form
        pass

    map_to_account.short_description = "Map selected orphan transactions to an account"


class XeroAccountsAdmin(admin.ModelAdmin):
    list_display = ('account_id', 'tenant_id', 'name', 'status', 'type', 'tax_type')
    search_fields = ('account_id', 'tenant_id', 'name', 'status', 'type', 'tax_type')


class XeroBankTransactionLineItemsAdmin(admin.ModelAdmin):
    list_display = ('line_item_id', 'tenant_id', 'bank_transaction_id', 'total', 'currency_code')
    search_fields = ('line_item_id', 'tenant_id', 'bank_transaction_id', 'total', 'currency_code')


class XeroJournalLinesAdmin(admin.ModelAdmin):
    list_display = ('journal_line_id', 'description', 'account_id', 'account_name', 'net_amount', 'journal_date', 'created_date_utc')
    search_fields = ('journal_line_id', 'description', 'account_id', 'journal_date', 'created_date_utc', 'account_name')



class XeroJournalLineTrackingCategoriesAdmin(admin.ModelAdmin):
    list_display = ('tracking_category_id', 'journal_line_id', 'name', 'option')
    search_fields = ('tracking_category_id', 'journal_line_id', 'name', 'option')


class XeroInvoiceLineItemsAdmin(admin.ModelAdmin):
    list_display = ('invoice_id', 'description', )
    search_fields = ('invoice_id', 'description',)


class XeroBudgetPeriodBalancesAdmin(admin.ModelAdmin):
    list_display = ('tenant_id', 'amount', 'period')
    search_fields = ('tenant_id', 'period')


class XeroGeneralLedgerAdmin(admin.ModelAdmin):
    list_display = ('tenant_id', 'account_id', 'account_name', 'journal_date', 'created_date', 'net_amount', 'invoice_url')
    search_fields = ('tenant_id', 'account_id', 'account_name', 'journal_date', 'created_date')


class XeroAccountsRawAdmin(admin.ModelAdmin):
    list_display = ('tenant_id', 'account_id', 'name', 'status', 'ingestion_timestamp')
    search_fields = ('tenant_id', 'account_id', 'name', 'status')


class XeroBudgetPeriodBalancesRawAdmin(admin.ModelAdmin):
    list_display = ('tenant_id', 'budget_id', 'account_id', 'amount', 'period')
    search_fields = ('tenant_id', 'budget_id', 'account_id', 'period')


class XeroBudgetsRawAdmin(admin.ModelAdmin):
    list_display = ('tenant_id', 'budget_id', 'description', 'ingestion_timestamp')
    search_fields = ('tenant_id', 'budget_id', 'description')


class XeroJournalsRawAdmin(admin.ModelAdmin):
    list_display = ('tenant_id', 'journal_id', 'reference', 'journal_date', 'ingestion_timestamp')
    search_fields = ('tenant_id', 'journal_id', 'reference')


class XeroBudgetPeriodBalancesAnalyticsAdmin(admin.ModelAdmin):
    list_display = ('tenant_id', 'budget_id', 'account_id', 'period', 'amount')
    search_fields = ('tenant_id', 'budget_id', 'account_id', 'period')


class XeroBankTransactionsRawAdmin(admin.ModelAdmin):
    list_display = ('bank_transaction_id', 'tenant_id', 'type', 'status', 'date')
    search_fields = ('bank_transaction_id', 'tenant_id', 'type', 'status')


class XeroConnectionsRawAdmin(admin.ModelAdmin):
    list_display = ('tenant_id', 'user_id', 'tenant_name', 'created_at')
    search_fields = ('tenant_id', 'user_id', 'tenant_name')


class XeroContactsRawAdmin(admin.ModelAdmin):
    list_display = ('contact_id', 'tenant_id', 'name', 'updated_date_utc')
    search_fields = ('contact_id', 'tenant_id', 'name')


class XeroInvoicesRawAdmin(admin.ModelAdmin):
    list_display = ('invoice_id', 'invoice_number', 'tenant_id', 'date')
    search_fields = ('invoice_id', 'invoice_number', 'tenant_id')


admin.site.register(Integration, IntegrationAdmin)
admin.site.register(IntegrationAccessToken, IntegrationAccessTokenAdmin)
admin.site.register(ChartOfAccounts, ChartOfAccountsAdmin)
admin.site.register(SyncTableLogs, SyncTableLogsAdmin)

admin.site.register(XeroAccounts, XeroAccountsAdmin)
admin.site.register(XeroBankTransactionLineItems, XeroBankTransactionLineItemsAdmin)
admin.site.register(XeroJournalLines, XeroJournalLinesAdmin)
admin.site.register(XeroJournalLineTrackingCategories, XeroJournalLineTrackingCategoriesAdmin)
admin.site.register(XeroInvoiceLineItems, XeroInvoiceLineItemsAdmin)
admin.site.register(XeroBudgetPeriodBalances, XeroBudgetPeriodBalancesAdmin)
admin.site.register(XeroGeneralLedger, XeroGeneralLedgerAdmin)
admin.site.register(XeroBudgetPeriodBalancesAnalytics, XeroBudgetPeriodBalancesAnalyticsAdmin)
admin.site.register(XeroAccountsRaw, XeroAccountsRawAdmin)
admin.site.register(XeroBudgetPeriodBalancesRaw, XeroBudgetPeriodBalancesRawAdmin)
admin.site.register(XeroBudgetsRaw, XeroBudgetsRawAdmin)
admin.site.register(XeroJournalsRaw, XeroJournalsRawAdmin)
admin.site.register(XeroBankTransactionsRaw, XeroBankTransactionsRawAdmin)
admin.site.register(XeroConnectionsRaw, XeroConnectionsRawAdmin)
admin.site.register(XeroContactsRaw, XeroContactsRawAdmin)
admin.site.register(XeroInvoicesRaw, XeroInvoicesRawAdmin)

# Netsuite
admin.site.register(NetSuiteVendors, NetSuiteVendorsAdmin)
admin.site.register(NetSuiteAccounts, NetSuiteAccountsAdmin)
admin.site.register(NetSuiteTransactions, NetSuiteTransactionsAdmin)
admin.site.register(NetSuiteSubsidiaries, NetSuiteSubsidiariesAdmin)
admin.site.register(NetSuiteDepartments, NetSuiteDepartmentsAdmin)
admin.site.register(NetSuiteGeneralLedger, NetSuiteGeneralLedgerAdmin)
admin.site.register(NetSuiteJournals, NetSuiteJournalsAdmin)
admin.site.register(NetSuiteEntity, NetSuiteEntityAdmin)
admin.site.register(NetSuiteAccountingPeriods, NetSuiteAccountingPeriodsAdmin)
admin.site.register(NetSuiteTransactionLine, NetSuiteTransactionLinesAdmin)
admin.site.register(NetSuiteTransactionAccountingLine, NetSuiteTransactionAccountingLineAdmin)
admin.site.register(NetSuiteTransformedTransaction, NetSuiteTransformedTransactionAdmin)


from integrations.models.netsuite.temp import (
    NetSuiteTransactionAccountingLine1,
    NetSuiteTransactionLine1,
    NetSuiteTransactions1
)


class NetSuiteTransactionAccountingLine1Admin(admin.ModelAdmin):
    list_display = ('org', 'transaction', 'account', 'amount', 'lastmodifieddate', 'transaction_line')
    search_fields = ('transaction', 'account', 'amount')
    

class NetSuiteTransactionLine1Admin(admin.ModelAdmin):
    list_display = ('transaction_line_id',  'subsidiary', 'transactionid', 'closedate')
    search_fields = ('transaction_line_id', 'transactionid', 'subsidiary')


class NetSuiteTransactions1Admin(admin.ModelAdmin):
    list_display = ('company_name', 'approvalstatus', 'createdby', 'transactionid', 'createddate', 'memo')
    search_fields = ('abbrevtype', 'createdby', 'transactionid', 'memo', 'transactionnumber')



admin.site.register(NetSuiteTransactionAccountingLine1, NetSuiteTransactionAccountingLine1Admin)
admin.site.register(NetSuiteTransactionLine1, NetSuiteTransactionLine1Admin)
admin.site.register(NetSuiteTransactions1, NetSuiteTransactions1Admin)
