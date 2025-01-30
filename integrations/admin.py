from django.contrib import admin
from .models.models import (
    Integration,
    IntegrationAccessToken,
    ChartOfAccounts,
    OrphanBankTransaction
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
    SquareOrders,
    XeroBankTransactionsRaw,
    XeroConnectionsRaw,
    XeroContactsRaw,
    XeroInvoicesRaw,
)




class IntegrationAdmin(admin.ModelAdmin):
    list_display = ('org', 'created_at', 'updated_at')
    search_fields = ('org__name', 'xero_client_id', 'xero_client_secret', 'xero_tenant_id', 'created_at', 'updated_at')


class IntegrationAccessTokenAdmin(admin.ModelAdmin):
    list_display = ('integration', 'integration_type', 'expires_at', 'created_at')
    search_fields = ('integration__org__name', 'integration_type', 'expires_at', 'created_at')


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
    list_display = ('journal_line_id', 'journal_id', 'description', 'account_id', 'net_amount', 'tax_amount')
    search_fields = ('journal_line_id', 'journal_id', 'description', 'account_id')


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
    list_display = ( 'tenant_id', 'account_id')
    search_fields = ( 'tenant_id', 'account_id')


class XeroAccountsRawAdmin(admin.ModelAdmin):
    list_display = ('tenant_id', 'account_id', 'name', 'status', 'ingestion_timestamp')
    search_fields = ('tenant_id', 'account_id', 'name', 'status')


class XeroBudgetPeriodBalancesRawAdmin(admin.ModelAdmin):
    list_display = ('tenant_id', 'amount', 'ingestion_timestamp')
    search_fields = ('tenant_id',)


class XeroBudgetsRawAdmin(admin.ModelAdmin):
    list_display = ('tenant_id', 'budget_id', 'description', 'ingestion_timestamp')
    search_fields = ('tenant_id', 'budget_id', 'description')


class XeroJournalsRawAdmin(admin.ModelAdmin):
    list_display = ('tenant_id', 'journal_id', 'reference', 'journal_date', 'ingestion_timestamp')
    search_fields = ('tenant_id', 'journal_id', 'reference')
    
    
class XeroBudgetPeriodBalancesAnalyticsAdmin(admin.ModelAdmin):
    list_display = ('tenant_id', 'budget_id', 'account_id', 'period', 'amount')
    search_fields = ('tenant_id', 'budget_id', 'account_id', 'period')


class SquareOrdersAdmin(admin.ModelAdmin):
    list_display = ('order_id', 'customer_id', 'location_id', 'created_at')
    search_fields = ('order_id', 'customer_id', 'location_id')


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
    search_fields = ('invoice_id', 'invoice_number', 'tenant_id', 'status')


admin.site.register(Integration, IntegrationAdmin)
admin.site.register(IntegrationAccessToken, IntegrationAccessTokenAdmin)
admin.site.register(ChartOfAccounts, ChartOfAccountsAdmin)
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
admin.site.register(SquareOrders, SquareOrdersAdmin)
admin.site.register(XeroBankTransactionsRaw, XeroBankTransactionsRawAdmin)
admin.site.register(XeroConnectionsRaw, XeroConnectionsRawAdmin)
admin.site.register(XeroContactsRaw, XeroContactsRawAdmin)
admin.site.register(XeroInvoicesRaw, XeroInvoicesRawAdmin)
