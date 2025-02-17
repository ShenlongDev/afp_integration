from django.contrib import admin
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
    list_display = ('invoice_id', 'description')
    search_fields = ('invoice_id', 'description')

class XeroBudgetPeriodBalancesAdmin(admin.ModelAdmin):
    list_display = ('tenant_id', 'amount', 'period')
    search_fields = ('tenant_id', 'period')

class XeroGeneralLedgerAdmin(admin.ModelAdmin):
    list_display = ('tenant_id', 'account_id', 'account_name', 'journal_date', 'created_date', 'net_amount', 'invoice_url')
    search_fields = ('tenant_id', 'account_id', 'account_name', 'journal_date', 'created_date')

class XeroBudgetPeriodBalancesAnalyticsAdmin(admin.ModelAdmin):
    list_display = ('tenant_id', 'budget_id', 'account_id', 'period', 'amount')
    search_fields = ('tenant_id', 'budget_id', 'account_id', 'period')

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