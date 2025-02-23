from django.contrib import admin
from integrations.models.xero.transformations import (
    XeroJournalLines,
    XeroJournalLineTrackingCategories,
    XeroInvoiceLineItems,
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
    XeroContactsRaw,
    XeroInvoicesRaw,
)


class XeroJournalLinesAdmin(admin.ModelAdmin):
    list_display = ('journal_line_id', 'description', 'account_id', 'account_name', 'net_amount', 'journal_date', 'created_date_utc')
    search_fields = ('journal_line_id', 'description', 'account_id', 'journal_date', 'created_date_utc', 'account_name')

class XeroJournalLineTrackingCategoriesAdmin(admin.ModelAdmin):
    list_display = ('tracking_category_id', 'journal_line_id', 'name', 'option')
    search_fields = ('tracking_category_id', 'journal_line_id', 'name', 'option')

class XeroInvoiceLineItemsAdmin(admin.ModelAdmin):
    list_display = ('invoice_id', 'description', 'invoice_number', 'url')
    search_fields = ('invoice_id', 'description', 'invoice_number', 'url')

class XeroGeneralLedgerAdmin(admin.ModelAdmin):
    list_display = ('tenant_id', 'account_id', 'account_reporting_code', 'account_name', 'journal_date', 'created_date', 'net_amount', 'tracking_category_name','invoice_url')
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


class XeroContactsRawAdmin(admin.ModelAdmin):
    list_display = ('contact_id', 'tenant_id', 'name', 'updated_date_utc')
    search_fields = ('contact_id', 'tenant_id', 'name')

class XeroInvoicesRawAdmin(admin.ModelAdmin):
    list_display = ('invoice_id', 'invoice_number', 'tenant_id', 'date')
    search_fields = ('invoice_id', 'invoice_number', 'tenant_id')

admin.site.register(XeroJournalLines, XeroJournalLinesAdmin)
admin.site.register(XeroJournalLineTrackingCategories, XeroJournalLineTrackingCategoriesAdmin)
admin.site.register(XeroInvoiceLineItems, XeroInvoiceLineItemsAdmin)
admin.site.register(XeroGeneralLedger, XeroGeneralLedgerAdmin)
admin.site.register(XeroBudgetPeriodBalancesAnalytics, XeroBudgetPeriodBalancesAnalyticsAdmin)
admin.site.register(XeroAccountsRaw, XeroAccountsRawAdmin)
admin.site.register(XeroBudgetPeriodBalancesRaw, XeroBudgetPeriodBalancesRawAdmin)
admin.site.register(XeroBudgetsRaw, XeroBudgetsRawAdmin)
admin.site.register(XeroJournalsRaw, XeroJournalsRawAdmin)
admin.site.register(XeroBankTransactionsRaw, XeroBankTransactionsRawAdmin)
admin.site.register(XeroContactsRaw, XeroContactsRawAdmin)
admin.site.register(XeroInvoicesRaw, XeroInvoicesRawAdmin) 