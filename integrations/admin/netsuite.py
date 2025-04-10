from django.contrib import admin
from integrations.models.models import Integration
from integrations.models.netsuite.analytics import (
    NetSuiteVendors,
    NetSuiteAccounts,
    # NetSuiteTransactions,
    NetSuiteSubsidiaries,
    NetSuiteDepartments,
    NetSuiteJournals,
    NetSuiteEntity,
    NetSuiteAccountingPeriods,
    NetSuiteTransformedTransaction,
    NetSuiteBudgets,
    NetSuiteLocations,
    # NetSuiteTransactionLine,
    # NetSuiteTransactionAccountingLine
)

class NetSuiteLocationsAdmin(admin.ModelAdmin):
    list_display = ('tenant_id', 'location_id', 'name', 'full_name', 'external_id', 'include_children', 'is_inactive', 'main_address', 'subsidiary', 'parent', 'last_modified_date', 'record_date')
    search_fields = ('tenant_id', 'location_id', 'name', 'full_name', 'external_id', 'include_children', 'is_inactive', 'main_address', 'subsidiary', 'parent', 'last_modified_date', 'record_date')

class NetSuiteBudgetsAdmin(admin.ModelAdmin):
    list_display = ('tenant_id', 'budget_id', 'account_id', 'amount', 'fiscal_year', 'period', 'last_modified_date', 'record_date')
    search_fields = ('tenant_id', 'budget_id', 'account_id', 'amount', 'fiscal_year', 'period', 'last_modified_date', 'record_date')

class NetSuiteVendorsAdmin(admin.ModelAdmin):
    list_display = (
        'vendor_id', 'tenant_id', 'entity_id', 'is_person', 'is_inactive',
        'email', 'phone', 'currency', 'subsidiary', 'terms', 'record_date'
    )
    search_fields = (
        'vendor_id', 'entity_id', 'is_person', 'is_inactive', 'email',
        'phone', 'currency', 'subsidiary', 'terms', 'record_date'
    )

class NetSuiteAccountsAdmin(admin.ModelAdmin):
    list_display = ( 'is_inactive', 'accountsearchdisplayname', 'accountsearchdisplaynamecopy')
    search_fields = ('is_inactive', 'accountsearchdisplayname', 'accountsearchdisplaynamecopy')

# class NetSuiteTransactionsAdmin(admin.ModelAdmin):
#     list_display = ('tenant_id', 'approvalstatus', 'createdby', 'transactionid', 'createddate', 'memo')
#     search_fields = ('abbrevtype', 'createdby', 'transactionid', 'memo', 'transactionnumber')

class NetSuiteSubsidiariesAdmin(admin.ModelAdmin):
    list_display = ('subsidiary_id', 'name', 'name_nohi', 'full_name', 'legal_name', 'federal_number', 'is_elimination', 'currency', 'country', 'record_date')
    search_fields = ('subsidiary_id', 'name', 'name_nohi', 'full_name', 'legal_name', 'federal_number', 'is_elimination', 'currency', 'country', 'record_date')

class NetSuiteDepartmentsAdmin(admin.ModelAdmin):
    list_display = ('department_id', 'tenant_id', 'name', 'full_name', 'is_inactive', 'record_date')
    search_fields = ('department_id', 'name', 'full_name', 'is_inactive', 'record_date')

class NetSuiteGeneralLedgerAdmin(admin.ModelAdmin):
    list_display = ('tenant_id', 'record_date', 'trandate', 'subsidiary')
    search_fields = ('tenant_id', 'is_inactive', 'record_date')

class NetSuiteJournalsAdmin(admin.ModelAdmin):
    list_display = ('journal_id', 'tenant_id', 'record_date')
    search_fields = ('journal_id', 'record_date')

class NetSuiteEntityAdmin(admin.ModelAdmin):
    list_display = ('id','tenant_id', 'entity_id', 'legal_name', 'is_person', 'is_inactive', 'parent_entity', 'email', 'phone', 'currency', 'terms', 'last_modified_date', 'record_date')
    search_fields = ('id', 'entity_id', 'email', 'phone', 'currency', 'terms')

class NetSuiteAccountingPeriodsAdmin(admin.ModelAdmin):
    list_display = ('tenant_id', 'period_name', 'start_date', 'end_date', 'closed', 'all_locked', 'fiscal_calendar', 'record_date')
    search_fields = ('period_name', 'start_date', 'end_date', 'closed', 'all_locked', 'fiscal_calendar', 'record_date')

class NetSuiteTransformedTransactionAdmin(admin.ModelAdmin):
    list_display = ('consolidation_key', 'transactionid', 'createdby', 'createddate', 'lastmodifieddate', 'entity_id')
    search_fields = ('consolidation_key', 'transactionid', 'createdby', 'createddate', 'lastmodifieddate', 'entity_id')

# class NetSuiteTransactionAccountingLineAdmin(admin.ModelAdmin):
#     list_display = ('tenant_id', 'transaction', 'account', 'amount', 'lastmodifieddate', 'transaction_line')
#     search_fields = ('transaction', 'account', 'amount')

# class NetSuiteTransactionLineAdmin(admin.ModelAdmin):
#     list_display = ('transaction_line_id', 'subsidiary', 'transactionid', 'closedate')
#     search_fields = ('transaction_line_id', 'transactionid', 'subsidiary')


# Register Netsuite related models
admin.site.register(NetSuiteVendors, NetSuiteVendorsAdmin)
admin.site.register(NetSuiteAccounts, NetSuiteAccountsAdmin)
admin.site.register(NetSuiteSubsidiaries, NetSuiteSubsidiariesAdmin)
admin.site.register(NetSuiteDepartments, NetSuiteDepartmentsAdmin)
admin.site.register(NetSuiteJournals, NetSuiteJournalsAdmin)
admin.site.register(NetSuiteEntity, NetSuiteEntityAdmin)
admin.site.register(NetSuiteAccountingPeriods, NetSuiteAccountingPeriodsAdmin)
admin.site.register(NetSuiteTransformedTransaction, NetSuiteTransformedTransactionAdmin)
admin.site.register(NetSuiteBudgets, NetSuiteBudgetsAdmin)
admin.site.register(NetSuiteLocations, NetSuiteLocationsAdmin)
# admin.site.register(NetSuiteTransactions, NetSuiteTransactionsAdmin)
# admin.site.register(NetSuiteTransactionLine, NetSuiteTranintegrations.NetSuiteAccountssactionLineAdmin)
# admin.site.register(NetSuiteTransactionAccountingLine, NetSuiteTransactionAccountingLineAdmin)
