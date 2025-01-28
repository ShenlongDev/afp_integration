from django.contrib import admin
from integrations.models.models import (
    Integration, IntegrationAccessToken, ChartOfAccounts, OrphanBankTransaction
)


class IntegrationAdmin(admin.ModelAdmin):
    list_display = ('org','created_at', 'updated_at')
    search_fields = ('org', 'xero_client_id', 'xero_client_secret', 'xero_tenant_id', 'created_at', 
                     'updated_at')
    
    
class IntegrationAccessTokenAdmin(admin.ModelAdmin):
    list_display = ('integration', 'integration_type', 'expires_at', 'created_at')
    search_fields = ('integration', 'integration_type', 'expires_at', 'created_at')
    
    
class ChartOfAccountsAdmin(admin.ModelAdmin):
    list_display = ('integration', 'account_id', 'code', 'name', 'status', 'account_type', 'tax_type')
    search_fields = ('integration', 'account_id', 'code', 'name', 'status', 'account_type', 'tax_type')
    
    
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

admin.site.register(Integration, IntegrationAdmin)
admin.site.register(IntegrationAccessToken, IntegrationAccessTokenAdmin)
admin.site.register(ChartOfAccounts, ChartOfAccountsAdmin)