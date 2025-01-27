from django.contrib import admin
from integrations.models import (
    Integration, IntegrationAccessToken, ChartOfAccounts
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
    

admin.site.register(Integration, IntegrationAdmin)
admin.site.register(IntegrationAccessToken, IntegrationAccessTokenAdmin)
admin.site.register(ChartOfAccounts, ChartOfAccountsAdmin)