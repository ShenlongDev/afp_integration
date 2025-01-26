from django.contrib import admin
from integrations.models import Integration, IntegrationAccessToken


class IntegrationAdmin(admin.ModelAdmin):
    list_display = ('org','created_at', 'updated_at')
    search_fields = ('org', 'xero_client_id', 'xero_client_secret', 'xero_tenant_id', 'created_at', 
                     'updated_at')
    
    
class IntegrationAccessTokenAdmin(admin.ModelAdmin):
    list_display = ('integration', 'integration_type', 'expires_at', 'created_at')
    search_fields = ('integration', 'integration_type', 'expires_at', 'created_at')
    
    
admin.site.register(Integration, IntegrationAdmin)
admin.site.register(IntegrationAccessToken, IntegrationAccessTokenAdmin)