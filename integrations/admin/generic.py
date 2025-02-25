from django.contrib import admin
from integrations.models.models import (
    Integration,
    IntegrationAccessToken,
    SyncTableLogs,
    HighPriorityTask
)


class IntegrationAdmin(admin.ModelAdmin):
    list_display = ('org', 'created_at', 'updated_at')
    search_fields = ('org__name', 'xero_client_id', 'xero_client_secret', 'xero_tenant_id', 'created_at', 'updated_at')


class IntegrationAccessTokenAdmin(admin.ModelAdmin):
    list_display = ('integration', 'integration_type', 'expires_at', 'created_at')
    search_fields = ('integration__org__name', 'integration_type', 'token', 'refresh_token', 'expires_at', 'created_at')
    
    def has_change_permission(self, request, obj=None):
        return False
    
    def has_add_permission(self, request):
        return False


class SyncTableLogsAdmin(admin.ModelAdmin):
    list_display = ('module_name', 'integration', 'organization', 'fetched_records', 'last_updated_time', 'last_updated_date')
    search_fields = ('module_name', 'integration__org__name', 'organization__name', 'organization')


class HighPriorityTaskAdmin(admin.ModelAdmin):
    list_display = ('integration', 'integration_type', 'since_date', 'selected_modules', 'processed')
    search_fields = ('integration__org__name', 'integration_type', 'since_date', 'selected_modules', 'processed')

    def has_add_permission(self, request):
        return False
    
    def has_delete_permission(self, request, obj=None):
        return False
    
    def has_change_permission(self, request, obj=None):
        return False

admin.site.register(Integration, IntegrationAdmin)
admin.site.register(IntegrationAccessToken, IntegrationAccessTokenAdmin)
admin.site.register(SyncTableLogs, SyncTableLogsAdmin) 
admin.site.register(HighPriorityTask, HighPriorityTaskAdmin)