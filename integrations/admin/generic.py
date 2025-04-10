from django.contrib import admin
from django.urls import path
from django.shortcuts import render, redirect
from django.contrib import messages
from core.forms import DataImportForm, BudgetImportForm
from integrations.models.models import (
    Integration,
    IntegrationAccessToken,
    SyncTableLogs,
    HighPriorityTask,
    GenericIntegration,
    IntegrationCredential
)


class IntegrationAdmin(admin.ModelAdmin):
    list_display = ('org', 'created_at', 'updated_at')
    search_fields = ('org__name', 'xero_client_id', 'xero_client_secret', 'created_at', 'updated_at')


class IntegrationAccessTokenAdmin(admin.ModelAdmin):
    list_display = ('integration', 'integration_type', 'expires_at', 'created_at')
    search_fields = ('integration__org__name', 'integration_type', 'token', 'refresh_token', 'expires_at', 'created_at')


class SyncTableLogsAdmin(admin.ModelAdmin):
    list_display = ('module_name', 'integration', 'organization', 'fetched_records', 'last_updated_time', 'last_updated_date')
    search_fields = ('module_name', 'organization__name')


class IntegrationCredentialInline(admin.TabularInline):
    model = IntegrationCredential
    extra = 1
    fields = ('key', 'value', 'created_at', 'updated_at')
    readonly_fields = ('created_at', 'updated_at')


class GenericIntegrationAdmin(admin.ModelAdmin):
    list_display = ('name', 'integration_type', 'org', 'is_active', 'created_at')
    list_filter = ('integration_type', 'is_active', 'created_at')
    search_fields = ('name', 'integration_type', 'org__name')
    inlines = [IntegrationCredentialInline]
    fieldsets = (
        (None, {
            'fields': ('name', 'integration_type', 'org', 'is_active')
        }),
        ('Timestamps', {
            'fields': ('created_at', 'updated_at'),
            'classes': ('collapse',),
        }),
    )
    readonly_fields = ('created_at', 'updated_at')


class IntegrationCredentialAdmin(admin.ModelAdmin):
    list_display = ('key', 'masked_value', 'created_at', 'updated_at')
    list_filter = ('integration__is_active', 'integration__integration_type')
    search_fields = ('integration__name', 'key', 'integration__org__name')
    readonly_fields = ('created_at', 'updated_at')
    
    def masked_value(self, obj):
        """Returns a masked version of the credential value for security"""
        if not obj.value:
            return ""
        if len(obj.value) <= 8:
            return "********"
        return obj.value[:4] + "****" + obj.value[-4:]
    
    masked_value.short_description = "Value"


class ImportToolsMixin:
    """Mixin that provides import tool views for admin classes."""
    
    def get_import_urls(self):
        return [
            path('import-data/', self.admin_site.admin_view(self.import_data_view), name="import-data"),
            path('import-budgets/', self.admin_site.admin_view(self.import_budgets_view), name="import-budgets"),
        ]
    
    def import_data_view(self, request):
        if request.method == 'POST':
            form = DataImportForm(request.POST)
            if form.is_valid():
                integration = form.cleaned_data['integration']
                integration_type = form.cleaned_data['integration_type'].lower()
                since_date = form.cleaned_data['since_date']
                selected_modules = form.cleaned_data.get('modules', [])
                
                HighPriorityTask.objects.create(
                    integration=integration,
                    integration_type=integration_type,
                    since_date=since_date,
                    selected_modules=selected_modules,
                    processed=False  
                )
                
                messages.info(
                    request,
                    "High priority data import record has been created. It will be processed shortly."
                )
                return redirect("..")
        else:
            form = DataImportForm()

        context = {
            **self.admin_site.each_context(request),
            'form': form,
            'title': "Data Import",
        }
        return render(request, "admin/data_import_form.html", context)
    
    def import_budgets_view(self, request):
        if request.method == 'POST':
            form = BudgetImportForm(request.POST)
            if form.is_valid():
                integration = form.cleaned_data['integration']
                since_date = form.cleaned_data['since_date']
                until_date = form.cleaned_data['until_date']
                
                HighPriorityTask.objects.create(
                    integration=integration,
                    integration_type='xero',
                    since_date=since_date,
                    selected_modules=['budgets'],
                    processed=False,
                    until_date=until_date
                )
                
                messages.info(
                    request,
                    "High priority budget import record has been created. It will be processed shortly."
                )
                return redirect("..")
        else:
            form = BudgetImportForm()

        context = {
            **self.admin_site.each_context(request),
            'form': form,
            'title': "Import Xero Budgets",
        }
        return render(request, "admin/budget_import_form.html", context)


class HighPriorityTaskAdmin(ImportToolsMixin, admin.ModelAdmin):
    list_display = ('integration', 'integration_type', 'since_date', 'selected_modules', 'processed')
    search_fields = ('integration__org__name', 'integration_type', 'since_date', 'selected_modules', 'processed')
    change_list_template = "admin/high_priority_task_changelist.html"

    def get_urls(self):
        urls = super().get_urls()
        custom_urls = self.get_import_urls()
        return custom_urls + urls

    def has_module_permission(self, request):
        return True

    def get_model_perms(self, request):
        # Returning view permission keeps it visible on the admin index.
        return {'view': True}

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
admin.site.register(GenericIntegration, GenericIntegrationAdmin)
admin.site.register(IntegrationCredential, IntegrationCredentialAdmin)
