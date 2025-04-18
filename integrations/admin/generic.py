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
)
from django import forms
from django.utils.html import format_html


class IntegrationAdminForm(forms.ModelForm):
    # Toast specific fields
    toast_api_url = forms.URLField(required=False, label="API URL")
    toast_client_id = forms.CharField(required=False, label="Client ID")
    toast_client_secret = forms.CharField(required=False, widget=forms.PasswordInput, label="Client Secret")
    toast_webhook_secret = forms.CharField(required=False, widget=forms.PasswordInput, label="Webhook Secret")
    
    # Xero specific fields
    xero_client_id = forms.CharField(required=False, label="Client ID")
    xero_client_secret = forms.CharField(required=False, widget=forms.PasswordInput, label="Client Secret")
    
    # NetSuite specific fields
    netsuite_account_id = forms.CharField(required=False, label="Account ID")
    netsuite_client_id = forms.CharField(required=False, label="Client ID")
    netsuite_client_secret = forms.CharField(required=False, widget=forms.PasswordInput, label="Client Secret")
    netsuite_consumer_key = forms.CharField(required=False, label="Consumer Key")
    netsuite_private_key = forms.CharField(required=False, widget=forms.Textarea, label="Private Key")
    netsuite_certificate_id = forms.CharField(required=False, label="Certificate ID")
    
    # For 'other' type or additional settings
    custom_settings = forms.CharField(required=False, widget=forms.Textarea, 
                               help_text="Add settings in JSON format: {\"key1\": \"value1\", \"key2\": \"value2\"}")
    
    class Meta:
        model = Integration
        exclude = ('organisation',)  # Exclude it from automatic form generation
    
    # Add a ModelChoiceField manually
    organisation = forms.ModelChoiceField(
        queryset=None,  # We'll set this in __init__
        required=True,
        label="Organisation"
    )
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Import Organisation here to avoid circular imports
        from core.models import Organisation
        self.fields['organisation'].queryset = Organisation.objects.all()
        
        # Set initial value if editing existing object
        if self.instance and self.instance.pk:
            self.fields['organisation'].initial = self.instance.organisation
        
        # If we're editing an existing object
        if self.instance and self.instance.pk and self.instance.settings:
            # Populate type-specific fields
            if self.instance.integration_type == 'toast':
                self.fields['toast_api_url'].initial = self.instance.get_setting('api_url')
                self.fields['toast_client_id'].initial = self.instance.get_setting('client_id')
                self.fields['toast_client_secret'].initial = self.instance.get_setting('client_secret')
                self.fields['toast_webhook_secret'].initial = self.instance.get_setting('webhook_secret')
            elif self.instance.integration_type == 'xero':
                self.fields['xero_client_id'].initial = self.instance.get_setting('client_id')
                self.fields['xero_client_secret'].initial = self.instance.get_setting('client_secret')
            elif self.instance.integration_type == 'netsuite':
                self.fields['netsuite_account_id'].initial = self.instance.get_setting('account_id')
                self.fields['netsuite_client_id'].initial = self.instance.get_setting('client_id')
                self.fields['netsuite_client_secret'].initial = self.instance.get_setting('client_secret')
                self.fields['netsuite_consumer_key'].initial = self.instance.get_setting('consumer_key')
                self.fields['netsuite_private_key'].initial = self.instance.get_setting('private_key')
                self.fields['netsuite_certificate_id'].initial = self.instance.get_setting('certificate_id')
            
            # For 'other' type or any additional settings not covered above
            if self.instance.integration_type == 'other':
                import json
                self.fields['custom_settings'].initial = json.dumps(self.instance.settings, indent=2)
    
    def save(self, commit=True):
        instance = super().save(commit=False)
        
        # Set the organisation
        if self.cleaned_data.get('organisation'):
            instance.organisation = self.cleaned_data['organisation']
        
        # Always start with empty settings to ensure no leakage between types
        instance.settings = {}
        
        # Only save settings for the selected integration type
        integration_type = instance.integration_type.lower()
        
        if integration_type == 'toast':
            # Save Toast fields
            if self.cleaned_data.get('toast_api_url'):
                instance.settings['api_url'] = self.cleaned_data.get('toast_api_url')
            if self.cleaned_data.get('toast_client_id'):
                instance.settings['client_id'] = self.cleaned_data.get('toast_client_id')
            if self.cleaned_data.get('toast_client_secret'):
                instance.settings['client_secret'] = self.cleaned_data.get('toast_client_secret')
            if self.cleaned_data.get('toast_webhook_secret'):
                instance.settings['webhook_secret'] = self.cleaned_data.get('toast_webhook_secret')
        elif integration_type == 'xero':
            # Save Xero fields
            if self.cleaned_data.get('xero_client_id'):
                instance.settings['client_id'] = self.cleaned_data.get('xero_client_id')
            if self.cleaned_data.get('xero_client_secret'):
                instance.settings['client_secret'] = self.cleaned_data.get('xero_client_secret')
        elif integration_type == 'netsuite':
            # Save NetSuite fields
            if self.cleaned_data.get('netsuite_account_id'):
                instance.settings['account_id'] = self.cleaned_data.get('netsuite_account_id')
            if self.cleaned_data.get('netsuite_client_id'):
                instance.settings['client_id'] = self.cleaned_data.get('netsuite_client_id')
            if self.cleaned_data.get('netsuite_client_secret'):
                instance.settings['client_secret'] = self.cleaned_data.get('netsuite_client_secret')
            if self.cleaned_data.get('netsuite_consumer_key'):
                instance.settings['consumer_key'] = self.cleaned_data.get('netsuite_consumer_key')
            if self.cleaned_data.get('netsuite_private_key'):
                instance.settings['private_key'] = self.cleaned_data.get('netsuite_private_key')
            if self.cleaned_data.get('netsuite_certificate_id'):
                instance.settings['certificate_id'] = self.cleaned_data.get('netsuite_certificate_id')
        elif integration_type == 'other':
            # For 'other' type, only use custom_settings
            try:
                import json
                if self.cleaned_data.get('custom_settings'):
                    instance.settings = json.loads(self.cleaned_data.get('custom_settings'))
            except json.JSONDecodeError:
                # Fallback for invalid JSON
                instance.settings = {'error': 'Invalid JSON format'}
            # Return early to avoid mixing with other settings
            if commit:
                instance.save()
            return instance
        
        # For all integration types besides 'other', allow custom_settings to be merged
        if self.cleaned_data.get('custom_settings'):
            try:
                import json
                custom_settings = json.loads(self.cleaned_data.get('custom_settings'))
                # Merge custom settings with existing settings
                instance.settings.update(custom_settings)
            except json.JSONDecodeError:
                # Skip invalid JSON
                pass
        
        if commit:
            instance.save()
        return instance

    def clean(self):
        cleaned_data = super().clean()
        integration_type = cleaned_data.get('integration_type', '').lower()
        
        # Check for credentials in fields not matching the selected type
        if integration_type == 'toast':
            # Required fields for Toast
            if not cleaned_data.get('toast_client_id'):
                self.add_error('toast_client_id', 'Client ID is required for Toast integration')
            if not cleaned_data.get('toast_client_secret'):
                self.add_error('toast_client_secret', 'Client Secret is required for Toast integration')
            
            # Reject credentials for other types
            if any([
                cleaned_data.get('xero_client_id'), 
                cleaned_data.get('xero_client_secret'),
                cleaned_data.get('netsuite_account_id'),
                cleaned_data.get('netsuite_client_id'),
                cleaned_data.get('netsuite_client_secret'),
                cleaned_data.get('netsuite_consumer_key'),
                cleaned_data.get('netsuite_private_key'),
                cleaned_data.get('netsuite_certificate_id'),
                cleaned_data.get('custom_settings')
            ]):
                self.add_error(None, 'Only Toast credentials should be provided for Toast integration type')
            
        elif integration_type == 'xero':
            # Required fields for Xero
            if not cleaned_data.get('xero_client_id'):
                self.add_error('xero_client_id', 'Client ID is required for Xero integration')
            if not cleaned_data.get('xero_client_secret'):
                self.add_error('xero_client_secret', 'Client Secret is required for Xero integration')
            
            # Reject credentials for other types
            if any([
                cleaned_data.get('toast_api_url'),
                cleaned_data.get('toast_client_id'),
                cleaned_data.get('toast_client_secret'),
                cleaned_data.get('toast_webhook_secret'),
                cleaned_data.get('netsuite_account_id'),
                cleaned_data.get('netsuite_client_id'),
                cleaned_data.get('netsuite_client_secret'),
                cleaned_data.get('netsuite_consumer_key'),
                cleaned_data.get('netsuite_private_key'),
                cleaned_data.get('netsuite_certificate_id'),
                cleaned_data.get('custom_settings')
            ]):
                self.add_error(None, 'Only Xero credentials should be provided for Xero integration type')
            
        elif integration_type == 'netsuite':
            # Required fields for NetSuite
            if not cleaned_data.get('netsuite_account_id'):
                self.add_error('netsuite_account_id', 'Account ID is required for NetSuite integration')
            
            # Reject credentials for other types
            if any([
                cleaned_data.get('toast_api_url'),
                cleaned_data.get('toast_client_id'),
                cleaned_data.get('toast_client_secret'),
                cleaned_data.get('toast_webhook_secret'),
                cleaned_data.get('xero_client_id'),
                cleaned_data.get('xero_client_secret'),
                cleaned_data.get('custom_settings')
            ]):
                self.add_error(None, 'Only NetSuite credentials should be provided for NetSuite integration type')
            
        elif integration_type == 'other':
            # For 'other' type, only custom_settings should be used
            if not cleaned_data.get('custom_settings'):
                self.add_error('custom_settings', 'Custom settings are required for Other integration type')
            else:
                try:
                    import json
                    json.loads(cleaned_data.get('custom_settings'))
                except json.JSONDecodeError:
                    self.add_error('custom_settings', 'Invalid JSON format')
                
            # Reject credentials for specific types
            if any([
                cleaned_data.get('toast_api_url'),
                cleaned_data.get('toast_client_id'),
                cleaned_data.get('toast_client_secret'),
                cleaned_data.get('toast_webhook_secret'),
                cleaned_data.get('xero_client_id'),
                cleaned_data.get('xero_client_secret'),
                cleaned_data.get('netsuite_account_id'),
                cleaned_data.get('netsuite_client_id'),
                cleaned_data.get('netsuite_client_secret'),
                cleaned_data.get('netsuite_consumer_key'),
                cleaned_data.get('netsuite_private_key'),
                cleaned_data.get('netsuite_certificate_id')
            ]):
                self.add_error(None, 'Only custom settings should be provided for Other integration type')
        
        return cleaned_data


class IntegrationAdmin(admin.ModelAdmin):
    form = IntegrationAdminForm
    list_display = ('name', 'integration_type', 'organisation', 'is_active', 'created_at')
    list_filter = ('integration_type', 'is_active', 'created_at')
    search_fields = ('name', 'organisation__name')
    readonly_fields = ('created_at', 'updated_at', 'settings_display')
    
    fieldsets = [
        (None, {
            'fields': ('organisation', 'integration_type', 'name', 'is_active')
        }),
        ('Toast Settings', {
            'classes': ('toast-fieldset',),
            'fields': ('toast_api_url', 'toast_client_id', 'toast_client_secret', 'toast_webhook_secret'),
        }),
        ('Xero Settings', {
            'classes': ('xero-fieldset',),
            'fields': ('xero_client_id', 'xero_client_secret'),
        }),
        ('NetSuite Settings', {
            'classes': ('netsuite-fieldset',),
            'fields': ('netsuite_account_id', 'netsuite_client_id', 'netsuite_client_secret', 
                     'netsuite_consumer_key', 'netsuite_private_key', 'netsuite_certificate_id'),
        }),
        ('Custom Settings', {
            'classes': ('custom-fieldset',),
            'fields': ('custom_settings',),
        }),
        ('Information', {
            'fields': ('created_at', 'updated_at', 'settings_display'),
            'classes': ('collapse',),
        }),
    ]
    
    def settings_display(self, obj):
        """Display settings in a readable format"""
        if not obj.settings:
            return "No settings saved"
            
        html = ['<div style="max-height: 300px; overflow: auto;">']
        html.append('<table style="width: 100%; border-collapse: collapse;">')
        html.append('<tr><th style="border: 1px solid #ddd; padding: 8px; text-align: left;">Key</th><th style="border: 1px solid #ddd; padding: 8px; text-align: left;">Value</th></tr>')
        
        for key, value in obj.settings.items():
            # Mask sensitive values
            if any(sensitive in key.lower() for sensitive in ['secret', 'password', 'key', 'token']):
                if value and len(str(value)) > 8:
                    display_value = str(value)[:4] + "****" + str(value)[-4:]
                else:
                    display_value = "********"
            else:
                display_value = str(value)
                
            html.append(f'<tr><td style="border: 1px solid #ddd; padding: 8px;">{key}</td><td style="border: 1px solid #ddd; padding: 8px;">{display_value}</td></tr>')
            
        html.append('</table>')
        html.append('</div>')
        return format_html(''.join(html))
    settings_display.short_description = "Settings Overview"
    
    class Media:
        js = ('admin/js/vendor/jquery/jquery.min.js',)
        css = {
            'all': ('css/admin_integration.css',)
        }


class IntegrationAccessTokenAdmin(admin.ModelAdmin):
    list_display = ('integration', 'integration_type', 'expires_at', 'created_at')
    search_fields = ('integration__organisation__name', 'integration_type', 'token', 'refresh_token', 'expires_at', 'created_at')


class SyncTableLogsAdmin(admin.ModelAdmin):
    list_display = ('module_name', 'integration', 'organisation', 'fetched_records', 'last_updated_time', 'last_updated_date')
    search_fields = ('module_name', 'organisation__name')


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
    search_fields = ('integration__organisation__name', 'integration_type', 'since_date', 'selected_modules', 'processed')
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
