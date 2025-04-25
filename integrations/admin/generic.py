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
from integrations.models.models import Organisation
from django import forms
from django.utils.html import format_html
from collections import OrderedDict
import json

CHAR  = forms.CharField
URL   = forms.URLField
TEXTA = {'type': forms.CharField, 'widget': forms.Textarea}

CREDENTIALS = {
    'toast': {
        'api_url':        {'type': URL,  'label': 'API URL',        'req': True},
        'client_id':      {'type': CHAR, 'label': 'Client ID',      'req': True},
        'client_secret':  {'type': CHAR, 'label': 'Client Secret',  'req': True},
        'webhook_secret': {'type': CHAR, 'label': 'Webhook Secret', 'req': True},
    },
    'xero': {
        'client_id':     {'type': CHAR, 'label': 'Client ID',     'req': True},
        'client_secret': {'type': CHAR, 'label': 'Client Secret', 'req': True},
    },
    'netsuite': {
        'account_id':    {'type': CHAR,  'label': 'Account ID',    'req': True},
        'consumer_key':  {'type': CHAR,  'label': 'Consumer Key',  'req': True},
        'private_key':   {'type': TEXTA['type'], 'label': 'Private Key',
                         'req': True, 'widget': TEXTA['widget']},
        'certificate_id':{'type': CHAR,  'label': 'Certificate ID','req': True},
    },
    'other': {},
}


class _BaseIntegrationForm(forms.ModelForm):
    custom_settings = forms.CharField(
        required=False, widget=forms.Textarea,
        help_text='Enter extra settings as JSON. You can use key-value pairs like "key": "value" or a complete JSON object {"key": "value"}.'
    )

    class Meta:
        model = Integration
        fields = ['organisation', 'integration_type', 'is_active']
        exclude = ('settings', 'name')

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        self.fields['integration_type'].widget.attrs.update({
            "onchange": "location.search='integration_type='+this.value.toLowerCase();"
        })
        
        if 'organisation' in self.fields:
            self.fields['organisation'].queryset = Organisation.objects.all()
            
        current_type = None
        if self.instance.pk:
            current_type = self.instance.integration_type
        elif 'initial' in kwargs and 'integration_type' in kwargs['initial']:
            current_type = kwargs['initial']['integration_type']
        
        if current_type == 'other':
            self.fields['name'] = forms.CharField(required=False, label='Name')
            
        if self.instance.pk and self.instance.settings:
            if hasattr(self.instance, 'organisation') and self.instance.organisation:
                self.initial['organisation'] = self.instance.organisation.pk
                
            kind = self.instance.integration_type
            
            if kind == 'other' and hasattr(self.instance, 'name') and self.instance.name:
                self.initial['name'] = self.instance.name
                
            for key in CREDENTIALS.get(kind, {}):
                if key in self.instance.settings:
                    field_name = f'{kind}_{key}'
                    if field_name in self.fields:
                        self.fields[field_name].initial = self.instance.settings[key]
            
            extra_settings = {}
            standard_keys = list(CREDENTIALS.get(kind, {}).keys())
            
            for key, value in self.instance.settings.items():
                if key not in standard_keys:
                    extra_settings[key] = value
            
            if extra_settings:
                self.fields['custom_settings'].initial = json.dumps(
                    extra_settings, indent=2
                )

    def clean_custom_settings(self):
        """Validate and format custom_settings JSON"""
        raw_data = self.cleaned_data.get('custom_settings')
        if not raw_data:
            return '{}'
            
        raw_data = raw_data.strip()
        if not (raw_data.startswith('{') and raw_data.endswith('}')):
            raw_data = '{' + raw_data + '}'
        
        try:
            json_data = json.loads(raw_data)
            return json.dumps(json_data, indent=2)
        except json.JSONDecodeError as e:
            raise forms.ValidationError(
                f"Invalid JSON format: {str(e)}. Please check your syntax - make sure keys and values are quoted properly."
            )

    def clean(self):
        cd = super().clean()
        kind = cd.get('integration_type')
        
        if kind == 'other':
            if not cd.get('custom_settings') or cd.get('custom_settings') == '{}':
                self.add_error('custom_settings', 'Custom settings are required for Other integration type.')
        else:
            for key, spec in CREDENTIALS.get(kind, {}).items():
                if spec.get('req') and not cd.get(f'{kind}_{key}'):
                    self.add_error(f'{kind}_{key}', f'{spec["label"]} is required.')
                    
        return cd

    def save(self, commit=True):
        obj = super().save(commit=False)
        kind = obj.integration_type
        
        if kind == 'other' and 'name' in self.cleaned_data:
            obj.name = self.cleaned_data['name']
        
        obj.settings = {}
        
        if kind == 'other':
            try:
                obj.settings = json.loads(self.cleaned_data.get('custom_settings') or '{}')
            except json.JSONDecodeError:
                obj.settings = {}
        else:
            obj.name = obj.name or kind.capitalize()
            
            for key in CREDENTIALS.get(kind, {}):
                field_name = f'{kind}_{key}'
                if field_name in self.cleaned_data:
                    val = self.cleaned_data[field_name]
                    if val not in (None, ''):
                        obj.settings[key] = val
                
            if self.instance.pk and self.instance.settings:
                for key in CREDENTIALS.get(kind, {}):
                    field_name = f'{kind}_{key}'
                    is_secret = any(x in key.lower() for x in ('secret', 'password', 'key', 'token'))
                    
                    if is_secret and field_name in self.cleaned_data and not self.cleaned_data[field_name]:
                        if key in self.instance.settings:
                            obj.settings[key] = self.instance.settings[key]
                    
            try:
                extra_settings = json.loads(self.cleaned_data.get('custom_settings') or '{}')
                obj.settings.update(extra_settings)
            except json.JSONDecodeError:
                pass
                
        if commit:
            obj.save()
        return obj


@admin.register(Integration)
class IntegrationAdmin(admin.ModelAdmin):
    form = _BaseIntegrationForm
    add_form_template    = "admin/integration_form.html"
    change_form_template = "admin/integration_form.html"

    list_display    = ('name','integration_type','organisation','is_active','created_at')
    list_filter     = ('integration_type','is_active','created_at')
    search_fields   = ('name','organisation__name')
    readonly_fields = ('created_at','updated_at','settings_view')

    def get_form(self, request, obj=None, **kwargs):
        current_type = (
            request.GET.get('integration_type')
            or (obj.integration_type if obj else 'toast')
        )

        dyn_fields = OrderedDict()
        
        if current_type == 'other':
            dyn_fields['name'] = forms.CharField(required=False, label='Name')
        
        for key, spec in CREDENTIALS.get(current_type, {}).items():
            dyn_fields[f'{current_type}_{key}'] = spec['type'](
                label = spec['label'],
                required = False,
                widget = spec.get('widget')
            )

        dyn_fields['custom_settings'] = _BaseIntegrationForm.base_fields['custom_settings']

        return type(
            'DynamicIntegrationForm',
            (_BaseIntegrationForm,),
            dyn_fields,
        )

    def get_fieldsets(self, request, obj=None):
        current_type = (
            request.GET.get('integration_type')
            or (obj.integration_type if obj else 'toast')
        )

        if current_type == 'other':
            main = ('organisation', 'integration_type', 'name', 'is_active')
        else:
            main = ('organisation', 'integration_type', 'is_active')

        creds = [f'{current_type}_{k}' for k in CREDENTIALS.get(current_type, {})]

        fs = [
            (None, {'fields': main}),
        ]

        if creds:
            fs.append((f'{current_type.capitalize()} Settings', {'fields': creds}))
        if current_type == 'other':
            fs.append(('Custom Settings', {'fields': ('custom_settings',)}))
        else:
            fs.append(('Extra JSON (optional)', {'fields': ('custom_settings',)}))

        fs.append(('Info', {
            'classes': ('collapse',),
            'fields': ('created_at','updated_at','settings_view')
        }))
        return fs

    def add_view(self, request, form_url='', extra_context=None):
        ctx = extra_context or {}
        ctx['types'] = list(CREDENTIALS.keys())
        return super().add_view(request, form_url, ctx)

    def change_view(self, request, object_id, form_url='', extra_context=None):
        ctx = extra_context or {}
        ctx['types'] = list(CREDENTIALS.keys())
        return super().change_view(request, object_id, form_url, ctx)

    def settings_view(self, obj):
        if not obj.settings:
            return '—'
        rows = []
        for k, v in obj.settings.items():
            if any(w in k.lower() for w in ('secret','password','key','token')):
                v = str(v)[:4] + '****' + str(v)[-4:]
            rows.append(f'<tr><td>{k}</td><td>{v}</td></tr>')
        return format_html('<table style="width:100%">{}</table>', ''.join(rows))
    settings_view.short_description = "Stored settings"




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

# admin.site.register(Integration, IntegrationAdmin)
admin.site.register(IntegrationAccessToken, IntegrationAccessTokenAdmin)
admin.site.register(SyncTableLogs, SyncTableLogsAdmin) 
admin.site.register(HighPriorityTask, HighPriorityTaskAdmin)