from django.contrib import admin
from .models import Client, Organisation, Site, TaskLog
from core.forms import DataImportForm, BudgetImportForm
from django.urls import path
from django.shortcuts import render, redirect
from django.contrib import messages

from integrations.models.models import HighPriorityTask
from integrations.services.xero.xero_client import XeroDataImporter

# Create a standalone admin site mixin class for import functions
class ImportToolsMixin:
    """Mixin that provides import tool views for any admin class"""
    
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
                
                # Process the budget import directly
                try:
                    # Create importer instance with date range
                    importer = XeroDataImporter(integration, since_date)
                    # Import budgets with the until_date
                    importer.import_xero_budgets(until_date=until_date)
                    
                    messages.success(
                        request,
                        f"Successfully imported Xero budget data for {integration.org.name} from {since_date} to {until_date}"
                    )
                except Exception as e:
                    messages.error(
                        request,
                        f"Error importing Xero budget data: {str(e)}"
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


@admin.register(Client)
class ClientAdmin(admin.ModelAdmin):
    list_display = ('name', 'industry', 'status', 'billing_cycle', 'created_at')
    list_filter = ('status', 'billing_cycle', 'industry')
    search_fields = ('name', 'industry', 'primary_contact', 'billing_email')
    readonly_fields = ('created_at', 'updated_at')
    fieldsets = (
        ('Basic Information', {
            'fields': ('name', 'industry', 'timezone', 'primary_contact')
        }),
        ('Billing Information', {
            'fields': ('subscription_plan', 'billing_email', 'billing_cycle')
        }),
        ('Status and Reporting', {
            'fields': ('status', 'reporting_calendar')
        }),
        ('Timestamps', {
            'fields': ('created_at', 'updated_at'),
            'classes': ('collapse',)
        })
    )


@admin.register(Organisation)
class OrganisationAdmin(admin.ModelAdmin):
    list_display = ('name', 'client', 'status', 'created_at')
    list_filter = ('status', 'client')
    search_fields = ('name', 'client__name')
    readonly_fields = ('created_at', 'updated_at')
    fieldsets = (
        ('Basic Information', {
            'fields': ('name', 'client', 'status')
        }),
        ('Timestamps', {
            'fields': ('created_at', 'updated_at'),
            'classes': ('collapse',)
        })
    )

@admin.register(Site)
class SiteAdmin(admin.ModelAdmin):
    list_display = ('name', 'organisation', 'postcode', 'region', 'status', 'opened_date')
    list_filter = ('status', 'region', 'organisation')
    search_fields = ('name', 'postcode', 'region', 'organisation__name')
    readonly_fields = ('created_at', 'updated_at')
    fieldsets = (
        ('Basic Information', {
            'fields': ('name', 'organisation', 'status')
        }),
        ('Location Information', {
            'fields': ('postcode', 'region', 'opened_date')
        }),
        ('Timestamps', {
            'fields': ('created_at', 'updated_at'),
            'classes': ('collapse',)
        })
    )


@admin.register(TaskLog)
class TaskLogAdmin(admin.ModelAdmin):
    list_display = ('task_name', 'status', 'timestamp')
    search_fields = ('task_name', 'status')
    list_filter = ('status',)
    
admin.site.register(TaskLog, TaskLogAdmin)

