from django.contrib import admin
from .models import TaskLog
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



class TaskLogAdmin(admin.ModelAdmin):
    list_display = ('task_name', 'status', 'timestamp')
    search_fields = ('task_name', 'status')
    list_filter = ('status',)
    
# Register regular model admins
admin.site.register(TaskLog, TaskLogAdmin)

