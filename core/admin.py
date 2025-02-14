from django.contrib import admin
from .models import Organisation
from core.forms import DataImportForm
from django.urls import path
from django.shortcuts import render, redirect
from django.contrib import messages

from core.tasks import (
    xero_sync_accounts_task, 
    netsuite_import_accounts,  
)

class DataImportAdmin(admin.ModelAdmin):
    change_list_template = "admin/data_import_changelist.html"  # custom template for the changelist

    def get_urls(self):
        urls = super().get_urls()
        custom_urls = [
            path('import-data/', self.admin_site.admin_view(self.import_data_view), name="import-data"),
        ]
        return custom_urls + urls

    def import_data_view(self, request):
        if request.method == 'POST':
            form = DataImportForm(request.POST)
            if form.is_valid():
                integration_type = form.cleaned_data['integration_type']
                organisation = form.cleaned_data['organisation']
                integration = form.cleaned_data.get('integration')
                since_date = form.cleaned_data['since_date']
                modules = form.cleaned_data.get('modules')
                since_str = since_date.strftime('%Y-%m-%d')

                # For demonstration, we trigger a simple celery task.
                # In a real scenario, you might dispatch one task per module or build a chain.
                if integration_type == 'XERO':
                    if integration:
                        # Dispatch tasks for the selected integration
                        xero_sync_accounts_task.delay(integration.id, since_str)
                        # Dispatch other tasks (e.g., journal lines, contacts, etc.) as needed.
                    else:
                        # If no specific integration is selected, you might run for all Xero integrations.
                        # For example, call a task that loops through integrations.
                        from core.tasks import sync_xero_data
                        sync_xero_data.delay(since_str)
                    messages.success(request, "Xero import has been initiated.")
                elif integration_type == 'NETSUITE':
                    if integration:
                        netsuite_import_accounts.delay(integration.id)
                        # Similarly, dispatch additional tasks based on modules.
                    else:
                        # For all NetSuite integrations:
                        from core.tasks import sync_netsuite_data
                        sync_netsuite_data.delay(organisation.id)  # example parameter
                    messages.success(request, "NetSuite import has been initiated.")
                else:
                    messages.error(request, "Unknown integration type.")

                return redirect("..")
        else:
            form = DataImportForm()

        context = {
            **self.admin_site.each_context(request),
            'form': form,
            'title': "Data Import",
        }
        return render(request, "admin/data_import_form.html", context)

class OrganisationAdmin(admin.ModelAdmin):
    list_display = ('name',)
    search_fields = ('name',)
    
    
admin.site.register(Organisation, DataImportAdmin)