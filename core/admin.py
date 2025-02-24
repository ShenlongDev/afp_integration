from django.contrib import admin
from .models import Organisation, TaskLog
from core.forms import DataImportForm
from django.urls import path
from django.shortcuts import render, redirect
from django.contrib import messages


from integrations.models.models import HighPriorityTask

class DataImportAdmin(admin.ModelAdmin):
    change_list_template = "admin/data_import_changelist.html"  

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
                integration = form.cleaned_data['integration']
                integration_type = form.cleaned_data['integration_type']
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

class OrganisationAdmin(admin.ModelAdmin):
    list_display = ('name',)
    search_fields = ('name',)
    

class TaskLogAdmin(admin.ModelAdmin):
    list_display = ('task_name', 'status', 'timestamp')
    search_fields = ('task_name', 'status')
    list_filter = ('status',)
    
admin.site.register(Organisation, DataImportAdmin)
admin.site.register(TaskLog)