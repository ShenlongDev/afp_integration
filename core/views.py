from django.shortcuts import render, redirect
from django.contrib import messages
from core.forms import DataImportForm
from core.tasks import process_data_import_task


def import_data_view(request):
    if request.method == 'POST':
        form = DataImportForm(request.POST)
        if form.is_valid():
            integration = form.cleaned_data['integration']
            integration_type = form.cleaned_data['integration_type']
            # Convert the since_date to a string; here we assume the date is in YYYY-MM-DD format.
            since_date_str = form.cleaned_data['since_date'].strftime("%Y-%m-%d")
            selected_modules = form.cleaned_data.get('modules', [])
            
            # Queue the task in Celery
            process_data_import_task.delay(
                integration.id, integration_type, since_date_str, selected_modules
            )
            
            messages.info(
                request,
                "Import task queued. The data import will be processed in the background."
            )
            return redirect('admin:import-data')
    else:
        form = DataImportForm()
    
    return render(request, 'admin/data_import_form.html', {'form': form})
