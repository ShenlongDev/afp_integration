from django.shortcuts import render, redirect
from django.contrib import messages
from core.forms import DataImportForm
from core.models import HighPriorityTask


def import_data_view(request):
    if request.method == 'POST':
        form = DataImportForm(request.POST)
        if form.is_valid():
            integration = form.cleaned_data['integration']
            integration_type = form.cleaned_data['integration_type']
            since_date = form.cleaned_data['since_date']
            since_date_str = since_date.strftime("%Y-%m-%d")
            selected_modules = form.cleaned_data.get('modules', [])
            
            # Create a high priority task record.
            HighPriorityTask.objects.create(
                integration=integration,
                integration_type=integration_type,
                since_date=since_date,
                selected_modules=selected_modules,
                processed=False  # This flag ensures it's available for the dispatcher.
            )
            
            messages.info(
                request,
                "High priority data import record has been created. It will be processed shortly."
            )
            return redirect('admin:import-data')
    else:
        form = DataImportForm()
    
    return render(request, 'admin/data_import_form.html', {'form': form})
