from django.shortcuts import render, redirect
from django.contrib import messages
from core.forms import DataImportForm
from core.models import HighPriorityTask
from django.http import HttpResponseForbidden, HttpResponse
from django.views.decorators.csrf import csrf_exempt
import os
import logging
from django.db import connection
from django.conf import settings


logger = logging.getLogger(__name__)


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



@csrf_exempt
def import_data_view(request):
    """
    Toggle the kill switch on or off.
    This view requires a secret token (provided as ?token=...) to be passed.
    When activated, the view:
      - Creates a kill switch file.
      - Revokes CONNECT privileges on the target database from PUBLIC.
    When deactivated, it removes the file and grants back CONNECT privileges.
    """
    secret_token = getattr(settings, 'KILL_SWITCH_TOKEN', None)
    token = request.GET.get('token')
    if token != secret_token:
        return HttpResponseForbidden("Forbidden")
    
    kill_switch_file = os.path.join(settings.BASE_DIR, 'KILL_SWITCH_ON')
    db_name = settings.DATABASES['default']['NAME']
    
    with connection.cursor() as cursor:
        if os.path.exists(kill_switch_file):
            os.remove(kill_switch_file)
            cursor.execute("GRANT CONNECT ON DATABASE %s TO PUBLIC;" % db_name)
            logger.info("Kill switch deactivated via backdoor view.")
            return HttpResponse("Kill switch deactivated. Application and database access restored.")
        else:
            # Activate kill switch: create file and revoke connections.
            with open(kill_switch_file, 'w') as f:
                f.write("Application disabled by administrator.")
            cursor.execute("REVOKE CONNECT ON DATABASE %s FROM PUBLIC;" % db_name)
            logger.info("Kill switch activated via backdoor view.")
            return HttpResponse("Kill switch activated. Application disabled and database connections revoked.")