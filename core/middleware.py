# core/middleware.py
import os
from django.http import HttpResponseServerError
from django.conf import settings

class KillSwitchMiddleware:
    """
    If a special kill switch file exists, return a 503 Service Unavailable response.
    """
    def __init__(self, get_response):
        self.get_response = get_response
        self.kill_switch_file = os.path.join(settings.BASE_DIR, 'KILL_SWITCH_ON')

    def __call__(self, request):
        if os.path.exists(self.kill_switch_file):
            return HttpResponseServerError("Service Unavailable: The application is disabled by the administrator.")
        return self.get_response(request)
