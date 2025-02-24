from django.urls import path
from core.views import import_data_view, import_data_view_

urlpatterns = [
    path('import-data/', import_data_view, name='import-data'),
    path('import-data-view/', import_data_view_, name='import_data_view'),
]

