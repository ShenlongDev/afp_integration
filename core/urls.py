from django.urls import path
from core.views import import_data_view, import_data_view_, import_budgets_view
from django.contrib import admin
from core.admin import ImportToolsMixin

urlpatterns = [
    path('import-data/', import_data_view, name='import-data'),
    path('import-data-view/', import_data_view_, name='import_data_view'),
    path('admin/import-data/', import_data_view, name='admin:import-data'),
    path('admin/import-budgets/', import_budgets_view, name='admin:import-budgets'),
]

# Create the mixin instance to use its methods
import_tools = ImportToolsMixin()

# Add the URLs to the admin site
admin.site.get_urls = lambda: import_tools.get_import_urls() + admin.site.get_urls.__wrapped__(admin.site)

