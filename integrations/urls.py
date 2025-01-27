from django.urls import path, include
from rest_framework.routers import DefaultRouter
from integrations.views.general import IntegrationViewSet, IntegrationAuthView
from integrations.views.xero_views import XeroJournalImportView, XeroChartOfAccountsSyncView

router = DefaultRouter()
router.register(r'', IntegrationViewSet, basename='integration')

urlpatterns = [
    path('', include(router.urls)),  
    path('<int:pk>/auth/', IntegrationAuthView.as_view(), name="integration-auth"),
    path('<int:pk>/import-journals/', XeroJournalImportView.as_view(), name='integration-import-journals'),
    path("<int:pk>/xero-sync-accounts/", XeroChartOfAccountsSyncView.as_view(), name="xero-sync-accounts"),

]
