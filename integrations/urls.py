from django.urls import path, include
from rest_framework.routers import DefaultRouter
from integrations.views.general import IntegrationViewSet, IntegrationAuthView, IntegrationCallbackView
from integrations.views.xero_views import XeroDataImportView, XeroAccountsListCreateView, XeroAccountsDetailView, XeroJournalLinesListCreateView, XeroJournalLinesDetailView
from integrations.views.netsuite_views import NetsuiteImportDataView
from integrations.views.toast import SalesSummaryAPIView

router = DefaultRouter()
router.register(r'integrations', IntegrationViewSet, basename='integration')

urlpatterns = [
    path('<int:pk>/auth/', IntegrationAuthView.as_view(), name="integration-auth"),
    path('<int:pk>/xero_import-data/', XeroDataImportView.as_view(), name='integration-import-journals'),
    path('<int:pk>/netsuite_import-data/', NetsuiteImportDataView.as_view(), name='integration-import-netsuite'),
     path(
        'auth/callback/',
        IntegrationCallbackView.as_view(),
        name='integration_callback'
    ),

    # Xero endpoints
    path('xero/accounts/', XeroAccountsListCreateView.as_view(), name='xero-accounts-list-create'),
    path('xero/accounts/<str:account_id>/', XeroAccountsDetailView.as_view(), name='xero-accounts-detail'),
    path('xero/journal-lines/', XeroJournalLinesListCreateView.as_view(), name='xero-journal-lines-list-create'),
    path('xero/journal-lines/<str:journal_line_id>/', XeroJournalLinesDetailView.as_view(), name='xero-journal-lines-detail'),

    # toast endpoints
    path('sales-summary/', SalesSummaryAPIView.as_view(), name='sales-summary'),
    
    
    path('', include(router.urls)),  

]

