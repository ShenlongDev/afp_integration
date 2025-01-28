from django.urls import path, include
from rest_framework.routers import DefaultRouter
from integrations.views.general import IntegrationViewSet, IntegrationAuthView
from integrations.views.xero_views import XeroJournalImportView, XeroChartOfAccountsSyncView, XeroAccountsListCreateView, XeroAccountsDetailView, XeroBankTransactionLineItemsListCreateView, XeroBankTransactionLineItemsDetailView, XeroJournalLinesListCreateView, XeroJournalLinesDetailView

router = DefaultRouter()
router.register(r'', IntegrationViewSet, basename='integration')

urlpatterns = [
    path('', include(router.urls)),  
    path('<int:pk>/auth/', IntegrationAuthView.as_view(), name="integration-auth"),
    path('<int:pk>/import-journals/', XeroJournalImportView.as_view(), name='integration-import-journals'),
    path("<int:pk>/xero-sync-accounts/", XeroChartOfAccountsSyncView.as_view(), name="xero-sync-accounts"),



    # Xero example endpoints
    path('xero/accounts/', XeroAccountsListCreateView.as_view(), name='xero-accounts-list-create'),
    path('xero/accounts/<str:account_id>/', XeroAccountsDetailView.as_view(), name='xero-accounts-detail'),
    path('xero/bank-transaction-line-items/', XeroBankTransactionLineItemsListCreateView.as_view(), name='xero-bank-transaction-line-items-list-create'),
    path('xero/bank-transaction-line-items/<str:line_item_id>/', XeroBankTransactionLineItemsDetailView.as_view(), name='xero-bank-transaction-line-items-detail'),
    path('xero/journal-lines/', XeroJournalLinesListCreateView.as_view(), name='xero-journal-lines-list-create'),
    path('xero/journal-lines/<str:journal_line_id>/', XeroJournalLinesDetailView.as_view(), name='xero-journal-lines-detail'),
]
