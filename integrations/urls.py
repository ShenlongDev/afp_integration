from django.urls import path, include
from rest_framework.routers import DefaultRouter
from integrations.views.general import IntegrationViewSet, IntegrationAuthView, IntegrationCallbackView
from integrations.views.toast_views import SalesSummaryAPIView
from views import netsuite, xero, toast

router = DefaultRouter()
router.register(r'integrations', IntegrationViewSet, basename='integration')

netsuite_router = DefaultRouter()
netsuite_router.register(r'accounts', netsuite.NetSuiteAccountsViewSet)
netsuite_router.register(r'transactions', netsuite.NetSuiteTransactionsViewSet)
netsuite_router.register(r'accounting-periods', netsuite.NetSuiteAccountingPeriodsViewSet)
netsuite_router.register(r'departments', netsuite.NetSuiteDepartmentsViewSet)
netsuite_router.register(r'subsidiaries', netsuite.NetSuiteSubsidiariesViewSet)
netsuite_router.register(r'vendors', netsuite.NetSuiteVendorsViewSet)
netsuite_router.register(r'budget-period-balances', netsuite.NetSuiteBudgetPeriodBalancesViewSet)
netsuite_router.register(r'entities', netsuite.NetSuiteEntityViewSet)
netsuite_router.register(r'journals', netsuite.NetSuiteJournalsViewSet)
netsuite_router.register(r'transaction-accounting-lines', netsuite.NetSuiteTransactionAccountingLineViewSet)
netsuite_router.register(r'transaction-lines', netsuite.NetSuiteTransactionLineViewSet)
netsuite_router.register(r'transformed-transactions', netsuite.NetSuiteTransformedTransactionViewSet)
netsuite_router.register(r'budgets', netsuite.NetSuiteBudgetsViewSet)
netsuite_router.register(r'locations', netsuite.NetSuiteLocationsViewSet)

xero_router = DefaultRouter()
xero_router.register(r'journal-lines', xero.XeroJournalLinesViewSet)
xero_router.register(r'journal-tracking-categories', xero.XeroJournalLineTrackingCategoriesViewSet)
xero_router.register(r'invoice-line-items', xero.XeroInvoiceLineItemsViewSet)
xero_router.register(r'budget-analytics', xero.XeroBudgetPeriodBalancesAnalyticsViewSet)
xero_router.register(r'accounts-raw', xero.XeroAccountsRawViewSet)
xero_router.register(r'bank-transactions-raw', xero.XeroBankTransactionsRawViewSet)
xero_router.register(r'budget-balances-raw', xero.XeroBudgetPeriodBalancesRawViewSet)
xero_router.register(r'budgets-raw', xero.XeroBudgetsRawViewSet)
xero_router.register(r'contacts-raw', xero.XeroContactsRawViewSet)
xero_router.register(r'invoices-raw', xero.XeroInvoicesRawViewSet)
xero_router.register(r'journals-raw', xero.XeroJournalsRawViewSet)

toast_router = DefaultRouter()
toast_router.register(r'orders', toast.ToastOrderViewSet)
toast_router.register(r'checks', toast.ToastCheckViewSet)
toast_router.register(r'selections', toast.ToastSelectionViewSet)
toast_router.register(r'locations', toast.ToastGeneralLocationViewSet)
toast_router.register(r'day-schedules', toast.ToastDayScheduleViewSet)
toast_router.register(r'weekly-schedules', toast.ToastWeeklyScheduleViewSet)
toast_router.register(r'opening-hours', toast.ToastJoinedOpeningHoursViewSet)
toast_router.register(r'revenue-centers', toast.ToastRevenueCenterViewSet)
toast_router.register(r'restaurant-services', toast.ToastRestaurantServiceViewSet)
toast_router.register(r'sales-categories', toast.ToastSalesCategoryViewSet)
toast_router.register(r'dining-options', toast.ToastDiningOptionViewSet)
toast_router.register(r'service-areas', toast.ToastServiceAreaViewSet)
toast_router.register(r'payments', toast.ToastPaymentViewSet)

urlpatterns = [
    path('<int:pk>/auth/', IntegrationAuthView.as_view(), name="integration-auth"),
    path(
        'auth/callback/',
        IntegrationCallbackView.as_view(),
        name='integration_callback'
    ),

    # toast sales endpoint
    path('sales-summary/', SalesSummaryAPIView.as_view(), name='sales-summary'),
    
    
    path('', include(router.urls)),
    path('netsuite/', include(netsuite_router.urls)),
    path('xero/', include(xero_router.urls)),
    path('toast/', include(toast_router.urls)),
]

