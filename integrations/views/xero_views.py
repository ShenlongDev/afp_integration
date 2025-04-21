from rest_framework.response import Response
from rest_framework import status
from django.shortcuts import get_object_or_404
from integrations.services.xero.xero_client import XeroDataImporter
from datetime import datetime
from integrations.models.models import Integration
from rest_framework.views import APIView
from rest_framework import generics
from integrations.models.xero.raw import XeroAccountsRaw
from integrations.models.xero.transformations import XeroJournalLines, XeroJournalLineTrackingCategories, XeroInvoiceLineItems
from rest_framework import viewsets
from integrations.models.xero.analytics import XeroBudgetPeriodBalancesAnalytics
from integrations.models.xero.raw import (
    XeroBankTransactionsRaw, XeroBudgetPeriodBalancesRaw, XeroBudgetsRaw,
    XeroContactsRaw, XeroInvoicesRaw, XeroJournalsRaw, 
)
from integrations.serializers.xero import (
    XeroBudgetPeriodBalancesAnalyticsSerializer, XeroAccountsRawSerializer, XeroBankTransactionsRawSerializer,
    XeroBudgetPeriodBalancesRawSerializer, XeroBudgetsRawSerializer, XeroContactsRawSerializer,
    XeroInvoicesRawSerializer, XeroJournalsRawSerializer, XeroJournalLinesSerializer, XeroJournalLineTrackingCategoriesSerializer,
    XeroInvoiceLineItemsSerializer
)


class XeroAccountsListCreateView(generics.ListCreateAPIView):
    queryset = XeroAccountsRaw.objects.all()
    serializer_class = XeroAccountsRawSerializer


class XeroAccountsDetailView(generics.RetrieveUpdateDestroyAPIView):
    queryset = XeroAccountsRaw.objects.all()
    serializer_class = XeroAccountsRawSerializer
    lookup_field = 'account_id'


class XeroJournalLinesListCreateView(generics.ListCreateAPIView):
    queryset = XeroJournalLines.objects.all()
    serializer_class = XeroJournalLinesSerializer


class XeroJournalLinesDetailView(generics.RetrieveUpdateDestroyAPIView):
    queryset = XeroJournalLines.objects.all()
    serializer_class = XeroJournalLinesSerializer
    lookup_field = 'journal_line_id'


class XeroJournalLinesViewSet(viewsets.ModelViewSet):
    queryset = XeroJournalLines.objects.all()
    serializer_class = XeroJournalLinesSerializer
    filterset_fields = ['tenant_id', 'journal_id', 'journal_date']


class XeroBudgetPeriodBalancesAnalyticsViewSet(viewsets.ModelViewSet):
    queryset = XeroBudgetPeriodBalancesAnalytics.objects.all()
    serializer_class = XeroBudgetPeriodBalancesAnalyticsSerializer
    filterset_fields = ['tenant_id', 'budget_id', 'account_id']


class XeroAccountsRawViewSet(viewsets.ModelViewSet):
    queryset = XeroAccountsRaw.objects.all()
    serializer_class = XeroAccountsRawSerializer
    filterset_fields = ['tenant_id', 'account_id']


class XeroBankTransactionsRawViewSet(viewsets.ModelViewSet):
    queryset = XeroBankTransactionsRaw.objects.all()
    serializer_class = XeroBankTransactionsRawSerializer
    filterset_fields = ['tenant_id', 'bank_transaction_id']


class XeroBudgetPeriodBalancesRawViewSet(viewsets.ModelViewSet):
    queryset = XeroBudgetPeriodBalancesRaw.objects.all()
    serializer_class = XeroBudgetPeriodBalancesRawSerializer
    filterset_fields = ['tenant_id', 'budget_id', 'account_id']


class XeroBudgetsRawViewSet(viewsets.ModelViewSet):
    queryset = XeroBudgetsRaw.objects.all()
    serializer_class = XeroBudgetsRawSerializer
    filterset_fields = ['tenant_id', 'budget_id']


class XeroContactsRawViewSet(viewsets.ModelViewSet):
    queryset = XeroContactsRaw.objects.all()
    serializer_class = XeroContactsRawSerializer
    filterset_fields = ['tenant_id', 'contact_id']


class XeroInvoicesRawViewSet(viewsets.ModelViewSet):
    queryset = XeroInvoicesRaw.objects.all()
    serializer_class = XeroInvoicesRawSerializer
    filterset_fields = ['tenant_id', 'invoice_id']


class XeroJournalsRawViewSet(viewsets.ModelViewSet):
    queryset = XeroJournalsRaw.objects.all()
    serializer_class = XeroJournalsRawSerializer
    filterset_fields = ['tenant_id', 'journal_id']
    

class XeroJournalLineTrackingCategoriesViewSet(viewsets.ModelViewSet):
    queryset = XeroJournalLineTrackingCategories.objects.all()
    serializer_class = XeroJournalLineTrackingCategoriesSerializer
    filterset_fields = ['tenant_id', 'jounal_line_id', 'tracking_category_id']
    
    
class XeroInvoiceLineItemsViewSet(viewsets.ModelViewSet):
    queryset = XeroInvoiceLineItems.objects.all()
    serializer_class = XeroInvoiceLineItemsSerializer
    filterset_fields = ['tenant_id', 'invoice_id']