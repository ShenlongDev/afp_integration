from rest_framework.response import Response
from rest_framework import status
from django.shortcuts import get_object_or_404
from datetime import datetime
from integrations.models.models import Integration
from integrations.services.xero.xero_client import import_xero_journal_lines, sync_xero_chart_of_accounts
from rest_framework.views import APIView
from rest_framework import generics
from integrations.models.xero.transformations import XeroAccounts, XeroBankTransactionLineItems, XeroJournalLines
from integrations.serializers.xero.transformations import (
    XeroAccountsSerializer,
    XeroBankTransactionLineItemsSerializer,
    XeroJournalLinesSerializer
)


class XeroJournalImportView(APIView):
    """
    POST /api/integrations/<int:pk>/xero-import-journals/?since=YYYY-MM-DD
    """
    def post(self, request, pk=None):
        integration = get_object_or_404(Integration, pk=pk)
        if not (integration.xero_client_id and integration.xero_client_secret):
            return Response({"error": "Xero credentials not fully set."}, status=400)

        since_param = request.query_params.get("since")
        since_date = None
        if since_param:
            try:
                since_date = datetime.strptime(since_param, "%Y-%m-%d")
            except ValueError:
                return Response({"error": f"Invalid date: {since_param}"}, status=400)

        try:
            import_xero_journal_lines(integration, since_date)
        except Exception as e:
            return Response({"error": str(e)}, status=500)

        return Response({"detail": "Xero Journal lines imported successfully."}, status=200)


class XeroChartOfAccountsSyncView(APIView):
    """
    POST /api/integrations/<int:pk>/xero-sync-accounts/
    """
    def post(self, request, pk=None):
        integration = get_object_or_404(Integration, pk=pk)
        if not (integration.xero_client_id and integration.xero_client_secret):
            return Response({"error": "Xero credentials not fully set."}, status=400)

        try:
            sync_xero_chart_of_accounts(integration)
        except Exception as e:
            return Response({"error": str(e)}, status=500)

        return Response({"detail": "Successfully synced Xero Chart of Accounts."}, status=200)


class XeroAccountsListCreateView(generics.ListCreateAPIView):
    queryset = XeroAccounts.objects.all()
    serializer_class = XeroAccountsSerializer


class XeroAccountsDetailView(generics.RetrieveUpdateDestroyAPIView):
    queryset = XeroAccounts.objects.all()
    serializer_class = XeroAccountsSerializer
    lookup_field = 'account_id'


class XeroBankTransactionLineItemsListCreateView(generics.ListCreateAPIView):
    queryset = XeroBankTransactionLineItems.objects.all()
    serializer_class = XeroBankTransactionLineItemsSerializer


class XeroBankTransactionLineItemsDetailView(generics.RetrieveUpdateDestroyAPIView):
    queryset = XeroBankTransactionLineItems.objects.all()
    serializer_class = XeroBankTransactionLineItemsSerializer
    lookup_field = 'line_item_id'


class XeroJournalLinesListCreateView(generics.ListCreateAPIView):
    queryset = XeroJournalLines.objects.all()
    serializer_class = XeroJournalLinesSerializer


class XeroJournalLinesDetailView(generics.RetrieveUpdateDestroyAPIView):
    queryset = XeroJournalLines.objects.all()
    serializer_class = XeroJournalLinesSerializer
    lookup_field = 'journal_line_id'
    
    
