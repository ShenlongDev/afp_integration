from rest_framework.response import Response
from rest_framework import status
from django.shortcuts import get_object_or_404
from datetime import datetime
from integrations.models import Integration
from integrations.services.xero_client import get_journals, import_xero_journal_lines, sync_xero_chart_of_accounts
from rest_framework.views import APIView


class XeroJournalImportView(APIView):
    """
    POST /api/integrations/<int:pk>/xero-import-journals/?since=YYYY-MM-DD
    Example usage:
      POST /api/integrations/5/xero-import-journals/?since=2023-01-01
    This triggers the import of Xero Journal Lines from the given date to now.
    """

    def post(self, request, pk=None):
        integration = get_object_or_404(Integration, pk=pk)

        since_param = request.query_params.get("since", None)
        since_date = None
        if since_param:
            try:
                since_date = datetime.strptime(since_param, "%Y-%m-%d")
            except ValueError:
                return Response(
                    {"error": f"Invalid 'since' date format: {since_param}"},
                    status=status.HTTP_400_BAD_REQUEST
                )

        try:
            import_xero_journal_lines(integration, since_date=since_date)
        except Exception as e:
            return Response({"error": str(e)}, status=status.HTTP_400_BAD_REQUEST)

        return Response({"detail": "Xero Journal lines imported successfully."}, status=200)
        

class XeroChartOfAccountsSyncView(APIView):
    """
    POST /api/integrations/<int:pk>/xero-sync-accounts/
    """

    def post(self, request, pk=None):
        integration = get_object_or_404(Integration, pk=pk)
        if not integration.xero_client_id or not integration.xero_client_secret:
            return Response(
                {"error": "Integration is not of type XERO."},
                status=status.HTTP_400_BAD_REQUEST
            )

        sync_xero_chart_of_accounts(integration)


        return Response({"detail": "Successfully synced Xero Chart of Accounts."}, status=200)