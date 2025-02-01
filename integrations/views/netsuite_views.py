from rest_framework.response import Response
from rest_framework import status
from rest_framework.views import APIView
from rest_framework import generics
from integrations.models.models import Integration
from integrations.services.netsuite.importer import NetSuiteImporter
from django.shortcuts import get_object_or_404
from django.utils import timezone
from integrations.services.netsuite_transformer import NetSuiteTransformer

class NetsuiteImportDataView(APIView):
    """
    POST /api/integrations/<int:pk>/netsuite-import-data/?since=YYYY-MM-DD
    Triggers the import of NetSuite data for the specified integration.
    """

    def post(self, request, pk=None):
        integration = get_object_or_404(Integration, pk=pk)
        since_param = request.query_params.get("since")
        since_date = None
        if since_param:
            try:
                since_date = timezone.datetime.strptime(since_param, "%Y-%m-%d")
            except ValueError:
                return Response({"error": "Invalid date format. Use YYYY-MM-DD."}, status=status.HTTP_400_BAD_REQUEST)

        importer = NetSuiteImporter(integration)
        try:
            # importer.import_vendors(load_type="drop_and_reload")
            # importer.import_accounts()
            # importer.import_transactions()
            # importer.import_vendors()
            # importer.import_subsidiaries()
            # importer.import_departments()
            # importer.import_entities()
            # importer.import_accounting_periods()
            importer.map_net_suite_general_ledger()

            
            # After importing raw data, transform it
            # transformer = NetSuiteTransformer()
            
            # transformer.transform_general_ledger(integration)
            # transformer.transform_accounts()
            # Example: transformer.transform_transactions()
            # Example: transformer.transform_vendors()
            
            return Response({
                "detail": "NetSuite data imported successfully",
            }, status=status.HTTP_200_OK)
        except Exception as e:
            return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
