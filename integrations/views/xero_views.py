from rest_framework.response import Response
from rest_framework import status
from django.shortcuts import get_object_or_404
from integrations.models import Integration
from integrations.services.xero_client import get_journals
from rest_framework.views import APIView


class IntegrationJournalsImportView(APIView):
    """
    POST /api/integrations/<int:pk>/import-journals/
    
    Example of manually triggering a Xero data import. 
    (We assume integration_type == "XERO" for now.)
    """

    def post(self, request, pk=None):
        integration = get_object_or_404(Integration, pk=pk)

        if integration.integration_type != "XERO":
            return Response(
                {"detail": "Integration type is not XERO."},
                status=status.HTTP_400_BAD_REQUEST
            )

        total_imported = 0
        for journal in get_journals(integration):
            # Save the journal or transform as needed...
            total_imported += 1

        return Response(
            {"message": "Xero Journals imported", "count": total_imported},
            status=status.HTTP_200_OK
        )