import requests
from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework import status
from django.shortcuts import get_object_or_404
from integrations.models import Integration
from integrations.services.xero_client import get_journals
from integrations.models import IntegrationAccessToken
from datetime import timedelta
from django.utils import timezone
from rest_framework.views import APIView


@api_view(["POST"])
def trigger_xero_journals_import(request, integration_id):
    """
    POST /api/integrations/xero/<integration_id>/import-journals/
    Example of manually triggering a Xero data import.
    """
    integration = get_object_or_404(Integration, pk=integration_id)

    # Example: calling your get_journals logic
    total_imported = 0
    for journal in get_journals(integration):
        # Save the journal somewhere or do transformation
        total_imported += 1
    
    return Response({"message": "Xero Journals imported", "count": total_imported}, status=status.HTTP_200_OK)


class IntegrationAuthView(APIView):
    """
    POST /api/integrations/<int:pk>/auth/
    Body: { "scope": "...optional custom scope..." }
    - Authenticates with Xero (or other integration) using client credentials.
    - Stores new token in IntegrationAccessToken with expiry.
    """

    def post(self, request, pk=None):
        integration = Integration.objects.filter(pk=pk).first()
        if not integration:
            return Response({"detail": "Integration not found."}, status=status.HTTP_404_NOT_FOUND)

        if integration.integration_type == "XERO":
            return self._authorize_xero(integration, request)
        elif integration.integration_type == "NETSUITE":
            return Response({"detail": "NetSuite logic not implemented yet."}, status=501)
        else:
            return Response({"detail": "Integration type not recognized."}, status=400)

    def _authorize_xero(self, integration: Integration, request):
        # client_credentials flow
        client_id = integration.xero_client_id
        client_secret = integration.xero_client_secret
        if not client_id or not client_secret:
            return Response(
                {"detail": "Xero client credentials not found on this Integration."}, 
                status=400
            )

        token_url = "https://identity.xero.com/connect/token"
        auth = (client_id, client_secret)

        # If you want custom scopes from request body, or default:
        scope = request.data.get("scope", 
            "accounting.transactions accounting.settings "
            "accounting.reports.read accounting.journals.read "
            "accounting.budgets.read"
        )

        data = {
            "grant_type": "client_credentials",
            "scope": scope,
        }

        resp = requests.post(token_url, data=data, auth=auth)
        if resp.status_code != 200:
            return Response(
                {"detail": f"Xero token request failed: {resp.text}"},
                status=resp.status_code
            )

        token_data = resp.json()
        access_token = token_data["access_token"]
        expires_in = token_data.get("expires_in", 1800)  # default 30 min

        expires_at = timezone.now() + timedelta(seconds=expires_in)

        xero_token = IntegrationAccessToken.objects.create(
            integration=integration,
            token=access_token,
            expires_at=expires_at,
            token_type="bearer",
        )

        return Response({
            "detail": "Xero token acquired successfully.",
            "access_token": access_token,
            "expires_at": expires_at.isoformat()
        }, status=200)