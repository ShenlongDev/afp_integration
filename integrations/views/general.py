from rest_framework import viewsets
from integrations.models.models import Integration, IntegrationAccessToken
from integrations.serializers.general import IntegrationSerializer
from integrations.services.xero_client import authorize_xero
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status


class IntegrationViewSet(viewsets.ModelViewSet):
    queryset = Integration.objects.all()
    serializer_class = IntegrationSerializer


class IntegrationAuthView(APIView):
    """
    POST /api/integrations/<int:pk>/auth/
    Body: { "scope": "...optional custom scope..." }
    
    If integration_type == "XERO", fetch token via Xero client_credentials flow
    and create an IntegrationAccessToken row.
    """

    def post(self, request, pk=None):
        integration = Integration.objects.filter(pk=pk).first()
        if not integration:
            return Response(
                {"detail": "Integration not found."},
                status=status.HTTP_404_NOT_FOUND
            )

        return authorize_xero(integration)