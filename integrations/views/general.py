from rest_framework import viewsets
from integrations.models.models import Integration, IntegrationAccessToken
from integrations.services.netsuite.auth import NetSuiteAuthService
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from django.views import View
from django.shortcuts import get_object_or_404
from django.http import JsonResponse
from urllib.parse import parse_qs



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
        
        tokens = {}
        
        if integration.netsuite_account_id and integration.netsuite_consumer_key:
            service = NetSuiteAuthService(integration)
            tokens["Netsuite Token"] = service.get_access_token()
            authorization_url = service.get_authorization_url(state="my-netsuite-state")
            tokens["Netsuite Auth URL"] = authorization_url

        # if integration.xero_client_id and integration.xero_client_secret:
        #     tokens["Xero Token"] = request_new_xero_token(integration)
        
        return Response(tokens, status=status.HTTP_200_OK)
    
    
class IntegrationCallbackView(View):
    """
    Handles OAuth2 callback from various integrations.
    
    URL: /api/integrations/callback/
    Query Params:
        - type: "NETSUITE" | "XERO" | ...
        - code: Authorization code
        - state: State parameter
    """

    def get(self, request):
        integration_type = request.GET.get('type', '').upper()
        code = request.GET.get('code')
        state = request.GET.get('state')

        if not code or not state:
            return JsonResponse({"error": "Missing code or state parameters."}, status=400)

        if integration_type == "NETSUITE":
            return self.handle_netsuite_callback(request, code, state)
        elif integration_type == "XERO":
            return self.handle_xero_callback(request, code, state)
        else:
            return JsonResponse({"error": "Unsupported integration type."}, status=400)

    def handle_netsuite_callback(self, request, code, state):
        try:
            state_params = parse_qs(state)
            integration_pk = state_params.get('integration_pk', [None])[0]
            state_token = state_params.get('state_token', [None])[0]

            if not integration_pk or not state_token:
                return JsonResponse({"error": "Invalid state parameter"}, status=400)

            integration = get_object_or_404(Integration, pk=integration_pk)
            
            # Validate state token from session
            stored_state = request.session.get('netsuite_auth_state')
            if stored_state != state_token:
                return JsonResponse({"error": "Invalid state token"}, status=400)

            # Process authorization code
            auth_service = NetSuiteAuthService(
                integration.netsuite_account_id,
                integration
            )
            auth_service.handle_callback(code)
            
            # Cleanup session
            del request.session['netsuite_auth_state']
            
            return JsonResponse({"detail": "Authorization successful"})
            
        except Exception as e:
            return JsonResponse({"error": str(e)}, status=500)