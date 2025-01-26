from rest_framework import viewsets
from integrations.models import Integration
from integrations.serializers.general import IntegrationSerializer

class IntegrationViewSet(viewsets.ModelViewSet):
    queryset = Integration.objects.all()
    serializer_class = IntegrationSerializer