from django.db import models
from core.models import Organisation


INTEGRATION_TYPE_CHOICES = (
    ("XERO", "Xero"),
    ("NETSUITE", "NetSuite"),
)

class Integration(models.Model):
    """
    One record that can store credentials for multiple possible integrations
    (Xero, NetSuite, etc.). Just fill the fields relevant to the integration_type.
    """
    org = models.ForeignKey(Organisation, on_delete=models.CASCADE, related_name="integrations")

    integration_type = models.CharField(
        max_length=50,
        choices=INTEGRATION_TYPE_CHOICES,
        default="XERO"
    )

    xero_client_id = models.CharField(max_length=255, blank=True, null=True)
    xero_client_secret = models.CharField(max_length=255, blank=True, null=True)
    xero_tenant_id = models.CharField(max_length=255, blank=True, null=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.org.name} - {self.integration_type}"
    
    
class IntegrationAccessToken(models.Model):
    """
    Stores access tokens for any given integration.
    Example: a short-lived Xero token (Custom Connection).
    """
    integration = models.ForeignKey(Integration, on_delete=models.CASCADE, related_name="access_tokens")

    token = models.TextField()
    expires_at = models.DateTimeField()

    token_type = models.CharField(max_length=50, default="bearer")

    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"Token({self.integration.org.name}-{self.integration.integration_type})"
