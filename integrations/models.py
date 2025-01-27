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

    xero_client_id = models.CharField(max_length=255, blank=True, null=True)
    xero_client_secret = models.CharField(max_length=255, blank=True, null=True)
    xero_tenant_id = models.CharField(max_length=255, blank=True, null=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.org.name} - {self.integration_type}"
    
    
class IntegrationAccessToken(models.Model):
    """
    Stores access tokens for integrations, with expiry and other metadata.
    """
    integration = models.ForeignKey(
        Integration,
        on_delete=models.CASCADE,
        related_name="access_tokens"
    )
    integration_type = models.CharField(
        max_length=50,
        choices=INTEGRATION_TYPE_CHOICES,
        default="XERO"
    )
    token = models.TextField()
    
    expires_at = models.DateTimeField()
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return (
            f"Token({self.integration_type} | "
            f"{self.integration.org.name})"
        )


class ChartOfAccounts(models.Model):
    integration = models.ForeignKey(Integration, on_delete=models.CASCADE, related_name="chart_of_accounts")
    account_id = models.CharField(max_length=36, unique=True)  # e.g. Xero's or NetSuite's ID
    code = models.CharField(max_length=50, blank=True, null=True)
    name = models.CharField(max_length=255, blank=True, null=True)
    table_name = models.CharField(max_length=255, blank=True, null=True)

    status = models.CharField(max_length=50, blank=True, null=True)
    account_type = models.CharField(max_length=50, blank=True, null=True)
    currency_code = models.CharField(max_length=10, blank=True, null=True)
    tax_type = models.CharField(max_length=50, blank=True, null=True)
    description = models.TextField(blank=True, null=True)

    updated_utc = models.DateTimeField(blank=True, null=True)
    insights_source = models.CharField(max_length=50, default="Xero")  # or "NetSuite"
    insights_imported_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"[{self.integration.integration_type}] {self.code} - {self.name}"
