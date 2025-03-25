from django.db import models
from core.models import Organisation


INTEGRATION_TYPE_CHOICES = (
    ("XERO", "Xero"),
    ("NETSUITE", "NetSuite"),
    ("TOAST", "Toast"),
)   


class Integration(models.Model):
    """
    One record that can store credentials for multiple possible integrations
    (Xero, NetSuite, etc.). Just fill the fields relevant to the integration_type.
    """
    org = models.ForeignKey(Organisation, on_delete=models.CASCADE, related_name="integrations")

    xero_client_id = models.CharField(max_length=255, blank=True, null=True)
    xero_client_secret = models.CharField(max_length=255, blank=True, null=True)

    netsuite_account_id = models.CharField(max_length=255, blank=True, null=True)
    netsuite_client_id = models.CharField(max_length=255, blank=True, null=True)
    netsuite_client_secret = models.CharField(max_length=255, blank=True, null=True)
    netsuite_consumer_key = models.CharField(max_length=255, blank=True, null=True)
    netsuite_private_key = models.TextField(blank=True, null=True)
    netsuite_certificate_id = models.CharField(max_length=255, blank=True, null=True)
    
    toast_api_url = models.CharField(max_length=100, blank=True, null=True)
    toast_client_id = models.CharField(max_length=255, blank=True, null=True)
    toast_client_secret = models.CharField(max_length=255, blank=True, null=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.org.name}"
    
    
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
    refresh_token = models.TextField(null=True, blank=True)
    
    expires_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True, null=True, blank=True)

    def __str__(self):
        return (
            f"Token({self.integration_type} | "
            f"{self.integration.org.name})"
        )


class SyncTableLogs(models.Model):
    module_name = models.CharField(max_length=255)
    integration = models.CharField(
        max_length=50,
        choices=INTEGRATION_TYPE_CHOICES,
        default="XERO"
    )
    organization = models.ForeignKey(Organisation, on_delete=models.CASCADE)
    fetched_records = models.IntegerField()
    last_updated_time = models.DateTimeField()
    last_updated_date = models.DateField()

    def __str__(self):
        return f"{self.module_name} import on {self.last_updated_date} ({self.fetched_records} records)" 


class HighPriorityTask(models.Model):
    integration = models.ForeignKey(Integration, on_delete=models.CASCADE)
    integration_type = models.CharField(max_length=50)
    since_date = models.DateField()
    selected_modules = models.JSONField(default=list, blank=True)
    processed = models.BooleanField(default=False)
    in_progress = models.BooleanField(default=False)
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"HighPriorityTask {self.id}"
    