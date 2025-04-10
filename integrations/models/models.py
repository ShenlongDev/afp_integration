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
    until_date = models.DateField(null=True, blank=True)
    selected_modules = models.JSONField(default=list, blank=True)
    processed = models.BooleanField(default=False)
    in_progress = models.BooleanField(default=False)
    in_progress_since = models.DateTimeField(null=True, blank=True)
    processed_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"HighPriorityTask {self.id}"
    
    @property
    def processing_duration(self):
        if self.in_progress_since and self.processed_at:
            return self.processed_at - self.in_progress_since
        return None
    
    
class GenericIntegration(models.Model):
    """
    Generic integration model that can be used with any external service.
    Specific credentials are stored in the related IntegrationCredential model.
    """
    org = models.ForeignKey(Organisation, on_delete=models.CASCADE, related_name="generic_integrations")
    name = models.CharField(max_length=255)
    integration_type = models.CharField(max_length=50)
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.name} ({self.integration_type}) - {self.org.name}"
    
    def get_credential(self, key):
        """Helper method to get a credential value by key"""
        try:
            return self.credentials.get(key=key).value
        except IntegrationCredential.DoesNotExist:
            return None
    
    def set_credential(self, key, value):
        """Helper method to set a credential value"""
        IntegrationCredential.objects.update_or_create(
            integration=self,
            key=key,
            defaults={'value': value}
        )


class IntegrationCredential(models.Model):
    """
    Stores credentials for integrations in a key-value format.
    This allows for flexible credential storage for any integration type.
    """
    integration = models.ForeignKey(
        GenericIntegration, 
        on_delete=models.CASCADE,
        related_name="credentials"
    )
    key = models.CharField(max_length=255)
    value = models.TextField()
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        unique_together = ('integration', 'key')
        
    def __str__(self):
        return f"{self.integration.name} - {self.key}"
    