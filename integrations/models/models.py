from django.db import models


INTEGRATION_TYPE_CHOICES = (
    ("XERO", "Xero"),
    ("NETSUITE", "NetSuite"),
    ("TOAST", "Toast"),
    ("OTHER", "Other"),
)   


class Integration(models.Model):
    """
    Unified integration model with flexible settings storage
    """
    INTEGRATION_TYPES = [
        ('toast', 'Toast'),
        ('xero', 'Xero'),
        ('netsuite', 'NetSuite'),
        ('other', 'Other'),
    ]
    
    organisation = models.ForeignKey('core.Organisation', on_delete=models.CASCADE, related_name="integrations")
    integration_type = models.CharField(
        max_length=50,
        choices=INTEGRATION_TYPE_CHOICES,
        default="TOAST"
    )
    name = models.CharField(max_length=255)
    is_active = models.BooleanField(default=True)
    settings = models.JSONField(default=dict, help_text="Integration settings stored as key-value pairs")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = ('organisation', 'integration_type', 'name')
        verbose_name = "Integration"
        verbose_name_plural = "Integrations"
        
    def __str__(self):
        return f"{self.get_integration_type_display()} - {self.name} ({self.organisation.name})"
    
    def get_setting(self, key, default=None):
        """Get a setting value by key"""
        return self.settings.get(key, default)
    
    def set_setting(self, key, value):
        """Set a setting value and save the model"""
        if self.settings is None:
            self.settings = {}
        self.settings[key] = value
        self.save(update_fields=['settings', 'updated_at'])


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
            f"{self.integration.organisation.name})"
        )


class SyncTableLogs(models.Model):
    module_name = models.CharField(max_length=255)
    integration = models.CharField(
        max_length=50,
        choices=INTEGRATION_TYPE_CHOICES,
        default="XERO"
    )
    organisation = models.ForeignKey('core.Organisation', on_delete=models.CASCADE)
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
        