from django.db import models
from core.models import Organisation


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

    organisation     = models.ForeignKey(Organisation, on_delete=models.CASCADE, related_name="integrations_org")
    integration_type = models.CharField(max_length=50,
                                        choices=INTEGRATION_TYPES,
                                        default='toast')
    name      = models.CharField(max_length=255)
    is_active = models.BooleanField(default=True)
    settings  = models.JSONField(default=dict, blank=True)
    
    # old fields
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

    class Meta:
        unique_together = ('organisation', 'integration_type')
        verbose_name_plural = 'Integrations'

    def __str__(self):
        return f'{self.get_integration_type_display()} – {self.name}'


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
        )


class SyncTableLogs(models.Model):
    module_name = models.CharField(max_length=255)
    integration = models.CharField(
        max_length=50,
        choices=INTEGRATION_TYPE_CHOICES,
        default="XERO"
    )
    organisation = models.ForeignKey(Organisation, on_delete=models.CASCADE)
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
        

class POSSales(models.Model):
    order_id = models.CharField(max_length=255, blank=True, null=True)
    date_ntz = models.DateTimeField(blank=True, null=True)
    staff_name = models.CharField(max_length=255, blank=True, null=True)
    sales_area = models.CharField(max_length=255, blank=True, null=True)
    service = models.CharField(max_length=255, blank=True, null=True)
    sale_type = models.CharField(max_length=255, blank=True, null=True)
    currency = models.CharField(max_length=3, blank=True, null=True)
    net_amount = models.FloatField(blank=True, null=True)
    gross_amount = models.FloatField(blank=True, null=True)
    tax = models.FloatField(blank=True, null=True)
    discount = models.FloatField(blank=True, null=True)
    gratuity = models.FloatField(blank=True, null=True)
    refund = models.FloatField(blank=True, null=True)
    void = models.BooleanField(blank=True, null=True)
    item_type = models.CharField(max_length=255, blank=True, null=True)
    item_category = models.CharField(max_length=255, blank=True, null=True)
    item_product_name = models.CharField(max_length=255, blank=True, null=True)
    item_quantity = models.FloatField(blank=True, null=True)
    item_variation = models.CharField(max_length=255, blank=True, null=True)
    item_net_amount = models.FloatField(blank=True, null=True)
    item_gross_amount = models.FloatField(blank=True, null=True)
    item_tax = models.FloatField(blank=True, null=True)
    item_line_discount = models.FloatField(blank=True, null=True)
    item_refunded = models.BooleanField(blank=True, null=True)
    item_index = models.DecimalField(max_digits=38, decimal_places=0, blank=True, null=True)
    outlet_name = models.CharField(max_length=255, blank=True, null=True)
    location = models.CharField(max_length=255, blank=True, null=True)
    register_name = models.CharField(max_length=255, blank=True, null=True)
    dining_options = models.CharField(max_length=255, blank=True, null=True)
    covers = models.DecimalField(max_digits=38, decimal_places=0, blank=True, null=True)
    receipt_no = models.CharField(max_length=255, blank=True, null=True)
    checks = models.CharField(max_length=255, blank=True, null=True)
    duration = models.FloatField(blank=True, null=True)
    order_status = models.CharField(max_length=255, blank=True, null=True)
    client_name = models.CharField(max_length=255, blank=True, null=True)
    source_system = models.CharField(max_length=255, blank=True, null=True)
    business_date = models.DateField(blank=True, null=True)
    date = models.DateTimeField(blank=True, null=True)
    opened = models.DateTimeField(blank=True, null=True)
    paid = models.DateTimeField(blank=True, null=True)
    closed = models.DateTimeField(blank=True, null=True)
    modified = models.DateTimeField(blank=True, null=True)
    automation = models.CharField(max_length=255, blank=True, null=True)
    item_voided = models.BooleanField(blank=True, null=True)
    organisation_id = models.DecimalField(max_digits=38, decimal_places=0, blank=True, null=True)
    client_id = models.DecimalField(max_digits=38, decimal_places=0, blank=True, null=True)
    site_id = models.DecimalField(max_digits=38, decimal_places=0, blank=True, null=True)
    item_id = models.CharField(max_length=255, blank=True, null=True)

    class Meta:
        unique_together = ('order_id', 'item_id')
        indexes = [
            models.Index(fields=['order_id', 'item_id']),
        ]

    def __str__(self):
        return f"Order {self.order_id}" if self.order_id else "POS Sale" 

class Weather(models.Model):
    client = models.ForeignKey('core.Client', on_delete=models.SET_NULL, null=True, blank=True, related_name='weather_records')
    organisation = models.ForeignKey('core.Organisation', on_delete=models.SET_NULL, null=True, blank=True, related_name='weather_records')
    site = models.ForeignKey('core.Site', on_delete=models.SET_NULL, null=True, blank=True, related_name='weather_records')
    
    client_name = models.CharField(max_length=255)
    store_name = models.CharField(max_length=255)
    city = models.CharField(max_length=255)
    latitude = models.FloatField()
    longitude = models.FloatField()
    
    temperature_value = models.FloatField()
    temperature_unit = models.CharField(max_length=10)
    
    pressure_value = models.FloatField()
    pressure_unit = models.CharField(max_length=10)
    
    relative_humidity = models.FloatField()
    wind_speed_value = models.FloatField()
    wind_speed_unit = models.CharField(max_length=10)
    wind_direction_degrees = models.FloatField()
    windgust_speed_value = models.FloatField(null=True, blank=True)
    windgust_speed_unit = models.CharField(max_length=10, null=True, blank=True)
    
    cloudiness = models.IntegerField()
    sunrise = models.DateTimeField()
    sunset = models.DateTimeField()
    rain = models.FloatField(null=True, blank=True)
    snow = models.FloatField(null=True, blank=True)
    
    status = models.CharField(max_length=50)
    code = models.IntegerField()
    description = models.TextField()
    icon = models.CharField(max_length=255)
    record_date = models.DateTimeField()
    source_system = models.CharField(max_length=50)
    automation = models.CharField(max_length=255)
    
    tags = models.JSONField(null=True, blank=True)
    masking_policy = models.CharField(max_length=50, null=True, blank=True)
    action_group = models.CharField(max_length=50, null=True, blank=True)
    
    class Meta:
        ordering = ['-record_date']
        indexes = [
            models.Index(fields=['client_name']),
            models.Index(fields=['store_name']),
            models.Index(fields=['city']),
            models.Index(fields=['record_date']),
            models.Index(fields=['status']),
        ]

    def __str__(self):
        return f"Weather for {self.store_name} ({self.city}) at {self.record_date}"
