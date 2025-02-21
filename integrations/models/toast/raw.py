from django.db import models
from django.contrib.postgres.fields import JSONField

class ToastOrder(models.Model):
    integration = models.ForeignKey("integrations.Integration", on_delete=models.CASCADE)
    order_guid = models.CharField(max_length=255, unique=True)
    payload = JSONField(help_text="Raw order data from Toast")
    order_net_sales = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    import_id = models.IntegerField(help_text="Reference to the integration ID")
    ws_import_date = models.DateTimeField(auto_now_add=True)
    created_date = models.DateTimeField(null=True, blank=True)
    closed_date = models.DateTimeField(null=True, blank=True)
    modified_date = models.DateTimeField(null=True, blank=True)

    def __str__(self):
        return f"ToastOrder {self.order_guid}"


class ToastGeneralLocation(models.Model):
    guid = models.CharField(max_length=255, unique=True)
    general_name = models.CharField(max_length=255)
    location_name = models.CharField(max_length=255, null=True, blank=True)
    location_code = models.CharField(max_length=50, null=True, blank=True)
    description = models.TextField(null=True, blank=True)
    timezone = models.CharField(max_length=50, null=True, blank=True)
    closeout_hour = models.TimeField(null=True, blank=True)
    management_group_guid = models.CharField(max_length=255, null=True, blank=True)
    currency_code = models.CharField(max_length=10, null=True, blank=True)
    first_business_date = models.DateField(null=True, blank=True)
    archived = models.BooleanField(default=False)
    # Example URL and address fields:
    url_website = models.URLField(null=True, blank=True)
    url_facebook = models.URLField(null=True, blank=True)
    url_twitter = models.URLField(null=True, blank=True)
    url_order_online = models.URLField(null=True, blank=True)
    address_line1 = models.CharField(max_length=255, null=True, blank=True)
    address_line2 = models.CharField(max_length=255, null=True, blank=True)
    city = models.CharField(max_length=100, null=True, blank=True)
    state_code = models.CharField(max_length=50, null=True, blank=True)
    zip_code = models.CharField(max_length=20, null=True, blank=True)
    country = models.CharField(max_length=100, null=True, blank=True)
    phone = models.CharField(max_length=20, null=True, blank=True)
    latitude = models.DecimalField(max_digits=9, decimal_places=6, null=True, blank=True)
    longitude = models.DecimalField(max_digits=9, decimal_places=6, null=True, blank=True)

    def __str__(self):
        return f"ToastGeneralLocation {self.general_name}"


class ToastDaySchedule(models.Model):
    integration = models.ForeignKey("integrations.Integration", on_delete=models.CASCADE)
    restaurant = models.ForeignKey(ToastGeneralLocation, on_delete=models.CASCADE)
    guid = models.CharField(max_length=255, unique=True)
    property_name = models.CharField(max_length=255)
    open_time = models.TimeField(null=True, blank=True)
    close_time = models.TimeField(null=True, blank=True)
    schedule_name = models.CharField(max_length=255, null=True, blank=True)

    def __str__(self):
        return f"ToastDaySchedule {self.guid}"


class ToastWeeklySchedule(models.Model):
    integration = models.ForeignKey("integrations.Integration", on_delete=models.CASCADE)
    restaurant = models.ForeignKey(ToastGeneralLocation, on_delete=models.CASCADE)
    monday = models.CharField(max_length=255, null=True, blank=True)
    tuesday = models.CharField(max_length=255, null=True, blank=True)
    wednesday = models.CharField(max_length=255, null=True, blank=True)
    thursday = models.CharField(max_length=255, null=True, blank=True)
    friday = models.CharField(max_length=255, null=True, blank=True)
    saturday = models.CharField(max_length=255, null=True, blank=True)
    sunday = models.CharField(max_length=255, null=True, blank=True)

    def __str__(self):
        return f"ToastWeeklySchedule for {self.restaurant.general_name}"


class ToastJoinedOpeningHours(models.Model):
    integration = models.ForeignKey("integrations.Integration", on_delete=models.CASCADE)
    restaurant = models.ForeignKey(ToastGeneralLocation, on_delete=models.CASCADE)
    monday_start_time = models.TimeField(null=True, blank=True)
    monday_end_time = models.TimeField(null=True, blank=True)
    monday_overnight = models.BooleanField(default=False)
    monday_related_day_schedule = models.CharField(max_length=255, null=True, blank=True)
    # Additional fields for Tuesday through Sunday would follow a similar pattern.
    
    def __str__(self):
        return f"ToastJoinedOpeningHours for {self.restaurant.general_name}"
