from django.db import models
from django.db.models import JSONField


class ToastOrder(models.Model):
    integration = models.ForeignKey("integrations.Integration", on_delete=models.CASCADE, related_name="toast_orders")
    tenant_id = models.IntegerField(db_index=True)
    order_guid = models.CharField(max_length=255, unique=True, db_index=True)
    payload = JSONField(help_text="Raw order data from Toast")
    order_net_sales = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    import_id = models.IntegerField(help_text="Reference to the integration ID")
    ws_import_date = models.DateTimeField(auto_now_add=True, db_index=True)
    created_date = models.DateTimeField(null=True, blank=True, db_index=True)
    closed_date = models.DateTimeField(null=True, blank=True, db_index=True)
    modified_date = models.DateTimeField(null=True, blank=True, db_index=True)

    def __str__(self):
        return f"ToastOrder {self.order_guid}"

    class Meta:
        indexes = [
            models.Index(fields=["tenant_id"]),
            models.Index(fields=["integration"]),
            models.Index(fields=["ws_import_date"]),
            models.Index(fields=["created_date"]),
            models.Index(fields=["closed_date"]),
            models.Index(fields=["modified_date"]),
        ]


class ToastCheck(models.Model):
    order = models.ForeignKey(ToastOrder, on_delete=models.CASCADE, related_name="checks")
    tenant_id = models.IntegerField(db_index=True)
    check_guid = models.CharField(max_length=255, db_index=True)
    display_number = models.CharField(max_length=50, null=True, blank=True)
    net_sales = models.DecimalField(max_digits=10, decimal_places=2, default=0)
    service_charge_total = models.DecimalField(max_digits=10, decimal_places=2, default=0)
    discount_total = models.DecimalField(max_digits=10, decimal_places=2, default=0)
    opened_date = models.DateTimeField(null=True, blank=True, db_index=True)
    closed_date = models.DateTimeField(null=True, blank=True, db_index=True)

    def __str__(self):
        return f"ToastCheck {self.check_guid}"

    class Meta:
        indexes = [
            models.Index(fields=["tenant_id"]),
            models.Index(fields=["order"]),
            models.Index(fields=["check_guid"]),
            models.Index(fields=["opened_date"]),
            models.Index(fields=["closed_date"]),
        ]


class ToastSelection(models.Model):
    toast_check = models.ForeignKey(ToastCheck, on_delete=models.CASCADE, related_name="selections")
    tenant_id = models.IntegerField(db_index=True)
    selection_guid = models.CharField(max_length=255, db_index=True)
    display_name = models.CharField(max_length=255)
    pre_discount_price = models.DecimalField(max_digits=10, decimal_places=2, default=0)
    discount_total = models.DecimalField(max_digits=10, decimal_places=2, default=0)
    net_sales = models.DecimalField(max_digits=10, decimal_places=2, default=0)
    quantity = models.DecimalField(max_digits=10, decimal_places=2, default=1)
    voided = models.BooleanField(default=False, db_index=True)

    def __str__(self):
        return f"ToastSelection {self.selection_guid}"

    class Meta:
        indexes = [
            models.Index(fields=["tenant_id"]),
            models.Index(fields=["toast_check"]),
            models.Index(fields=["selection_guid"]),
            models.Index(fields=["voided"]),
        ]


class ToastGeneralLocation(models.Model):
    tenant_id = models.IntegerField(db_index=True)
    guid = models.CharField(max_length=255, unique=True, db_index=True)
    general_name = models.CharField(max_length=255, db_index=True)
    location_name = models.CharField(max_length=255, null=True, blank=True)
    location_code = models.CharField(max_length=50, null=True, blank=True)
    description = models.TextField(null=True, blank=True)
    timezone = models.CharField(max_length=50, null=True, blank=True)
    closeout_hour = models.TimeField(null=True, blank=True)
    management_group_guid = models.CharField(max_length=255, null=True, blank=True, db_index=True)
    currency_code = models.CharField(max_length=10, null=True, blank=True)
    first_business_date = models.DateField(null=True, blank=True)
    archived = models.BooleanField(default=False, db_index=True)
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

    class Meta:
        indexes = [
            models.Index(fields=["tenant_id"]),
            models.Index(fields=["general_name"]),
            models.Index(fields=["archived"]),
            models.Index(fields=["management_group_guid"]),
        ]


class ToastDaySchedule(models.Model):
    integration = models.ForeignKey("integrations.Integration", on_delete=models.CASCADE)
    tenant_id = models.IntegerField(db_index=True)
    restaurant = models.ForeignKey(ToastGeneralLocation, on_delete=models.CASCADE)
    guid = models.CharField(max_length=255, unique=True, db_index=True)
    property_name = models.CharField(max_length=255, db_index=True)
    open_time = models.TimeField(null=True, blank=True)
    close_time = models.TimeField(null=True, blank=True)
    schedule_name = models.CharField(max_length=255, null=True, blank=True)

    def __str__(self):
        return f"ToastDaySchedule {self.guid}"

    class Meta:
        indexes = [
            models.Index(fields=["tenant_id"]),
            models.Index(fields=["integration"]),
            models.Index(fields=["restaurant"]),
            models.Index(fields=["property_name"]),
        ]


class ToastWeeklySchedule(models.Model):
    integration = models.ForeignKey("integrations.Integration", on_delete=models.CASCADE)
    tenant_id = models.IntegerField(db_index=True)
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

    class Meta:
        indexes = [
            models.Index(fields=["tenant_id"]),
            models.Index(fields=["integration"]),
            models.Index(fields=["restaurant"]),
        ]


class ToastJoinedOpeningHours(models.Model):
    integration = models.ForeignKey("integrations.Integration", on_delete=models.CASCADE)
    tenant_id = models.IntegerField(db_index=True)
    restaurant = models.ForeignKey(ToastGeneralLocation, on_delete=models.CASCADE)
    monday_start_time = models.TimeField(null=True, blank=True)
    monday_end_time = models.TimeField(null=True, blank=True)
    monday_overnight = models.BooleanField(default=False)
    monday_related_day_schedule = models.CharField(max_length=255, null=True, blank=True)
    tuesday_start_time = models.TimeField(null=True, blank=True)
    tuesday_end_time = models.TimeField(null=True, blank=True)
    tuesday_overnight = models.BooleanField(default=False)
    tuesday_related_day_schedule = models.CharField(max_length=255, null=True, blank=True)
    wednesday_start_time = models.TimeField(null=True, blank=True)
    wednesday_end_time = models.TimeField(null=True, blank=True)
    wednesday_overnight = models.BooleanField(default=False)
    wednesday_related_day_schedule = models.CharField(max_length=255, null=True, blank=True)
    thursday_start_time = models.TimeField(null=True, blank=True)
    thursday_end_time = models.TimeField(null=True, blank=True)
    thursday_overnight = models.BooleanField(default=False)
    thursday_related_day_schedule = models.CharField(max_length=255, null=True, blank=True)
    friday_start_time = models.TimeField(null=True, blank=True)
    friday_end_time = models.TimeField(null=True, blank=True)
    friday_overnight = models.BooleanField(default=False)
    friday_related_day_schedule = models.CharField(max_length=255, null=True, blank=True)
    saturday_start_time = models.TimeField(null=True, blank=True)
    saturday_end_time = models.TimeField(null=True, blank=True)
    saturday_overnight = models.BooleanField(default=False)
    saturday_related_day_schedule = models.CharField(max_length=255, null=True, blank=True)
    sunday_start_time = models.TimeField(null=True, blank=True)
    sunday_end_time = models.TimeField(null=True, blank=True)
    sunday_overnight = models.BooleanField(default=False)
    sunday_related_day_schedule = models.CharField(max_length=255, null=True, blank=True)
        
    def __str__(self):
        return f"ToastJoinedOpeningHours for {self.restaurant.general_name}"

    class Meta:
        indexes = [
            models.Index(fields=["tenant_id"]),
            models.Index(fields=["integration"]),
            models.Index(fields=["restaurant"]),
            models.Index(fields=["monday_start_time", "monday_end_time"]),
            models.Index(fields=["tuesday_start_time", "tuesday_end_time"]),
            models.Index(fields=["wednesday_start_time", "wednesday_end_time"]),
            models.Index(fields=["thursday_start_time", "thursday_end_time"]),
            models.Index(fields=["friday_start_time", "friday_end_time"]),
            models.Index(fields=["saturday_start_time", "saturday_end_time"]),
            models.Index(fields=["sunday_start_time", "sunday_end_time"]),
        ]
