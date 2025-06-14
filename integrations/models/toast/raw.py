from django.db import models
from django.db.models import JSONField
from decimal import Decimal
from core.models import Site

class ToastOrder(models.Model):
    order_guid = models.CharField(max_length=255, primary_key=True)
    integration = models.ForeignKey("integrations.Integration", on_delete=models.CASCADE, related_name="toast_orders")
    tenant_id = models.IntegerField(db_index=True)
    site = models.ForeignKey(Site, on_delete=models.CASCADE, related_name="toast_orders", null=True, blank=True)
    restaurant_guid = models.CharField(max_length=255, null=True, blank=True)
    payload = JSONField(help_text="Raw order data from Toast")
    order_net_sales = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal('0.00'))
    import_id = models.IntegerField(help_text="Reference to the integration ID")
    ws_import_date = models.DateTimeField(auto_now_add=True, db_index=True)
    created_date = models.DateTimeField(null=True, blank=True, db_index=True)
    closed_date = models.DateTimeField(null=True, blank=True, db_index=True)
    modified_date = models.DateTimeField(null=True, blank=True, db_index=True)
    updated_date = models.DateTimeField(null=True, blank=True)
    toast_sales = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal('0.00'))
    total_amount = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal('0.00'))
    total_refunds = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal('0.00'))
    refund_business_date = models.IntegerField(null=True, blank=True)
    tip = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal('0.00'))
    service_charges = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal('0.00'))
    total_discount_amount = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal('0.00'))
    discount_count = models.IntegerField(default=0)
    external_id = models.CharField(max_length=255, null=True, blank=True)
    entity_type = models.CharField(max_length=100, null=True, blank=True)
    revenue_center_guid = models.CharField(max_length=255, null=True, blank=True)
    server_guid = models.CharField(max_length=255, null=True, blank=True)
    created_in_test_mode = models.BooleanField(null=True, blank=True)
    display_number = models.CharField(max_length=100, null=True, blank=True)
    last_modified_device_id = models.CharField(max_length=100, null=True, blank=True)
    source = models.CharField(max_length=100, null=True, blank=True)
    void_date = models.DateTimeField(null=True, blank=True)
    duration = models.IntegerField(null=True, blank=True)
    business_date = models.IntegerField(null=True, blank=True)
    paid_date = models.DateTimeField(null=True, blank=True)
    restaurant_service_guid = models.CharField(max_length=255, null=True, blank=True)
    excess_food = models.BooleanField(null=True, blank=True)
    voided = models.BooleanField(null=True, blank=True)
    deleted = models.BooleanField(null=True, blank=True)
    estimated_fulfillment_date = models.DateTimeField(null=True, blank=True)
    table_guid = models.CharField(max_length=255, null=True, blank=True)
    required_prep_time = models.CharField(max_length=50, null=True, blank=True)
    approval_status = models.CharField(max_length=50, null=True, blank=True)
    delivery_info = JSONField(null=True, blank=True)
    service_area_guid = models.CharField(max_length=255, null=True, blank=True)
    curbside_pickup_info = JSONField(null=True, blank=True)
    number_of_guests = models.IntegerField(null=True, blank=True)
    dining_option = models.CharField(max_length=100, null=True, blank=True)
    applied_packaging_info = JSONField(null=True, blank=True)
    opened_date = models.DateTimeField(null=True, blank=True)
    void_business_date = models.CharField(max_length=50, null=True, blank=True)
    payments = JSONField(null=True, blank=True, help_text="Raw payment data from Toast")

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
            models.Index(fields=["payments"]),
            models.Index(fields=["site"]),
        ]




class ToastRefund(models.Model):
    restaurant_guid = models.CharField(max_length=255, null=True, blank=True)
    order_guid = models.CharField(max_length=255, db_index=True)
    check_guid = models.CharField(max_length=255, db_index=True)
    payment_guid = models.CharField(max_length=255, db_index=True)
    refund_guid = models.CharField(max_length=255, db_index=True)
    refund_amount = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal('0.00'))
    tip_refund_amount = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal('0.00'))
    refund_date = models.DateTimeField(null=True, blank=True, db_index=True)
    refund_business_date = models.IntegerField(null=True, blank=True)
    refund_transaction_guid = models.CharField(null=True, blank=True)
    refund_transaction_entity_type = models.CharField(max_length=100, null=True, blank=True)
    tenant_id = models.IntegerField(db_index=True)


class ToastCheck(models.Model):
    order = models.ForeignKey(ToastOrder, on_delete=models.CASCADE, related_name="checks")
    tenant_id = models.IntegerField(db_index=True)
    check_guid = models.CharField(max_length=255, db_index=True)
    display_number = models.CharField(max_length=100, null=True, blank=True)
    net_sales = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal('0.00'))
    service_charge_total = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal('0.00'))
    discount_total = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal('0.00'))
    check_refund = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal('0.00'))
    opened_date = models.DateTimeField(null=True, blank=True, db_index=True)
    closed_date = models.DateTimeField(null=True, blank=True, db_index=True)
    # Additional check-level fields:
    external_id = models.CharField(max_length=255, null=True, blank=True)
    entity_type = models.CharField(max_length=100, null=True, blank=True)
    last_modified_device_id = models.CharField(max_length=100, null=True, blank=True)
    void_date = models.DateTimeField(null=True, blank=True)
    duration = models.IntegerField(null=True, blank=True)
    opened_by = models.CharField(max_length=255, null=True, blank=True)
    paid_date = models.DateTimeField(null=True, blank=True)
    applied_loyalty_info = JSONField(null=True, blank=True)
    voided = models.BooleanField(null=True, blank=True)
    payment_status = models.CharField(max_length=100, null=True, blank=True)
    amount = models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    tab_name = models.CharField(max_length=100, null=True, blank=True)
    tax_exempt = models.BooleanField(null=True, blank=True)
    tax_exemption_account = models.CharField(max_length=100, null=True, blank=True)
    total_amount = models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)

    def __str__(self):
        return f"ToastCheck {self.check_guid} (Order: {self.order.order_guid})"

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
    order_guid = models.CharField(max_length=255, db_index=True, null=True, blank=True)
    selection_guid = models.CharField(max_length=255, db_index=True)
    display_name = models.CharField(max_length=255)
    pre_discount_price = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal('0.00'))
    discount_total = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal('0.00'))
    net_sales = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal('0.00'))
    quantity = models.DecimalField(max_digits=10, decimal_places=2, default=1)
    voided = models.BooleanField(default=False, db_index=True)
    business_date = models.IntegerField(null=True, blank=True)
    # Additional selection-level fields:
    external_id = models.CharField(max_length=255, null=True, blank=True)
    entity_type = models.CharField(max_length=100, null=True, blank=True)
    deferred = models.BooleanField(null=True, blank=True)
    void_reason = JSONField(null=True, blank=True)
    option_group = JSONField(null=True, blank=True)
    modifiers = JSONField(null=True, blank=True)
    seat_number = models.IntegerField(null=True, blank=True)
    fulfillment_status = models.CharField(max_length=100, null=True, blank=True)
    option_group_pricing_mode = models.CharField(max_length=100, null=True, blank=True)
    gift_card_selection_info = JSONField(null=True, blank=True)
    sales_category_guid = models.CharField(max_length=255, null=True, blank=True)
    split_origin = models.CharField(max_length=255, null=True, blank=True)
    selection_type = models.CharField(max_length=100, null=True, blank=True)
    price = models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    applied_taxes = JSONField(null=True, blank=True)
    stored_value_transaction_id = models.CharField(max_length=255, null=True, blank=True)
    item_group = JSONField(null=True, blank=True)
    item = JSONField(null=True, blank=True)
    tax_inclusion = models.CharField(max_length=50, null=True, blank=True)
    receipt_line_price = models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    unit_of_measure = models.CharField(max_length=50, null=True, blank=True)
    refund_details = JSONField(null=True, blank=True)
    toast_gift_card = JSONField(null=True, blank=True)
    tax = models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    dining_option = models.CharField(max_length=100, null=True, blank=True)
    void_business_date = models.CharField(max_length=50, null=True, blank=True)
    created_date = models.DateTimeField(null=True, blank=True)
    pre_modifier = JSONField(null=True, blank=True)
    modified_date = models.DateTimeField(null=True, blank=True)

    def __str__(self):
        return f"ToastSelection {self.selection_guid} (Check: {self.toast_check.check_guid})"

    class Meta:
        indexes = [
            models.Index(fields=["tenant_id"]),
            models.Index(fields=["toast_check"]),
            models.Index(fields=["selection_guid"]),
            models.Index(fields=["voided"]),
            models.Index(fields=["business_date"]),
        ]
        unique_together = ('tenant_id', 'selection_guid')

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
        unique_together = ('tenant_id', 'guid')

class ToastDaySchedule(models.Model):
    integration = models.ForeignKey("integrations.Integration", on_delete=models.CASCADE)
    tenant_id = models.IntegerField(db_index=True)
    restaurant = models.ForeignKey(Site, on_delete=models.CASCADE)
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
        unique_together = ('tenant_id', 'guid')


class ToastWeeklySchedule(models.Model):
    integration = models.ForeignKey("integrations.Integration", on_delete=models.CASCADE)
    tenant_id = models.IntegerField(db_index=True)
    restaurant = models.ForeignKey(Site, on_delete=models.CASCADE)
    monday = models.CharField(max_length=255, null=True, blank=True)
    tuesday = models.CharField(max_length=255, null=True, blank=True)
    wednesday = models.CharField(max_length=255, null=True, blank=True)
    thursday = models.CharField(max_length=255, null=True, blank=True)
    friday = models.CharField(max_length=255, null=True, blank=True)
    saturday = models.CharField(max_length=255, null=True, blank=True)
    sunday = models.CharField(max_length=255, null=True, blank=True)

    def __str__(self):
        return f"ToastWeeklySchedule for {self.restaurant.name}"

    class Meta:
        indexes = [
            models.Index(fields=["tenant_id"]),
            models.Index(fields=["integration"]),
            models.Index(fields=["restaurant"]),
        ]
        unique_together = ('tenant_id', 'restaurant')


class ToastJoinedOpeningHours(models.Model):
    integration = models.ForeignKey("integrations.Integration", on_delete=models.CASCADE)
    tenant_id = models.IntegerField(db_index=True)
    restaurant = models.ForeignKey(Site, on_delete=models.CASCADE)
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
        return f"ToastJoinedOpeningHours for {self.restaurant.name}"

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
        unique_together = ('tenant_id', 'restaurant')

class ToastRevenueCenter(models.Model):
    tenant_id = models.IntegerField(db_index=True)
    integration = models.ForeignKey("integrations.Integration", on_delete=models.CASCADE)
    restaurant_guid = models.CharField(max_length=255, db_index=True)
    revenue_center_guid = models.CharField(max_length=255, db_index=True)
    name = models.CharField(max_length=255, null=True, blank=True)
    description = models.TextField(null=True, blank=True)
    entity_type = models.CharField(max_length=100, null=True, blank=True)
    
    class Meta:
        unique_together = ('tenant_id', 'revenue_center_guid')
        indexes = [
            models.Index(fields=["tenant_id"]),
            models.Index(fields=["integration"]),
            models.Index(fields=["restaurant_guid"]),
            models.Index(fields=["revenue_center_guid"]),
        ]
        
    def __str__(self):
        return f"ToastRevenueCenter: {self.name} ({self.revenue_center_guid})"

class ToastRestaurantService(models.Model):
    tenant_id = models.IntegerField(db_index=True)
    integration = models.ForeignKey("integrations.Integration", on_delete=models.CASCADE)
    restaurant_guid = models.CharField(max_length=255, db_index=True)
    service_guid = models.CharField(max_length=255, db_index=True)
    name = models.CharField(max_length=255)
    entity_type = models.CharField(max_length=100, null=True, blank=True)
    
    class Meta:
        unique_together = ('tenant_id', 'service_guid')
        indexes = [
            models.Index(fields=["tenant_id"]),
            models.Index(fields=["integration"]),
            models.Index(fields=["restaurant_guid"]),
        ]
        
    def __str__(self):
        return f"ToastRestaurantService: {self.name} ({self.service_guid})"


class ToastSalesCategory(models.Model):
    tenant_id = models.IntegerField(db_index=True)
    integration = models.ForeignKey("integrations.Integration", on_delete=models.CASCADE)
    restaurant_guid = models.CharField(max_length=255, db_index=True)
    category_guid = models.CharField(max_length=255, db_index=True)
    name = models.CharField(max_length=255)
    entity_type = models.CharField(max_length=100, null=True, blank=True)
    
    class Meta:
        unique_together = ('tenant_id', 'category_guid')
        indexes = [
            models.Index(fields=["tenant_id"]),
            models.Index(fields=["integration"]),
            models.Index(fields=["restaurant_guid"]),
        ]
        
    def __str__(self):
        return f"ToastSalesCategory: {self.name} ({self.category_guid})"


class ToastDiningOption(models.Model):
    tenant_id = models.IntegerField(db_index=True)
    integration = models.ForeignKey("integrations.Integration", on_delete=models.CASCADE)
    restaurant_guid = models.CharField(max_length=255, db_index=True)
    option_guid = models.CharField(max_length=255, db_index=True)
    name = models.CharField(max_length=255)
    entity_type = models.CharField(max_length=100, null=True, blank=True)
    external_id = models.CharField(max_length=255, null=True, blank=True)
    behavior = models.CharField(max_length=50, null=True, blank=True)
    curbside = models.BooleanField(default=False)
    
    class Meta:
        unique_together = ('tenant_id', 'option_guid')
        indexes = [
            models.Index(fields=["tenant_id"]),
            models.Index(fields=["integration"]),
            models.Index(fields=["restaurant_guid"]),
        ]
        
    def __str__(self):
        return f"ToastDiningOption: {self.name} ({self.option_guid})"


class ToastServiceArea(models.Model):
    tenant_id = models.IntegerField(db_index=True)
    integration = models.ForeignKey("integrations.Integration", on_delete=models.CASCADE)
    restaurant_guid = models.CharField(max_length=255, db_index=True)
    area_guid = models.CharField(max_length=255, db_index=True)
    name = models.CharField(max_length=255)
    entity_type = models.CharField(max_length=100, null=True, blank=True)
    revenue_center_guid = models.CharField(max_length=255, null=True, blank=True, db_index=True)
    
    class Meta:
        unique_together = ('tenant_id', 'area_guid')
        indexes = [
            models.Index(fields=["tenant_id"]),
            models.Index(fields=["integration"]),
            models.Index(fields=["restaurant_guid"]),
            models.Index(fields=["revenue_center_guid"]),
        ]
        
    def __str__(self):
        return f"ToastServiceArea: {self.name} ({self.area_guid})"

class ToastPayment(models.Model):
    payment_guid = models.CharField(max_length=255, db_index=True)
    tenant_id = models.IntegerField(db_index=True)
    order_guid = models.CharField(max_length=255, db_index=True)
    restaurant_guid = models.CharField(max_length=255, db_index=True)
    check_guid = models.CharField(max_length=255, db_index=True)
    integration = models.ForeignKey("integrations.Integration", on_delete=models.CASCADE, related_name="toast_payments")
    
    type = models.CharField(max_length=50, null=True, blank=True)
    amount = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal('0.00'))
    tip_amount = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal('0.00'))
    card_type = models.CharField(max_length=50, null=True, blank=True)
    last4_digits = models.CharField(max_length=10, null=True, blank=True)
    paid_date = models.DateTimeField(null=True, blank=True, db_index=True)
    paid_business_date = models.IntegerField(null=True, blank=True)
    refund_status = models.CharField(max_length=50, null=True, blank=True)
    payment_status = models.CharField(max_length=50, null=True, blank=True)
    card_entry_mode = models.CharField(max_length=50, null=True, blank=True)
    
    server_guid = models.CharField(max_length=255, null=True, blank=True)
    created_device_id = models.CharField(max_length=100, null=True, blank=True)
    last_modified_device_id = models.CharField(max_length=100, null=True, blank=True)
    
    raw_payload = JSONField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        unique_together = (('tenant_id', 'payment_guid'),)
        indexes = [
            models.Index(fields=['tenant_id', 'order_guid']),
            models.Index(fields=['tenant_id', 'restaurant_guid']),
            models.Index(fields=['tenant_id', 'paid_business_date']),
            models.Index(fields=['tenant_id', 'payment_status']),
        ]
        
    def __str__(self):
        return f"Payment {self.payment_guid}"
