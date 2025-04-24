from django.db import models


class XeroJournalLines(models.Model):
    journal_line_id = models.CharField(max_length=50, primary_key=True)
    journal_id = models.CharField(max_length=50, blank=True, null=True, db_index=True)
    journal_number = models.CharField(max_length=50, blank=True, null=True)
    tenant_id = models.CharField(max_length=50, blank=True, null=True, db_index=True)
    reference = models.TextField(blank=True, null=True)
    source_id = models.CharField(max_length=50, blank=True, null=True)
    source_type = models.CharField(max_length=50, blank=True, null=True)
    account_id = models.CharField(max_length=50, blank=True, null=True)
    account_code = models.CharField(max_length=50, blank=True, null=True)
    account_type = models.CharField(max_length=50, blank=True, null=True)
    account_name = models.CharField(max_length=255, blank=True, null=True)
    description = models.TextField(blank=True, null=True)
    net_amount = models.DecimalField(max_digits=19, decimal_places=2, blank=True, null=True)
    gross_amount = models.DecimalField(max_digits=19, decimal_places=2, blank=True, null=True)
    tax_amount = models.DecimalField(max_digits=19, decimal_places=2, blank=True, null=True)
    journal_date = models.DateField(blank=True, null=True, db_index=True)
    created_date_utc = models.DateTimeField(blank=True, null=True)
    ingestion_timestamp = models.DateTimeField(blank=True, null=True)
    source_system = models.CharField(max_length=50, blank=True, null=True)
    tracking_category_name = models.CharField(max_length=255, blank=True, null=True)
    tracking_category_option = models.CharField(max_length=255, blank=True, null=True)

    class Meta:
        verbose_name = "Xero Journal Line"
        indexes = [
            models.Index(fields=['tenant_id']),
            models.Index(fields=['journal_line_id']),
            models.Index(fields=['journal_date']),
        ]

    def __str__(self):
        return f"{self.journal_line_id}"


class XeroJournalLineTrackingCategories(models.Model):
    tenant_id = models.CharField(max_length=50, null=True, blank=True, db_index=True)
    line_item_id = models.CharField(max_length=50, null=True, blank=True, db_index=True)
    journal_line_id = models.CharField(max_length=50, null=True, blank=True, db_index=True)
    tracking_category_id = models.CharField(max_length=50, blank=True, null=True, db_index=True)
    tracking_option_id = models.CharField(max_length=50, blank=True, null=True)
    name = models.CharField(max_length=255, blank=True, null=True)
    option = models.CharField(max_length=255, blank=True, null=True)
    ingestion_timestamp = models.DateTimeField(blank=True, null=True)
    source_system = models.CharField(max_length=50, blank=True, null=True)

    def __str__(self):
        return f"TrackingCat {self.tracking_category_id} / {self.tracking_option_id}"

    class Meta:
        verbose_name = "Xero Journal Line Tracking Category"
        indexes = [
            models.Index(fields=['tenant_id']),
            models.Index(fields=['journal_line_id']),
            models.Index(fields=['line_item_id']),
            models.Index(fields=['tracking_category_id']),
        ]


class XeroInvoiceLineItems(models.Model):
    invoice_id = models.CharField(max_length=255, null=True, blank=True, db_index=True)
    invoice_number = models.CharField(max_length=255, blank=True, null=True)
    unit_amount = models.DecimalField(max_digits=19, decimal_places=2, blank=True, null=True)
    quantity = models.DecimalField(max_digits=19, decimal_places=2, blank=True, null=True)
    account_code = models.CharField(max_length=255, blank=True, null=True)
    line_item_id = models.CharField(max_length=255, db_index=True)
    tenant_id = models.CharField(max_length=255, db_index=True)
    account_id = models.CharField(max_length=255, blank=True, null=True)
    line_amount = models.DecimalField(max_digits=19, decimal_places=2, blank=True, null=True)
    tax_type = models.CharField(max_length=255, blank=True, null=True)
    reference = models.CharField(max_length=255, blank=True, null=True)
    type = models.CharField(max_length=255, blank=True, null=True)   
    url = models.TextField(blank=True, null=True)
    amount_due = models.DecimalField(max_digits=19, decimal_places=2, blank=True, null=True)
    amount_paid = models.DecimalField(max_digits=19, decimal_places=2, blank=True, null=True)
    amount_credited = models.DecimalField(max_digits=19, decimal_places=2, blank=True, null=True)
    currency_rate = models.DecimalField(max_digits=19, decimal_places=2, blank=True, null=True)
    is_discounted = models.BooleanField(default=False)
    has_attachments = models.BooleanField(default=False)
    has_errors = models.BooleanField(default=False)
    contact_id = models.CharField(max_length=255, blank=True, null=True)
    contact_name = models.CharField(max_length=255, blank=True, null=True)
    status = models.CharField(max_length=255, blank=True, null=True)
    line_amount_types = models.CharField(max_length=255, blank=True, null=True)
    sub_total = models.DecimalField(max_digits=19, decimal_places=2, blank=True, null=True)
    tax_amount = models.DecimalField(max_digits=19, decimal_places=2, blank=True, null=True)
    total = models.DecimalField(max_digits=19, decimal_places=2, blank=True, null=True)
    currency_code = models.CharField(max_length=255, blank=True, null=True)
    description = models.TextField(blank=True, null=True)
    date = models.DateField(blank=True, null=True)
    due_date = models.DateField(blank=True, null=True)
    updated_date_utc = models.DateTimeField(blank=True, null=True)
    fully_paid_on_date = models.DateTimeField(blank=True, null=True)
    ingestion_timestamp = models.DateTimeField(blank=True, null=True)
    source_system = models.CharField(max_length=255, blank=True, null=True)

    class Meta:
        verbose_name = "Xero Invoice Line Item"
        indexes = [
            models.Index(fields=['tenant_id']),
            models.Index(fields=['line_item_id']),
            models.Index(fields=['invoice_id']),
        ]

    def __str__(self):
        return f"{self.invoice_id} - {self.line_item_id}"

