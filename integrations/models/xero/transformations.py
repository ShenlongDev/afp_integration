from django.db import models

class XeroAccounts(models.Model):
    account_id = models.CharField(max_length=50)
    tenant_id = models.CharField(max_length=50)
    code = models.CharField(max_length=50, blank=True, null=True)
    name = models.CharField(max_length=255, blank=True, null=True)
    status = models.CharField(max_length=50, blank=True, null=True)
    type = models.CharField(max_length=50, blank=True, null=True)
    tax_type = models.CharField(max_length=50, blank=True, null=True)
    # "class" is a reserved keyword in Python, so let's rename to account_class:
    account_class = models.CharField(max_length=50, blank=True, null=True)
    bank_account_type = models.CharField(max_length=50, blank=True, null=True)
    currency_code = models.CharField(max_length=10, blank=True, null=True)
    reporting_code = models.CharField(max_length=50, blank=True, null=True)
    reporting_code_name = models.CharField(max_length=100, blank=True, null=True)
    description = models.TextField(blank=True, null=True)
    system_account = models.CharField(max_length=50, blank=True, null=True)
    updated_date_utc = models.DateTimeField(blank=True, null=True)
    ingestion_timestamp = models.DateTimeField(blank=True, null=True)
    source_system = models.CharField(max_length=50, blank=True, null=True)

    def __str__(self):
        return f"{self.account_id} - {self.name}"

    class Meta:
        verbose_name = "Xero Account"


class XeroBankTransactionLineItems(models.Model):
    tenant_id = models.CharField(max_length=50)
    bank_transaction_id = models.CharField(max_length=50)
    line_item_id = models.CharField(max_length=50)
    bank_account_id = models.CharField(max_length=50, blank=True, null=True)
    bank_account_code = models.CharField(max_length=50, blank=True, null=True)
    bank_account_name = models.CharField(max_length=255, blank=True, null=True)
    type = models.CharField(max_length=50, blank=True, null=True)
    reference = models.CharField(max_length=255, blank=True, null=True)
    is_reconcilied = models.BooleanField(default=False)
    has_attachments = models.BooleanField(default=False)
    contact_id = models.CharField(max_length=50, blank=True, null=True)
    contact_name = models.CharField(max_length=255, blank=True, null=True)
    status = models.CharField(max_length=50, blank=True, null=True)
    line_amount_types = models.CharField(max_length=50, blank=True, null=True)
    line_item_description = models.TextField(blank=True, null=True)
    line_item_unit_amount = models.DecimalField(max_digits=19, decimal_places=2, blank=True, null=True)
    line_item_tax_type = models.CharField(max_length=50, blank=True, null=True)
    line_item_tax_amount = models.DecimalField(max_digits=19, decimal_places=2, blank=True, null=True)
    line_item_line_amount = models.DecimalField(max_digits=19, decimal_places=2, blank=True, null=True)
    line_item_quantity = models.DecimalField(max_digits=19, decimal_places=2, blank=True, null=True)
    line_item_account_code = models.CharField(max_length=50, blank=True, null=True)
    line_item_account_id = models.CharField(max_length=50, blank=True, null=True)
    sub_total = models.DecimalField(max_digits=19, decimal_places=2, blank=True, null=True)
    total_tax = models.DecimalField(max_digits=19, decimal_places=2, blank=True, null=True)
    total = models.DecimalField(max_digits=19, decimal_places=2, blank=True, null=True)
    currency_code = models.CharField(max_length=10, blank=True, null=True)
    date = models.DateField(blank=True, null=True)
    updated_date_utc = models.DateTimeField(blank=True, null=True)
    ingestion_timestamp = models.DateTimeField(blank=True, null=True)
    source_system = models.CharField(max_length=50, blank=True, null=True)

    def __str__(self):
        return f"{self.bank_transaction_id} - {self.line_item_id}"

    class Meta:
        verbose_name = "Xero Bank Transaction Line Item"


class XeroJournalLines(models.Model):
    journal_line_id = models.CharField(max_length=50, primary_key=True)
    journal_id = models.CharField(max_length=50, blank=True, null=True)
    journal_number = models.CharField(max_length=50, blank=True, null=True)
    tenant_id = models.CharField(max_length=50, blank=True, null=True)
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
    journal_date = models.DateField(blank=True, null=True)
    created_date_utc = models.DateTimeField(blank=True, null=True)
    ingestion_timestamp = models.DateTimeField(blank=True, null=True)
    source_system = models.CharField(max_length=50, blank=True, null=True)
    
    tracking_category_name = models.CharField(max_length=255, blank=True, null=True)
    tracking_category_option = models.CharField(max_length=255, blank=True, null=True)

    def __str__(self):
        return f"{self.journal_line_id}"

    class Meta:
        verbose_name = "Xero Journal Line"


class XeroJournalLineTrackingCategories(models.Model):
    tenant_id = models.CharField(max_length=50, null=True, blank=True)
    line_item_id = models.CharField(max_length=50, null=True, blank=True)
    journal_line_id = models.CharField(max_length=50, null=True, blank=True)
    tracking_category_id = models.CharField(max_length=50, blank=True, null=True)

    tracking_option_id = models.CharField(max_length=50, blank=True, null=True)
    name = models.CharField(max_length=255, blank=True, null=True)
    option = models.CharField(max_length=255, blank=True, null=True)
    ingestion_timestamp = models.DateTimeField(blank=True, null=True)
    source_system = models.CharField(max_length=50, blank=True, null=True)

    def __str__(self):
        return f"TrackingCat {self.tracking_category_id} / {self.tracking_option_id}"

    class Meta:
        verbose_name = "Xero Journal Line Tracking Category"


class XeroInvoiceLineItems(models.Model):
    invoice_id = models.CharField(max_length=50, null=True, blank=True)
    invoice_number = models.CharField(max_length=50, blank=True, null=True)
    unit_amount = models.DecimalField(max_digits=19, decimal_places=2, blank=True, null=True)
    quantity = models.DecimalField(max_digits=19, decimal_places=2, blank=True, null=True)
    account_code = models.CharField(max_length=50, blank=True, null=True)
    line_item_id = models.CharField(max_length=50)
    tenant_id = models.CharField(max_length=50)
    account_id = models.CharField(max_length=50, blank=True, null=True)
    line_amount = models.DecimalField(max_digits=19, decimal_places=2, blank=True, null=True)
    tax_type = models.CharField(max_length=50, blank=True, null=True)
    reference = models.CharField(max_length=255, blank=True, null=True)
    url = models.TextField(blank=True, null=True)
    amount_due = models.DecimalField(max_digits=19, decimal_places=2, blank=True, null=True)
    amount_paid = models.DecimalField(max_digits=19, decimal_places=2, blank=True, null=True)
    amount_credited = models.DecimalField(max_digits=19, decimal_places=2, blank=True, null=True)
    currency_rate = models.DecimalField(max_digits=19, decimal_places=2, blank=True, null=True)
    is_discounted = models.BooleanField(default=False)
    has_attachments = models.BooleanField(default=False)
    has_errors = models.BooleanField(default=False)
    contact_id = models.CharField(max_length=50, blank=True, null=True)
    contact_name = models.CharField(max_length=255, blank=True, null=True)
    status = models.CharField(max_length=50, blank=True, null=True)
    line_amount_types = models.CharField(max_length=50, blank=True, null=True)
    sub_total = models.DecimalField(max_digits=19, decimal_places=2, blank=True, null=True)
    tax_amount = models.DecimalField(max_digits=19, decimal_places=2, blank=True, null=True)
    total = models.DecimalField(max_digits=19, decimal_places=2, blank=True, null=True)
    currency_code = models.CharField(max_length=10, blank=True, null=True)
    description = models.TextField(blank=True, null=True)
    date = models.DateField(blank=True, null=True)
    due_date = models.DateField(blank=True, null=True)
    updated_date_utc = models.DateTimeField(blank=True, null=True)
    fully_paid_on_date = models.DateTimeField(blank=True, null=True)
    ingestion_timestamp = models.DateTimeField(blank=True, null=True)
    source_system = models.CharField(max_length=50, blank=True, null=True)

    def __str__(self):
        return f"{self.invoice_id} - {self.line_item_id}"

    class Meta:
        verbose_name = "Xero Invoice Line Item"


class XeroBudgetPeriodBalances(models.Model):
    tenant_id = models.CharField(max_length=50)
    tenant_name = models.CharField(max_length=255, blank=True, null=True)
    budget_id = models.CharField(max_length=50)
    budget_name = models.CharField(max_length=255, blank=True, null=True)
    budget_status = models.CharField(max_length=50, blank=True, null=True)
    budget_type = models.CharField(max_length=50, blank=True, null=True)
    account_id = models.CharField(max_length=50, blank=True, null=True)
    account_code = models.CharField(max_length=50, blank=True, null=True)
    account_name = models.CharField(max_length=255, blank=True, null=True)
    account_status = models.CharField(max_length=50, blank=True, null=True)
    account_tax_type = models.CharField(max_length=50, blank=True, null=True)
    account_class = models.CharField(max_length=50, blank=True, null=True)
    account_reporting_code = models.CharField(max_length=50, blank=True, null=True)
    account_reporting_code_name = models.CharField(max_length=100, blank=True, null=True)
    tracking_category_name = models.CharField(max_length=255, blank=True, null=True)
    tracking_category_option = models.CharField(max_length=255, blank=True, null=True)
    period = models.DateField(blank=True, null=True)
    amount = models.DecimalField(max_digits=19, decimal_places=2, blank=True, null=True)
    notes = models.TextField(blank=True, null=True)
    updated_date_utc = models.DateTimeField(blank=True, null=True)
    ingestion_timestamp = models.DateTimeField(blank=True, null=True)
    source_system = models.CharField(max_length=50, blank=True, null=True)


    def __str__(self):
        return f"{self.budget_id} - {self.period}"


# class XeroGeneralLedger(models.Model):
#     tenant_id = models.CharField(max_length=50)
#     tenant_name = models.CharField(max_length=255, blank=True, null=True)
#     journal_id = models.CharField(max_length=50)
#     journal_number = models.CharField(max_length=50, blank=True, null=True)
#     journal_date = models.DateField(blank=True, null=True)
#     created_date = models.DateTimeField(blank=True, null=True)
#     journal_line_id = models.CharField(max_length=50)
#     journal_reference = models.TextField(blank=True, null=True)
#     source_id = models.CharField(max_length=50, blank=True, null=True)
#     source_type = models.CharField(max_length=50, blank=True, null=True)
#     tracking_category_name = models.CharField(max_length=255, blank=True, null=True)
#     tracking_category_option = models.CharField(max_length=255, blank=True, null=True)
#     account_id = models.CharField(max_length=50, blank=True, null=True)
#     account_code = models.CharField(max_length=50, blank=True, null=True)
#     account_type = models.CharField(max_length=50, blank=True, null=True)
#     account_name = models.CharField(max_length=255, blank=True, null=True)
#     account_status = models.CharField(max_length=50, blank=True, null=True)
#     account_tax_type = models.CharField(max_length=50, blank=True, null=True)
#     account_class = models.CharField(max_length=50, blank=True, null=True)
#     account_currency_code = models.CharField(max_length=10, blank=True, null=True)
#     account_reporting_code = models.CharField(max_length=50, blank=True, null=True)
#     account_reporting_code_name = models.CharField(max_length=100, blank=True, null=True)
#     account_description = models.TextField(blank=True, null=True)
#     account_system_account = models.CharField(max_length=50, blank=True, null=True)
#     statement = models.CharField(max_length=2, blank=True, null=True)  # e.g. 'PL' or 'BS'
#     bank_account_type = models.CharField(max_length=50, blank=True, null=True)
#     journal_line_description = models.TextField(blank=True, null=True)
#     net_amount = models.DecimalField(max_digits=19, decimal_places=2, blank=True, null=True)
#     gross_amount = models.DecimalField(max_digits=19, decimal_places=2, blank=True, null=True)
#     tax_amount = models.DecimalField(max_digits=19, decimal_places=2, blank=True, null=True)
#     invoice_number = models.CharField(max_length=50, blank=True, null=True)
#     invoice_url = models.TextField(blank=True, null=True)
#     invoice_description = models.TextField(blank=True, null=True)
#     contact_id = models.CharField(max_length=50, blank=True, null=True)
#     contact_name = models.CharField(max_length=255, blank=True, null=True)
#     amount_due = models.DecimalField(max_digits=19, decimal_places=2, blank=True, null=True)
#     amount_paid = models.DecimalField(max_digits=19, decimal_places=2, blank=True, null=True)
#     invoice_date = models.DateField(blank=True, null=True)
#     invoice_due_date = models.DateField(blank=True, null=True)
#     ingestion_timestamp = models.DateTimeField(blank=True, null=True)
#     source_system = models.CharField(max_length=50, blank=True, null=True)
#     duplicate_check = models.IntegerField(blank=True, null=True)

#     def __str__(self):
#         return f"GL line {self.journal_line_id} (Tenant {self.tenant_id})"

