from django.db import models

class SquareOrders(models.Model):
    id = models.AutoField(primary_key=True)

    order_id = models.CharField(max_length=255, unique=True)
    customer_id = models.CharField(max_length=255, blank=True, null=True)
    location_id = models.CharField(max_length=255, blank=True, null=True)

    created_at = models.DateTimeField(blank=True, null=True)
    updated_at = models.DateTimeField(blank=True, null=True)
    closed_at = models.DateTimeField(blank=True, null=True)

    # raw_payload is a JSON (VARIANT). In Django, we might store as TextField or JSONField if available.
    raw_payload = models.JSONField(blank=True, null=True)

    ingestion_timestamp = models.DateTimeField(blank=True, null=True)
    source_system = models.CharField(max_length=255, blank=True, null=True)

    def __str__(self):
        return self.order_id


class XeroAccountsRaw(models.Model):
    id = models.AutoField(primary_key=True)

    tenant_id = models.CharField(max_length=255)
    account_id = models.CharField(max_length=255)
    name = models.CharField(max_length=255, blank=True, null=True)
    status = models.CharField(max_length=50, blank=True, null=True)
    type = models.CharField(max_length=50, blank=True, null=True)
    updated_date_utc = models.DateTimeField(blank=True, null=True)
    raw_payload = models.JSONField(blank=True, null=True)
    ingestion_timestamp = models.DateTimeField(blank=True, null=True)
    source_system = models.CharField(max_length=50, blank=True, null=True)

    class Meta:
        unique_together = (('tenant_id', 'account_id'), )

    def __str__(self):
        return f"{self.tenant_id} - {self.account_id}"


class XeroBankTransactionsRaw(models.Model):
    id = models.AutoField(primary_key=True)

    bank_transaction_id = models.CharField(max_length=255)
    tenant_id = models.CharField(max_length=255)
    type = models.CharField(max_length=50, blank=True, null=True)
    status = models.CharField(max_length=50, blank=True, null=True)
    date = models.DateTimeField(blank=True, null=True)
    updated_date_utc = models.DateTimeField(blank=True, null=True)
    raw_payload = models.JSONField(blank=True, null=True)
    ingestion_timestamp = models.DateTimeField(blank=True, null=True)
    source_system = models.CharField(max_length=50, blank=True, null=True)

    class Meta:
        unique_together = (('tenant_id', 'bank_transaction_id'), )

    def __str__(self):
        return f"{self.tenant_id} - {self.bank_transaction_id}"


class XeroBudgetPeriodBalancesRaw(models.Model):
    id = models.AutoField(primary_key=True)

    tenant_id = models.CharField(max_length=255)
    budget_id = models.CharField(max_length=255)
    account_id = models.CharField(max_length=255)
    account_code = models.CharField(max_length=255, blank=True, null=True)
    period = models.CharField(max_length=255)
    amount = models.DecimalField(max_digits=19, decimal_places=2, blank=True, null=True)
    notes = models.TextField(blank=True, null=True)
    updated_date_utc = models.DateTimeField(blank=True, null=True)
    ingestion_timestamp = models.DateTimeField(blank=True, null=True)
    source_system = models.CharField(max_length=50, blank=True, null=True)

    class Meta:
        unique_together = (('tenant_id', 'budget_id', 'account_id', 'period'), )

    def __str__(self):
        return f"{self.tenant_id} - {self.budget_id} - {self.account_id} - {self.period}"


class XeroBudgetsRaw(models.Model):
    id = models.AutoField(primary_key=True)

    budget_id = models.CharField(max_length=255)
    tenant_id = models.CharField(max_length=255)
    status = models.CharField(max_length=50)
    type = models.CharField(max_length=50)
    description = models.TextField(blank=True, null=True)
    updated_date_utc = models.DateTimeField(blank=True, null=True)
    raw_payload = models.JSONField(blank=True, null=True)
    ingestion_timestamp = models.DateTimeField(blank=True, null=True)
    source_system = models.CharField(max_length=50, blank=True, null=True)

    class Meta:
        unique_together = (('tenant_id', 'budget_id'), )

    def __str__(self):
        return f"{self.tenant_id} - {self.budget_id}"


class XeroConnectionsRaw(models.Model):
    id = models.AutoField(primary_key=True)

    tenant_id = models.CharField(max_length=255)
    user_id = models.CharField(max_length=255)
    tenant_name = models.CharField(max_length=255)
    created_at = models.DateTimeField(blank=True, null=True)
    updated_at = models.DateTimeField(blank=True, null=True)
    raw_payload = models.JSONField(blank=True, null=True)
    ingestion_timestamp = models.DateTimeField(blank=True, null=True)
    source_system = models.CharField(max_length=50, blank=True, null=True)

    class Meta:
        unique_together = (('tenant_id', 'user_id'), )

    def __str__(self):
        return f"{self.tenant_id} - {self.user_id}"


class XeroContactsRaw(models.Model):
    id = models.AutoField(primary_key=True)

    contact_id = models.CharField(max_length=255)
    tenant_id = models.CharField(max_length=255)
    name = models.CharField(max_length=255, blank=True, null=True)
    updated_date_utc = models.DateTimeField(blank=True, null=True)
    raw_payload = models.JSONField(blank=True, null=True)
    ingestion_timestamp = models.DateTimeField(blank=True, null=True)
    source_system = models.CharField(max_length=50, blank=True, null=True)

    class Meta:
        unique_together = (('tenant_id', 'contact_id'), )

    def __str__(self):
        return f"{self.tenant_id} - {self.contact_id}"


class XeroInvoicesRaw(models.Model):
    id = models.AutoField(primary_key=True)

    invoice_id = models.CharField(max_length=255)
    invoice_number = models.CharField(max_length=255, blank=True, null=True)
    tenant_id = models.CharField(max_length=255)
    reference = models.CharField(max_length=255, blank=True, null=True)
    date = models.DateTimeField(blank=True, null=True)
    due_date = models.DateTimeField(blank=True, null=True)
    updated_date_utc = models.DateTimeField(blank=True, null=True)
    fully_paid_on_date = models.DateTimeField(blank=True, null=True)
    raw_payload = models.JSONField(blank=True, null=True)
    ingestion_timestamp = models.DateTimeField(blank=True, null=True)
    source_system = models.CharField(max_length=50, blank=True, null=True)

    class Meta:
        unique_together = (('tenant_id', 'invoice_id'), )

    def __str__(self):
        return f"{self.tenant_id} - {self.invoice_id}"


class XeroJournalsRaw(models.Model):
    id = models.AutoField(primary_key=True)

    journal_id = models.CharField(max_length=255)
    journal_number = models.IntegerField()
    tenant_id = models.CharField(max_length=255)
    reference = models.TextField(blank=True, null=True)
    source_id = models.CharField(max_length=255, blank=True, null=True)
    source_type = models.CharField(max_length=255, blank=True, null=True)
    journal_date = models.DateTimeField(blank=True, null=True)
    created_date_utc = models.DateTimeField(blank=True, null=True)
    raw_payload = models.JSONField(blank=True, null=True)
    ingestion_timestamp = models.DateTimeField(blank=True, null=True)
    source_system = models.CharField(max_length=50, blank=True, null=True)

    class Meta:
        db_table = '"production"."raw_data"."xero_journals"'
        unique_together = (('tenant_id', 'journal_id'), )

    def __str__(self):
        return f"{self.tenant_id} - {self.journal_id}"
    
    