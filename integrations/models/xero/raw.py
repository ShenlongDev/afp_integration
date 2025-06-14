from django.db import models
from integrations.models.models import Organisation


class XeroAccountsRaw(models.Model):
    id = models.AutoField(primary_key=True)
    tenant_id = models.CharField(max_length=255, blank=True, null=True, db_index=True)
    account_id = models.CharField(max_length=255, blank=True, null=True, db_index=True)
    name = models.CharField(max_length=255, blank=True, null=True)
    status = models.CharField(max_length=50, blank=True, null=True)
    type = models.CharField(max_length=255, blank=True, null=True)
    updated_date_utc = models.DateTimeField(blank=True, null=True)
    raw_payload = models.JSONField(blank=True, null=True)
    ingestion_timestamp = models.DateTimeField(blank=True, null=True)
    source_system = models.CharField(max_length=50, blank=True, null=True)

    class Meta:
        unique_together = (('tenant_id', 'account_id'), )
        indexes = [
            models.Index(fields=['tenant_id']),
            models.Index(fields=['account_id']),
        ]
        verbose_name = "Xero Account Raw"

    def __str__(self):
        return f"{self.tenant_id} - {self.account_id}"


class XeroBankTransactionsRaw(models.Model):
    id = models.AutoField(primary_key=True)
    bank_transaction_id = models.CharField(max_length=255, db_index=True)
    tenant_id = models.CharField(max_length=255, blank=True, null=True, db_index=True)
    type = models.CharField(max_length=255, blank=True, null=True)
    status = models.CharField(max_length=50, blank=True, null=True)
    date = models.DateTimeField(blank=True, null=True)
    updated_date_utc = models.DateTimeField(blank=True, null=True)
    raw_payload = models.JSONField(blank=True, null=True)
    ingestion_timestamp = models.DateTimeField(blank=True, null=True)
    source_system = models.CharField(max_length=50, blank=True, null=True)

    class Meta:
        unique_together = (('tenant_id', 'bank_transaction_id'), )
        indexes = [
            models.Index(fields=['tenant_id']),
            models.Index(fields=['bank_transaction_id']),
        ]
        verbose_name = "Xero Bank Transaction Raw"

    def __str__(self):
        return f"{self.tenant_id} - {self.bank_transaction_id}"


class XeroBudgetPeriodBalancesRaw(models.Model):
    id = models.AutoField(primary_key=True)
    tenant_name = models.ForeignKey(
        Organisation,
        on_delete=models.CASCADE,
        related_name="xero_budget_period_balances_raws",
        null=True
    )
    tenant_id = models.CharField(max_length=255, db_index=True)
    budget_id = models.CharField(max_length=255, db_index=True)
    account_id = models.CharField(max_length=255, db_index=True)
    account_code = models.CharField(max_length=255, blank=True, null=True)
    account_name = models.CharField(max_length=255, blank=True, null=True)
    reporting_code = models.CharField(max_length=255, blank=True, null=True)
    reporting_code_name = models.CharField(max_length=255, blank=True, null=True)
    period = models.CharField(max_length=255, db_index=True)
    amount = models.DecimalField(max_digits=19, decimal_places=2, blank=True, null=True)
    notes = models.TextField(blank=True, null=True)
    updated_date_utc = models.DateTimeField(blank=True, null=True)
    ingestion_timestamp = models.DateTimeField(blank=True, null=True)
    source_system = models.CharField(max_length=50, blank=True, null=True)
    tracking_category_id = models.CharField(max_length=255, blank=True, null=True)
    tracking_category_name = models.CharField(max_length=255, blank=True, null=True)
    tracking_category_option = models.CharField(max_length=255, blank=True, null=True)

    class Meta:
        unique_together = (('tenant_id', 'budget_id', 'account_id', 'period'), )
        indexes = [
            models.Index(fields=['tenant_id']),
            models.Index(fields=['budget_id']),
            models.Index(fields=['account_id']),
            models.Index(fields=['period']),
        ]
        verbose_name = "Xero Budget Period Balances Raw"

    def __str__(self):
        return f"{self.tenant_id} - {self.budget_id} - {self.account_id} - {self.period}"


class XeroBudgetsRaw(models.Model):
    id = models.AutoField(primary_key=True)
    budget_id = models.CharField(max_length=255, db_index=True)
    tenant_id = models.CharField(max_length=255, blank=True, null=True, db_index=True)
    tenant_name = models.ForeignKey(
        Organisation,
        on_delete=models.CASCADE,
        related_name="xero_budgets_raws",
        null=True
    )
    status = models.CharField(max_length=50, blank=True, null=True)
    type = models.CharField(max_length=255, blank=True, null=True)
    description = models.TextField(blank=True, null=True)
    updated_date_utc = models.DateTimeField(blank=True, null=True)
    raw_payload = models.JSONField(blank=True, null=True)
    ingestion_timestamp = models.DateTimeField(blank=True, null=True)
    source_system = models.CharField(max_length=50, blank=True, null=True)

    class Meta:
        unique_together = (('tenant_id', 'budget_id'), )
        indexes = [
            models.Index(fields=['tenant_id']),
            models.Index(fields=['budget_id']),
        ]
        verbose_name = "Xero Budgets Raw"

    def __str__(self):
        return f"{self.tenant_id} - {self.budget_id}"


class XeroContactsRaw(models.Model):
    id = models.AutoField(primary_key=True)
    contact_id = models.CharField(max_length=255, db_index=True)
    tenant_id = models.CharField(max_length=255, blank=True, null=True, db_index=True)
    name = models.CharField(max_length=255, blank=True, null=True)
    updated_date_utc = models.DateTimeField(blank=True, null=True)
    raw_payload = models.JSONField(blank=True, null=True)
    ingestion_timestamp = models.DateTimeField(blank=True, null=True)
    source_system = models.CharField(max_length=50, blank=True, null=True)

    class Meta:
        unique_together = (('tenant_id', 'contact_id'), )
        indexes = [
            models.Index(fields=['tenant_id']),
            models.Index(fields=['contact_id']),
        ]
        verbose_name = "Xero Contacts Raw"

    def __str__(self):
        return f"{self.tenant_id} - {self.contact_id}"


class XeroInvoicesRaw(models.Model):
    id = models.AutoField(primary_key=True)
    invoice_id = models.CharField(max_length=255, db_index=True)
    invoice_number = models.CharField(max_length=255, blank=True, null=True)
    tenant_id = models.CharField(max_length=255, blank=True, null=True, db_index=True)
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
        indexes = [
            models.Index(fields=['tenant_id']),
            models.Index(fields=['invoice_id']),
        ]
        verbose_name = "Xero Invoices Raw"

    def __str__(self):
        return f"{self.tenant_id} - {self.invoice_id}"


class XeroJournalsRaw(models.Model):
    id = models.AutoField(primary_key=True)
    journal_id = models.CharField(max_length=255, null=True, db_index=True)
    journal_number = models.IntegerField(null=True)
    tenant_id = models.CharField(max_length=255, null=True, db_index=True)
    reference = models.TextField(blank=True, null=True)
    source_id = models.CharField(max_length=255, blank=True, null=True)
    source_type = models.CharField(max_length=255, blank=True, null=True)
    journal_date = models.DateTimeField(blank=True, null=True, db_index=True)
    created_date_utc = models.DateTimeField(blank=True, null=True)
    raw_payload = models.JSONField(blank=True, null=True)
    ingestion_timestamp = models.DateTimeField(blank=True, null=True)
    source_system = models.CharField(max_length=50, blank=True, null=True)

    class Meta:
        unique_together = (('tenant_id', 'journal_id'), )
        indexes = [
            models.Index(fields=['tenant_id']),
            models.Index(fields=['journal_id']),
            models.Index(fields=['journal_date']),
        ]
        verbose_name = "Xero Journals Raw"

    def __str__(self):
        return f"{self.tenant_id} - {self.journal_id}"

   
    