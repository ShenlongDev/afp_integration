from django.db import models
from integrations.models.models import Organisation


class XeroBudgetPeriodBalancesAnalytics(models.Model):
    """
    Mirrors production.analytics.xero_budget_period_balances table in Snowflake.
    """
    # We add a synthetic primary key for Django
    id = models.AutoField(primary_key=True)

    # Columns
    tenant_id = models.CharField(max_length=255)
    tenant_name = models.CharField(max_length=255)
    budget_id = models.CharField(max_length=255)
    budget_name = models.CharField(max_length=255)
    budget_status = models.CharField(max_length=255)
    budget_type = models.CharField(max_length=255)
    account_id = models.CharField(max_length=255)
    account_code = models.CharField(max_length=255, blank=True, null=True)
    account_name = models.CharField(max_length=255, blank=True, null=True)
    account_status = models.CharField(max_length=255, blank=True, null=True)
    account_tax_type = models.CharField(max_length=255, blank=True, null=True)
    account_class = models.CharField(max_length=255, blank=True, null=True)
    account_reporting_code = models.CharField(max_length=255, blank=True, null=True)
    account_reporting_code_name = models.CharField(max_length=255, blank=True, null=True)
    tracking_category_name = models.CharField(max_length=255, blank=True, null=True)
    tracking_category_option = models.CharField(max_length=255, blank=True, null=True)
    period = models.CharField(max_length=255)
    amount = models.DecimalField(max_digits=19, decimal_places=2, blank=True, null=True)
    notes = models.TextField(blank=True, null=True)
    updated_date_utc = models.DateTimeField(blank=True, null=True)
    ingestion_timestamp = models.DateTimeField(blank=True, null=True)
    source_system = models.CharField(max_length=255, blank=True, null=True)

    class Meta:
        unique_together = (('tenant_id', 'budget_id', 'account_id', 'period'), )
        verbose_name = "Xero Budget Period Balances Analytics"

    def __str__(self):
        return f"{self.tenant_id} - {self.budget_id} - {self.account_id} - {self.period}"


class XeroGeneralLedger(models.Model):
    """
    Mirrors production.analytics.xero_general_ledger table in Snowflake.
    """
    org = models.ForeignKey(
        Organisation,
        on_delete=models.CASCADE,
        related_name="xero_general_ledgers",
        null=True
    )
    tenant_id = models.CharField(max_length=255, null=True)
    tenant_name = models.CharField(max_length=255, null=True)

    journal_id = models.CharField(max_length=255, null=True)
    journal_number = models.IntegerField(null=True)
    journal_date = models.DateField(blank=True, null=True)
    created_date = models.DateTimeField(blank=True, null=True)
    journal_line_id = models.CharField(max_length=255)
    journal_reference = models.TextField(blank=True, null=True)
    source_id = models.CharField(max_length=255, blank=True, null=True)
    source_type = models.CharField(max_length=255, blank=True, null=True)
    tracking_category_name = models.CharField(max_length=255, blank=True, null=True)
    tracking_category_option = models.CharField(max_length=255, blank=True, null=True)
    account_id = models.CharField(max_length=255, blank=True, null=True)
    account_code = models.CharField(max_length=255, blank=True, null=True)
    account_type = models.CharField(max_length=255, blank=True, null=True)
    account_name = models.CharField(max_length=255, blank=True, null=True)
    account_status = models.CharField(max_length=255, blank=True, null=True)
    account_tax_type = models.CharField(max_length=255, blank=True, null=True)
    account_class = models.CharField(max_length=255, blank=True, null=True)
    account_currency_code = models.CharField(max_length=10, blank=True, null=True)
    account_reporting_code = models.CharField(max_length=255, blank=True, null=True)
    account_reporting_code_name = models.CharField(max_length=255, blank=True, null=True)
    account_description = models.TextField(blank=True, null=True)
    account_system_account = models.CharField(max_length=255, blank=True, null=True)
    statement = models.CharField(max_length=2, blank=True, null=True)
    bank_account_type = models.CharField(max_length=255, blank=True, null=True)
    journal_line_description = models.TextField(blank=True, null=True)
    net_amount = models.DecimalField(max_digits=19, decimal_places=2, blank=True, null=True)
    gross_amount = models.DecimalField(max_digits=19, decimal_places=2, blank=True, null=True)
    tax_amount = models.DecimalField(max_digits=19, decimal_places=2, blank=True, null=True)
    invoice_number = models.CharField(max_length=255, blank=True, null=True)
    invoice_url = models.TextField(blank=True, null=True)

    class Meta:
        unique_together = (('tenant_id', 'journal_id', 'journal_line_id'), )
        verbose_name = "Xero General Ledger"


class XeroGeneralLedger1(models.Model):
    """
    Mirrors production.analytics.xero_general_ledger table in Snowflake.
    """
    org = models.ForeignKey(
        Organisation,
        on_delete=models.CASCADE,
        related_name="xero_general_ledgers",
        null=True
    )
    tenant_id = models.CharField(max_length=255, null=True)
    tenant_name = models.CharField(max_length=255, null=True)

    journal_id = models.CharField(max_length=255, null=True)
    journal_number = models.IntegerField(null=True)
    journal_date = models.DateField(blank=True, null=True)
    created_date = models.DateTimeField(blank=True, null=True)
    journal_line_id = models.CharField(max_length=255)
    journal_reference = models.TextField(blank=True, null=True)
    source_id = models.CharField(max_length=255, blank=True, null=True)
    source_type = models.CharField(max_length=255, blank=True, null=True)
    tracking_category_name = models.CharField(max_length=255, blank=True, null=True)
    tracking_category_option = models.CharField(max_length=255, blank=True, null=True)
    account_id = models.CharField(max_length=255, blank=True, null=True)
    account_code = models.CharField(max_length=255, blank=True, null=True)
    account_type = models.CharField(max_length=255, blank=True, null=True)
    account_name = models.CharField(max_length=255, blank=True, null=True)
    account_status = models.CharField(max_length=255, blank=True, null=True)
    account_tax_type = models.CharField(max_length=255, blank=True, null=True)
    account_class = models.CharField(max_length=255, blank=True, null=True)
    account_currency_code = models.CharField(max_length=10, blank=True, null=True)
    account_reporting_code = models.CharField(max_length=255, blank=True, null=True)
    account_reporting_code_name = models.CharField(max_length=255, blank=True, null=True)
    account_description = models.TextField(blank=True, null=True)
    account_system_account = models.CharField(max_length=255, blank=True, null=True)
    statement = models.CharField(max_length=2, blank=True, null=True)
    bank_account_type = models.CharField(max_length=255, blank=True, null=True)
    journal_line_description = models.TextField(blank=True, null=True)
    net_amount = models.DecimalField(max_digits=19, decimal_places=2, blank=True, null=True)
    gross_amount = models.DecimalField(max_digits=19, decimal_places=2, blank=True, null=True)
    tax_amount = models.DecimalField(max_digits=19, decimal_places=2, blank=True, null=True)
    invoice_number = models.CharField(max_length=255, blank=True, null=True)
    invoice_url = models.TextField(blank=True, null=True)

    class Meta:
        unique_together = (('tenant_id', 'journal_id', 'journal_line_id'), )
        verbose_name = "Xero General Ledger1"

