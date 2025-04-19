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

