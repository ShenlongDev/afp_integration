from django.db import models


class NetSuiteRawBase(models.Model):
    consolidation_key = models.CharField(max_length=255)
    raw_payload = models.JSONField()
    ingestion_timestamp = models.DateTimeField(auto_now_add=True)


    class Meta:
        abstract = True


class NetSuiteRawAccounts(NetSuiteRawBase):
    class Meta:
        db_table = 'raw_data.netsuite_accounts'


class NetSuiteRawTransactions(NetSuiteRawBase):
    class Meta:
        db_table = 'raw_data.netsuite_transactions'


class NetSuiteRawTransactionLine(NetSuiteRawBase):
    class Meta:
        db_table = 'raw_data.netsuite_transaction_line'


class NetSuiteRawTransactionAccountingLine(NetSuiteRawBase):
    class Meta:
        db_table = 'raw_data.netsuite_transaction_accounting_line'


class NetSuiteRawSubsidiary(NetSuiteRawBase):
    class Meta:
        db_table = 'raw_data.netsuite_subsidiary'


class NetSuiteRawEntity(NetSuiteRawBase):
    class Meta:
        db_table = 'raw_data.netsuite_entity'


class NetSuiteRawAccountingPeriod(NetSuiteRawBase):
    class Meta:
        db_table = 'raw_data.netsuite_accounting_periods'


class NetSuiteRawDepartment(NetSuiteRawBase):
    class Meta:
        db_table = 'raw_data.netsuite_departments'


class NetSuiteRawBudget(NetSuiteRawBase):
    class Meta:
        db_table = 'raw_data.netsuite_budgets'


class NetSuiteRawBudgetLine(NetSuiteRawBase):
    class Meta:
        db_table = 'raw_data.netsuite_budget_lines'


class Companies(models.Model):
    company_name = models.CharField(max_length=255)
    consolidation_key = models.CharField(max_length=255, unique=True)


class NetSuiteRawBase(models.Model):
    consolidation_key = models.CharField(max_length=255)
    raw_payload = models.JSONField()
    ingestion_timestamp = models.DateTimeField(auto_now_add=True)
    companies = models.ForeignKey('Companies', on_delete=models.CASCADE)


    class Meta:
        abstract = True
