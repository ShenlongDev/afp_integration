from django.db import models
from integrations.models.models import Organisation 


class NetSuiteAccounts(models.Model):
    company_name = models.ForeignKey(Organisation, on_delete=models.CASCADE, null=True)
    account_id = models.CharField(max_length=255, null=True)
    account_number = models.CharField(max_length=255, null=True)
    account_name = models.CharField(max_length=255, null=True)
    account_hierarchy = models.CharField(max_length=255, null=True)
    account_display_name = models.CharField(max_length=255, null=True)
    account_display_hierarchy = models.CharField(max_length=255, null=True)
    parent_id = models.CharField(max_length=255, null=True)
    parent_account = models.CharField(max_length=255, null=True)
    account_type = models.CharField(max_length=255, null=True)
    sspecacct = models.CharField(max_length=255, null=True)
    description = models.TextField(null=True)
    eliminate = models.BooleanField(null=True)
    external_id = models.CharField(max_length=255, null=True)
    include_children = models.BooleanField(null=True)
    inventory = models.BooleanField(null=True)
    is_inactive = models.BooleanField(null=True)
    is_summary = models.BooleanField(null=True)
    last_modified_date = models.DateTimeField(null=True)
    reconcile_with_matching = models.BooleanField(null=True)
    revalue = models.BooleanField(null=True)
    subsidiary = models.CharField(max_length=255, null=True)
    balance = models.DecimalField(max_digits=19, decimal_places=2, null=True)
    record_date = models.DateTimeField(null=True)


class NetSuiteTransactions(models.Model):
    company_name = models.ForeignKey(Organisation, on_delete=models.CASCADE, null=True)
    abbrevtype = models.CharField(max_length=255, null=True)
    transactionid = models.CharField(max_length=255, null=True)
    uniquekey = models.CharField(max_length=255, null=True)
    linesequencenumber = models.IntegerField(null=True)
    lineid = models.CharField(max_length=255, null=True)
    approvalstatus = models.CharField(max_length=255, null=True)
    number = models.CharField(max_length=255, null=True)
    source = models.CharField(max_length=255, null=True)
    status = models.CharField(max_length=255, null=True)
    trandisplayname = models.CharField(max_length=255, null=True)
    tranid = models.CharField(max_length=255, null=True)
    transactionnumber = models.CharField(max_length=255, null=True)
    type = models.CharField(max_length=255, null=True)
    recordtype = models.CharField(max_length=255, null=True)
    createdby = models.CharField(max_length=255, null=True)
    createddate = models.DateTimeField(null=True)
    lastmodifiedby = models.CharField(max_length=255, null=True)
    lastmodifieddate = models.DateTimeField(null=True)
    postingperiod = models.CharField(max_length=255, null=True)
    yearperiod = models.IntegerField(null=True)
    trandate = models.DateField(null=True)
    subsidiary = models.CharField(max_length=255, null=True)
    subsidiaryfullname = models.CharField(max_length=255, null=True)
    subsidiaryid = models.CharField(max_length=255, null=True)
    department = models.CharField(max_length=255, null=True)
    departmentid = models.CharField(max_length=255, null=True)
    location = models.CharField(max_length=255, null=True)
    class_field = models.CharField(db_column='class', max_length=255, null=True)
    memo = models.TextField(null=True)
    linememo = models.TextField(null=True)
    externalid = models.CharField(max_length=255, null=True)
    entity = models.CharField(max_length=255, null=True)
    entityid = models.CharField(max_length=255, null=True)
    account = models.CharField(max_length=255, null=True)
    acctnumber = models.CharField(max_length=255, null=True)
    accountsearchdisplayname = models.CharField(max_length=255, null=True)
    amount = models.DecimalField(max_digits=19, decimal_places=2, null=True)
    debit = models.DecimalField(max_digits=19, decimal_places=2, null=True)
    credit = models.DecimalField(max_digits=19, decimal_places=2, null=True)
    netamount = models.DecimalField(max_digits=19, decimal_places=2, null=True)
    currency = models.CharField(max_length=3, null=True)
    exchangerate = models.DecimalField(max_digits=19, decimal_places=6, null=True)
    record_date = models.DateTimeField(null=True)
    duplicate_check = models.IntegerField(null=True)


    class Meta:
        indexes = [
            models.Index(fields=['transactionid', 'linesequencenumber'])
        ]


class NetSuiteGeneralLedger(models.Model):
    company_name = models.ForeignKey(Organisation, on_delete=models.CASCADE, null=True)
    abbrevtype = models.CharField(max_length=255, null=True)
    transactionid = models.CharField(max_length=255, null=True)
    uniquekey = models.CharField(max_length=255, null=True)
    linesequencenumber = models.IntegerField(null=True)
    lineid = models.CharField(max_length=255, null=True)
    approvalstatus = models.CharField(max_length=255, null=True)
    postingperiod = models.CharField(max_length=255, null=True)
    yearperiod = models.IntegerField(null=True)
    trandate = models.DateTimeField(null=True)
    subsidiary = models.CharField(max_length=255, null=True)
    account = models.CharField(max_length=255, null=True)
    acctnumber = models.CharField(max_length=255, null=True)
    amount = models.DecimalField(max_digits=19, decimal_places=2, null=True)
    debit = models.DecimalField(max_digits=19, decimal_places=2, null=True)
    credit = models.DecimalField(max_digits=19, decimal_places=2, null=True)
    netamount = models.DecimalField(max_digits=19, decimal_places=2, null=True)
    currency = models.CharField(max_length=3, null=True)
    exchangerate = models.DecimalField(max_digits=19, decimal_places=6, null=True)
    record_date = models.DateTimeField(null=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=['company_name', 'transactionid', 'linesequencenumber'],
                name='unique_netsuite_gl_entry'
            )
        ]
        indexes = [
            models.Index(fields=['company_name', 'transactionid']),
            models.Index(fields=['trandate']),
            models.Index(fields=['account']),
        ]



class NetSuiteAccountingPeriods(models.Model):
    company_name = models.ForeignKey(Organisation, on_delete=models.CASCADE, null=True)
    period_id = models.CharField(max_length=255, null=True)
    period_name = models.CharField(max_length=255, null=True)
    start_date = models.DateField(null=True)
    end_date = models.DateField(null=True)
    closed = models.BooleanField(null=True)
    all_locked = models.BooleanField(null=True)
    fiscal_calendar = models.CharField(max_length=255, null=True)
    year = models.IntegerField(null=True)
    quarter = models.IntegerField(null=True)
    period = models.IntegerField(null=True)
    record_date = models.DateTimeField(null=True)


class NetSuiteDepartments(models.Model):
    company_name = models.ForeignKey(Organisation, on_delete=models.CASCADE, null=True)
    department_id = models.CharField(max_length=255, null=True)
    name = models.CharField(max_length=255, null=True)
    full_name = models.CharField(max_length=255, null=True)
    subsidiary = models.CharField(max_length=255, null=True)
    is_inactive = models.BooleanField(null=True)
    record_date = models.DateTimeField(null=True)


class NetSuiteSubsidiaries(models.Model):
    company_name = models.ForeignKey(Organisation, on_delete=models.CASCADE, null=True)
    subsidiary_id = models.CharField(max_length=255, null=True)
    name = models.CharField(max_length=255, null=True)
    name_nohi = models.CharField(max_length=255, null=True)
    full_name = models.CharField(max_length=255, null=True)
    legal_name = models.CharField(max_length=255, null=True)
    federal_number = models.CharField(max_length=255, null=True)
    is_elimination = models.BooleanField(null=True)
    currency = models.CharField(max_length=3, null=True)
    country = models.CharField(max_length=255, null=True)
    record_date = models.DateTimeField(null=True)


class NetSuiteVendors(models.Model):
    company_name = models.ForeignKey(Organisation, on_delete=models.CASCADE, null=True)
    vendor_id = models.CharField(max_length=255, null=True)
    entity_id = models.CharField(max_length=255, null=True)
    is_person = models.BooleanField(null=True)
    is_inactive = models.BooleanField(null=True)
    email = models.EmailField(null=True)
    phone = models.CharField(max_length=255, null=True)
    currency = models.CharField(max_length=3, null=True)
    subsidiary = models.CharField(max_length=255, null=True)
    terms = models.CharField(max_length=255, null=True)
    record_date = models.DateTimeField(null=True)


class NetSuiteBudgetPeriodBalances(models.Model):
    company_name = models.ForeignKey(Organisation, on_delete=models.CASCADE, null=True)
    budget_id = models.CharField(max_length=255, null=True)
    budget_name = models.CharField(max_length=255, null=True)
    budget_status = models.CharField(max_length=255, null=True)
    budget_type = models.CharField(max_length=255, null=True)
    account_id = models.CharField(max_length=255, null=True)
    account_code = models.CharField(max_length=255, null=True)
    account_name = models.CharField(max_length=255, null=True)
    account_class = models.CharField(max_length=50, null=True)
    department = models.CharField(max_length=255, null=True)
    location = models.CharField(max_length=255, null=True)
    period = models.CharField(max_length=255, null=True)
    amount = models.DecimalField(max_digits=19, decimal_places=2, null=True)
    notes = models.TextField(null=True)
    updated_date_utc = models.DateTimeField(null=True)
    ingestion_timestamp = models.DateTimeField(auto_now_add=True, null=True)
    source_system = models.CharField(max_length=50, default='NETSUITE', null=True)


    class Meta:
        unique_together = ['company_name', 'budget_id', 'account_id', 'period']


class NetSuiteEntity(models.Model):
    company_name = models.ForeignKey(Organisation, on_delete=models.CASCADE, null=True)
    company_display_name = models.CharField(max_length=255, null=True)
    entity_id = models.CharField(max_length=255, null=True)
    entity_number = models.CharField(max_length=255, null=True)
    legal_name = models.CharField(max_length=255, null=True)
    is_person = models.BooleanField(null=True)
    is_inactive = models.BooleanField(null=True)
    parent_entity = models.CharField(max_length=255, null=True)
    email = models.EmailField(null=True)
    phone = models.CharField(max_length=255, null=True)
    currency = models.CharField(max_length=3, null=True)
    subsidiary = models.CharField(max_length=255, null=True)
    terms = models.CharField(max_length=255, null=True)
    last_modified_date = models.DateTimeField(null=True)
    record_date = models.DateTimeField(null=True)


class NetSuiteJournals(models.Model):
    company_name = models.ForeignKey(Organisation, on_delete=models.CASCADE, null=True)
    journal_id = models.CharField(max_length=255, db_index=True, null=True)
    date = models.DateField(db_index=True, null=True)
    memo = models.TextField(null=True)
    account = models.CharField(max_length=255, null=True)
    debit = models.DecimalField(max_digits=19, decimal_places=2, null=True)
    credit = models.DecimalField(max_digits=19, decimal_places=2, null=True)
    currency = models.CharField(max_length=3, null=True)
    exchangerate = models.DecimalField(max_digits=19, decimal_places=6, null=True)
    record_date = models.DateTimeField(null=True)

    class Meta:
        db_table = 'netsuite.journals'
        indexes = [
            models.Index(fields=['company_name', 'journal_id']),
            models.Index(fields=['date']),
            models.Index(fields=['account']),
        ]
        constraints = [
            models.UniqueConstraint(
                fields=['company_name', 'journal_id'],
                name='unique_journal_entry'
            )
        ]
