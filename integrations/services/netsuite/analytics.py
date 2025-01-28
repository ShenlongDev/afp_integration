from django.db import models
from django.contrib.postgres.indexes import BTreeIndex, GinIndex


class NetSuiteAccounts(models.Model):
    company_name = models.CharField(max_length=255, db_index=True)
    account_id = models.CharField(max_length=255, db_index=True)
    account_number = models.CharField(max_length=255)
    account_name = models.CharField(max_length=255)
    account_hierarchy = models.CharField(max_length=255)
    account_display_name = models.CharField(max_length=255)
    account_display_hierarchy = models.CharField(max_length=255)
    parent_id = models.CharField(max_length=255, null=True)
    parent_account = models.CharField(max_length=255, null=True)
    account_type = models.CharField(max_length=255)
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
    record_date = models.DateTimeField()


    class Meta:
        db_table = 'netsuite.accounts'
        indexes = [
            BTreeIndex(fields=['company_name', 'account_number']),
            BTreeIndex(fields=['company_name', 'account_id']),
            models.Index(fields=['subsidiary'])
        ]


class NetSuiteTransactions(models.Model):
    company_name = models.CharField(max_length=255, db_index=True)
    abbrevtype = models.CharField(max_length=255)
    transactionid = models.CharField(max_length=255, db_index=True)
    uniquekey = models.CharField(max_length=255)
    linesequencenumber = models.IntegerField()
    lineid = models.CharField(max_length=255)
    approvalstatus = models.CharField(max_length=255)
    number = models.CharField(max_length=255)
    source = models.CharField(max_length=255)
    status = models.CharField(max_length=255)
    trandisplayname = models.CharField(max_length=255)
    tranid = models.CharField(max_length=255)
    transactionnumber = models.CharField(max_length=255)
    type = models.CharField(max_length=255)
    recordtype = models.CharField(max_length=255)
    createdby = models.CharField(max_length=255)
    createddate = models.DateTimeField(db_index=True)
    lastmodifiedby = models.CharField(max_length=255)
    lastmodifieddate = models.DateTimeField(db_index=True)
    postingperiod = models.CharField(max_length=255)
    yearperiod = models.IntegerField()
    trandate = models.DateField(db_index=True)
    subsidiary = models.CharField(max_length=255)
    subsidiaryfullname = models.CharField(max_length=255)
    subsidiaryid = models.CharField(max_length=255)
    department = models.CharField(max_length=255, null=True)
    departmentid = models.CharField(max_length=255, null=True)
    location = models.CharField(max_length=255, null=True)
    class_field = models.CharField(db_column='class', max_length=255, null=True)
    memo = models.TextField(null=True)
    linememo = models.TextField(null=True)
    externalid = models.CharField(max_length=255, null=True)
    entity = models.CharField(max_length=255, null=True)
    entityid = models.CharField(max_length=255, null=True)
    account = models.CharField(max_length=255)
    acctnumber = models.CharField(max_length=255)
    accountsearchdisplayname = models.CharField(max_length=255)
    amount = models.DecimalField(max_digits=19, decimal_places=2)
    debit = models.DecimalField(max_digits=19, decimal_places=2, null=True)
    credit = models.DecimalField(max_digits=19, decimal_places=2, null=True)
    netamount = models.DecimalField(max_digits=19, decimal_places=2)
    currency = models.CharField(max_length=3)
    exchangerate = models.DecimalField(max_digits=19, decimal_places=6)
    record_date = models.DateTimeField()
    duplicate_check = models.IntegerField()


    class Meta:
        db_table = 'netsuite.transactions'
        indexes = [
            BTreeIndex(fields=['company_name', 'transactionid']),
            BTreeIndex(fields=['company_name', 'trandate']),
            BTreeIndex(fields=['company_name', 'postingperiod']),
            models.Index(fields=['account']),
            models.Index(fields=['subsidiary'])
        ]
        constraints = [
            models.UniqueConstraint(
                fields=['company_name', 'transactionid', 'linesequencenumber'],
                name='unique_transaction_line'
            )
        ]


class NetSuiteGeneralLedger(models.Model):
    company_name = models.CharField(max_length=255, db_index=True)
    abbrevtype = models.CharField(max_length=255)
    transactionid = models.CharField(max_length=255, db_index=True)
    uniquekey = models.CharField(max_length=255)
    linesequencenumber = models.IntegerField()
    lineid = models.CharField(max_length=255)
    approvalstatus = models.CharField(max_length=255)
    postingperiod = models.CharField(max_length=255, db_index=True)
    yearperiod = models.IntegerField()
    trandate = models.DateField(db_index=True)
    subsidiary = models.CharField(max_length=255)
    account = models.CharField(max_length=255)
    acctnumber = models.CharField(max_length=255)
    amount = models.DecimalField(max_digits=19, decimal_places=2)
    debit = models.DecimalField(max_digits=19, decimal_places=2, null=True)
    credit = models.DecimalField(max_digits=19, decimal_places=2, null=True)
    netamount = models.DecimalField(max_digits=19, decimal_places=2)
    currency = models.CharField(max_length=3)
    exchangerate = models.DecimalField(max_digits=19, decimal_places=6)
    record_date = models.DateTimeField()


    class Meta:
        db_table = 'netsuite.general_ledger'
        indexes = [
            BTreeIndex(fields=['company_name', 'trandate']),
            BTreeIndex(fields=['company_name', 'postingperiod']),
            BTreeIndex(fields=['company_name', 'account']),
            models.Index(fields=['subsidiary'])
        ]
