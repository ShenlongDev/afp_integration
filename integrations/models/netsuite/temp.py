from django.db import models
from core.models import Organisation


class NetSuiteTransactionAccountingLine1(models.Model):
    tenant_id = models.IntegerField(null=True, blank=True)
    
    # Text fields
    links = models.TextField(null=True, blank=True)
    accountingbook = models.TextField(null=True, blank=True)
    posting = models.TextField(null=True, blank=True)
    processedbyrevcommit = models.TextField(null=True, blank=True)
    
    # Numeric fields
    account = models.BigIntegerField(null=True, blank=True)
    amount = models.DecimalField(max_digits=15, decimal_places=2, null=True, blank=True)
    amountlinked = models.DecimalField(max_digits=15, decimal_places=2, null=True, blank=True)
    debit = models.DecimalField(max_digits=15, decimal_places=2, null=True, blank=True)
    netamount = models.DecimalField(max_digits=15, decimal_places=2, null=True, blank=True)
    paymentamountunused = models.DecimalField(max_digits=15, decimal_places=2, null=True, blank=True)
    paymentamountused = models.DecimalField(max_digits=15, decimal_places=2, null=True, blank=True)
    credit = models.DecimalField(max_digits=15, decimal_places=2, null=True, blank=True)
    amountpaid = models.DecimalField(max_digits=15, decimal_places=2, null=True, blank=True)
    amountunpaid = models.DecimalField(max_digits=15, decimal_places=2, null=True, blank=True)
    
    # Transaction identifiers – here we store them as strings to allow use of .lower() in transformations.
    transaction = models.CharField(max_length=50, null=True, blank=True)
    transaction_line = models.CharField(max_length=50, null=True, blank=True)
    
    # Date/Time field
    lastmodifieddate = models.DateTimeField(null=True, blank=True)
    
    # NOT NULL per SQL.
    consolidation_key = models.CharField(max_length=255, null=True)
    
    source_uri = models.TextField(null=True, blank=True)
    
    def __str__(self):
        return f"AcctLine {self.transaction}-{self.transaction_line}"

    class Meta:
        indexes = [
            models.Index(fields=['tenant_id', 'transaction', 'transaction_line']),
            models.Index(fields=['lastmodifieddate']),
            models.Index(fields=['consolidation_key']),
        ]


class NetSuiteTransactionLine1(models.Model):
    id = models.AutoField(primary_key=True)
    transaction_line_id = models.BigIntegerField(null=True, blank=True)
    tenant_id = models.IntegerField(null=True, blank=True)
    unique_key = models.CharField(max_length=32, unique=True, null=True, blank=True)
    
    # Many of the long VARCHAR columns are stored as TextFields.
    links = models.TextField(null=True, blank=True)
    accountinglinetype = models.TextField(null=True, blank=True)
    cleared = models.TextField(null=True, blank=True)
    commitmentfirm = models.TextField(null=True, blank=True)
    old_commitment_firm = models.TextField(null=True, blank=True)
    department = models.TextField(null=True, blank=True)
    departmentid = models.BigIntegerField(null=True, blank=True)
    donotdisplayline = models.TextField(null=True, blank=True)
    eliminate = models.TextField(null=True, blank=True)
    entity = models.TextField(null=True, blank=True)
    entityid = models.BigIntegerField(null=True, blank=True)
    expenseaccount = models.TextField(null=True, blank=True)
    expenseaccountid = models.BigIntegerField(null=True, blank=True)
    foreignamount = models.FloatField(null=True, blank=True)
    foreignamountpaid = models.FloatField(null=True, blank=True)
    foreignamountunpaid = models.FloatField(null=True, blank=True)
    
    # Fields already defined in your original model (some sizes increased to TextField)
    is_billable = models.CharField(max_length=50, blank=True, null=True)
    is_closed = models.CharField(max_length=50, blank=True, null=True)
    is_cogs = models.CharField(max_length=50, blank=True, null=True)
    is_custom_gl_line = models.CharField(max_length=50, blank=True, null=True)
    is_fully_shipped = models.CharField(max_length=50, blank=True, null=True)
    is_fx_variance = models.CharField(max_length=50, blank=True, null=True)
    is_inventory_affecting = models.CharField(max_length=50, blank=True, null=True)
    is_rev_rec_transaction = models.CharField(max_length=50, blank=True, null=True)
    
    line_last_modified_date = models.DateField(null=True, blank=True)
    line_sequence_number = models.IntegerField(null=True, blank=True)
    main_line = models.CharField(max_length=50, blank=True, null=True)
    match_bill_to_receipt = models.CharField(max_length=50, blank=True, null=True)
    memo = models.TextField(null=True, blank=True)
    net_amount = models.DecimalField(max_digits=14, decimal_places=2, null=True, blank=True)
    quantity_billed = models.FloatField(null=True, blank=True)
    quantity_rejected = models.FloatField(null=True, blank=True)
    quantity_ship_recv = models.FloatField(null=True, blank=True)
    
    # Additional fields from the SQL:
    subsidiary = models.TextField(null=True, blank=True)
    subsidiaryid = models.BigIntegerField(null=True, blank=True)
    tax_line = models.TextField(null=True, blank=True)
    transactionid = models.BigIntegerField(null=True, blank=True)
    transaction_discount = models.TextField(null=True, blank=True)
    uniquekey = models.BigIntegerField(null=True, blank=True)
    creditforeignamount = models.FloatField(null=True, blank=True)
    closedate = models.DateField(null=True, blank=True)
    documentnumber = models.TextField(null=True, blank=True)
    # Use TextField for long text data.
    location = models.TextField(null=True, blank=True)
    class_field = models.TextField(null=True, blank=True)
    
    consolidation_key = models.CharField(max_length=255, null=True)
    source_uri = models.TextField(null=True, blank=True)
    
    # Timestamp fields for tracking creation and updates
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    def __str__(self):
        return f"TransactionLine {self.id}"
  
    class Meta:
        indexes = [
            models.Index(fields=['tenant_id', 'transaction_line_id']),
            models.Index(fields=['uniquekey']),
        ]

class NetSuiteTransactions1(models.Model):
    transactionid = models.CharField(max_length=50, null=True)
    tenant_id = models.IntegerField(null=True, blank=True)
    
    # Text (VARCHAR) fields
    links = models.TextField(null=True, blank=True)
    abbrevtype = models.TextField(null=True, blank=True)
    approvalstatus = models.TextField(null=True, blank=True)
    balsegstatus = models.TextField(null=True, blank=True)
    billingstatus = models.TextField(null=True, blank=True)
    createdby = models.TextField(null=True, blank=True)
    currency = models.TextField(null=True, blank=True)
    custbody5 = models.TextField(null=True, blank=True)
    custbody_cash_register = models.TextField(null=True, blank=True)
    custbody_nondeductible_processed = models.TextField(null=True, blank=True)
    custbody_rpc_duplicate_bill_created = models.TextField(null=True, blank=True)
    custbody_rpc_duplicate_bill_credit_crt = models.TextField(null=True, blank=True)
    custbody_rpc_lightyear_bill = models.TextField(null=True, blank=True)
    custbody_rpc_lightyear_bill_credit = models.TextField(null=True, blank=True)
    custbody_wrong_subs = models.TextField(null=True, blank=True)
    entity = models.TextField(null=True, blank=True)
    externalid = models.TextField(null=True, blank=True)
    foreigntotal = models.TextField(null=True, blank=True)
    intercoadj = models.TextField(null=True, blank=True)
    isfinchrg = models.TextField(null=True, blank=True)
    isreversal = models.TextField(null=True, blank=True)
    lastmodifiedby = models.TextField(null=True, blank=True)
    ordpicked = models.TextField(null=True, blank=True)
    paymenthold = models.TextField(null=True, blank=True)
    posting = models.TextField(null=True, blank=True)
    postingperiod = models.TextField(null=True, blank=True)
    printedpickingticket = models.TextField(null=True, blank=True)
    recordtype = models.TextField(null=True, blank=True)
    source = models.TextField(null=True, blank=True)
    status = models.TextField(null=True, blank=True)
    terms = models.TextField(null=True, blank=True)
    tobeprinted = models.TextField(null=True, blank=True)
    trandisplayname = models.TextField(null=True, blank=True)
    tranid = models.TextField(null=True, blank=True)
    transactionnumber = models.TextField(null=True, blank=True)
    type = models.TextField(null=True, blank=True)
    userevenuearrangement = models.TextField(null=True, blank=True)
    visibletocustomer = models.TextField(null=True, blank=True)
    void_field = models.TextField(null=True, blank=True)  # 'void' is a reserved word
    voided = models.TextField(null=True, blank=True)
    custbody_nexus_notc = models.TextField(null=True, blank=True)
    custbody_rpc_payment_approved = models.TextField(null=True, blank=True)
    custbody_rpc_same_reference_number = models.TextField(null=True, blank=True)
    memo = models.TextField(null=True, blank=True)
    source_uri = models.TextField(null=True, blank=True)
    
    # Date fields
    closedate = models.DateField(null=True, blank=True)
    createddate = models.DateField(null=True, blank=True)
    duedate = models.DateField(null=True, blank=True)
    trandate = models.DateField(null=True, blank=True)
    lastmodifieddate = models.DateField(null=True, blank=True)
    # Timestamp field from SQL:
    custbody_report_timestamp = models.DateTimeField(null=True, blank=True)
    record_date = models.DateTimeField(null=True, blank=True)
    
    # Numeric fields
    customtype = models.BigIntegerField(null=True, blank=True)
    daysopen = models.BigIntegerField(null=True, blank=True)
    daysoverduesearch = models.BigIntegerField(null=True, blank=True)
    exchangerate = models.FloatField(null=True, blank=True)
    foreignamountpaid = models.FloatField(null=True, blank=True)
    foreignamountunpaid = models.FloatField(null=True, blank=True)
    number = models.DecimalField(max_digits=50, decimal_places=20, null=True, blank=True)
    nexus = models.BigIntegerField(null=True, blank=True)
    
    # This field is NOT NULL per SQL.
    consolidation_key = models.CharField(max_length=255, null=True)
    
    def __str__(self):
        return f"Transaction {self.transactionid}"

    class Meta:
        indexes = [
            models.Index(fields=['tenant_id', 'transactionid']),
            models.Index(fields=['tenant_id', 'lastmodifieddate']),
        ]
