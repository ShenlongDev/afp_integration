from django.db import models
from integrations.models.models import Organisation 


class NetSuiteAccounts(models.Model):
    company_name = models.ForeignKey(Organisation, on_delete=models.CASCADE, null=True)
    account_id = models.CharField(max_length=255, null=True)
    links = models.TextField(null=True, blank=True)
    accountsearchdisplayname = models.TextField(null=True, blank=True)
    accountsearchdisplaynamecopy = models.TextField(null=True, blank=True)
    acctnumber = models.TextField(null=True, blank=True)
    accttype = models.TextField(null=True, blank=True)
    balance = models.TextField(null=True, blank=True)  # Balance is a VARCHAR in the source
    displaynamewithhierarchy = models.TextField(null=True, blank=True)
    eliminate = models.TextField(null=True, blank=True)
    externalid = models.TextField(null=True, blank=True)
    fullname = models.TextField(null=True, blank=True)
    include_children = models.TextField(null=True, blank=True)
    inventory = models.TextField(null=True, blank=True)
    is_inactive = models.TextField(null=True, blank=True)
    is_summary = models.TextField(null=True, blank=True)
    last_modified_date = models.TextField(null=True, blank=True)
    reconcile_with_matching = models.TextField(null=True, blank=True)
    revalue = models.TextField(null=True, blank=True)
    subsidiary = models.TextField(null=True, blank=True)
    description = models.TextField(null=True, blank=True)

    parent = models.TextField(null=True, blank=True)
    department = models.TextField(null=True, blank=True)
    sspecacct = models.TextField(null=True, blank=True)
    custrecord_rpc_model_cat_group = models.TextField(null=True, blank=True)
    custrecord_rpc_model_category_sub = models.TextField(null=True, blank=True)
    custrecord3 = models.TextField(null=True, blank=True)
    consolidation_key = models.CharField(max_length=255, null=True)
    
    record_date = models.DateTimeField(null=True)

    def __str__(self):
        return f"Account {self.account_id}"
    
    class Meta:
        db_table = "Account"


class NetSuiteTransactions(models.Model):
    company_name = models.ForeignKey(Organisation, on_delete=models.CASCADE, null=True)
    # We assume the NetSuite Transaction ID is unique; here we use it as the primary key.
    transactionid = models.CharField(max_length=50, null=True)
    
    # Text fields (using TextField for very long VARCHAR columns)
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
    
    # Date/Time fields
    closedate = models.DateField(null=True, blank=True)
    createddate = models.DateField(null=True, blank=True)
    duedate = models.DateField(null=True, blank=True)
    trandate = models.DateField(null=True, blank=True)
    custbody_report_timestamp = models.DateTimeField(null=True, blank=True)
    lastmodifieddate = models.DateField(null=True, blank=True)
    record_date = models.DateTimeField(null=True, blank=True)
    
    # Numeric fields (stored as BigInteger or Float)
    customtype = models.BigIntegerField(null=True, blank=True)
    daysopen = models.BigIntegerField(null=True, blank=True)
    daysoverduesearch = models.BigIntegerField(null=True, blank=True)
    exchangerate = models.FloatField(null=True, blank=True)
    foreignamountpaid = models.FloatField(null=True, blank=True)
    foreignamountunpaid = models.FloatField(null=True, blank=True)
    number = models.DecimalField(max_digits=50, decimal_places=20, null=True, blank=True)
    nexus = models.BigIntegerField(null=True, blank=True)
    
    # This field is defined as VARCHAR(255) NOT NULL in the table.
    consolidation_key = models.CharField(max_length=255, null=True)

    def __str__(self):
        return f"Transaction {self.transactionid}"
    
    class Meta:
        db_table = "Transaction"


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
    id = models.CharField(primary_key=True, max_length=255)
    company_display_name = models.CharField(max_length=255, null=True)
    entity_id = models.CharField(max_length=255, null=True)
    entity_title = models.CharField(max_length=255, null=True)
    type = models.CharField(max_length=255, null=True)
    legal_name = models.CharField(max_length=255, null=True)
    external_id = models.CharField(max_length=255, null=True)
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


class NetSuiteTransactionLine(models.Model):
    # Use the NetSuite TransactionLine ID as the primary key.
    id = models.BigIntegerField(primary_key=True)  # maps to the "ID" column

    # Optional: record the company associated with the data import.
    company_name = models.ForeignKey(Organisation, on_delete=models.CASCADE, null=True)

    # Varchar fields
    is_billable = models.CharField(max_length=50, blank=True, null=True)            # ISBILLABLE
    is_closed = models.CharField(max_length=50, blank=True, null=True)              # ISCLOSED
    is_cogs = models.CharField(max_length=50, blank=True, null=True)                # ISCOGS
    is_custom_gl_line = models.CharField(max_length=50, blank=True, null=True)      # ISCUSTOMGLLINE
    is_fully_shipped = models.CharField(max_length=50, blank=True, null=True)       # ISFULLYSHIPPED
    is_fx_variance = models.CharField(max_length=50, blank=True, null=True)         # ISFXVARIANCE
    is_inventory_affecting = models.CharField(max_length=50, blank=True, null=True)   # ISINVENTORYAFFECTING
    is_rev_rec_transaction = models.CharField(max_length=50, blank=True, null=True) # ISREVRECTRANSACTION
    links = models.CharField(max_length=255, blank=True, null=True)                 # LINKS
    location = models.CharField(max_length=255, blank=True, null=True)              # LOCATION
    main_line = models.CharField(max_length=50, blank=True, null=True)              # MAINLINE
    match_bill_to_receipt = models.CharField(max_length=50, blank=True, null=True)  # MATCHBILLTORECEIPT
    memo = models.TextField(blank=True, null=True)                                # MEMO
    old_commitment_firm = models.CharField(max_length=255, blank=True, null=True)   # OLDCOMMITMENTFIRM
    subsidiary = models.CharField(max_length=255, blank=True, null=True)            # SUBSIDIARY
    tax_line = models.CharField(max_length=255, blank=True, null=True)              # TAXLINE
    transaction_discount = models.CharField(max_length=255, blank=True, null=True)  # TRANSACTIONDISCOUNT

    # Number/Float/Decimal fields
    line_sequence_number = models.IntegerField(blank=True, null=True)             # LINESEQUENCENUMBER
    net_amount = models.DecimalField(max_digits=14, decimal_places=2, blank=True, null=True)  # NETAMOUNT
    quantity_billed = models.FloatField(blank=True, null=True)                    # QUANTITYBILLED
    quantity_rejected = models.FloatField(blank=True, null=True)                  # QUANTITYREJECTED
    quantity_ship_recv = models.FloatField(blank=True, null=True)                 # QUANTITYSHIPRECV

    # Date field – note that even though the column is named LINELASTMODIFIEDDATE,
    # you may choose to store it as a date (or datetime) based on your needs.
    line_last_modified_date = models.DateField(blank=True, null=True)             # LINELASTMODIFIEDDATE

    def __str__(self):
        return f"Line {self.id}"
    
    
class NetSuiteTransactionAccountingLine(models.Model):
    org = models.ForeignKey(Organisation, on_delete=models.CASCADE, null=True)
    # Text fields (the huge VARCHAR columns are stored as TextFields)
    links = models.TextField(null=True, blank=True)
    accountingbook = models.TextField(null=True, blank=True)
    posting = models.TextField(null=True, blank=True)
    processedbyrevcommit = models.TextField(null=True, blank=True)
    
    # Numeric fields (using BigIntegerField for NUMBER(38,0) and DecimalField for NUMBER(15,2))
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
    
    # Transaction identifiers (assumed NUMBER(38,0))
    transaction = models.CharField(max_length=355, null=True, blank=True)
    transaction_line = models.BigIntegerField(null=True, blank=True)
    

    # Date/Time fields
    lastmodifieddate = models.DateTimeField(null=True, blank=True)
    
    # This field is defined as VARCHAR(255) NOT NULL in the source table.
    consolidation_key = models.CharField(max_length=255)

    class Meta:
        db_table = "Transaction Accounting Line"  
        
        
class NetSuiteTransformedTransaction(models.Model):
    company_name = models.ForeignKey(Organisation, on_delete=models.CASCADE, null=True)
    consolidation_key = models.IntegerField(null=True)
    # Transaction header fields (from NetSuiteTransactions)
    transactionid = models.CharField(max_length=50, null=True)
    abbrevtype = models.CharField(max_length=255, null=True)
    approvalstatus = models.CharField(max_length=255, null=True)
    number = models.DecimalField(max_digits=50, decimal_places=20, null=True, blank=True)
    source = models.CharField(max_length=255, null=True)
    status = models.CharField(max_length=255, null=True)
    trandisplayname = models.CharField(max_length=255, null=True)
    tranid = models.CharField(max_length=255, null=True)
    transactionnumber = models.CharField(max_length=255, null=True)
    type = models.CharField(max_length=255, null=True)
    recordtype = models.CharField(max_length=255, null=True)
    createdby = models.CharField(max_length=255, null=True)
    createddate = models.DateField(null=True)
    lastmodifiedby = models.CharField(max_length=255, null=True)
    lastmodifieddate = models.DateField(null=True)
    postingperiod = models.CharField(max_length=255, null=True)
    yearperiod = models.IntegerField(null=True)
    trandate = models.DateField(null=True)
    
    # Subsidiary (from the transaction line joined with subsidiary lookup)
    subsidiary = models.CharField(max_length=255, null=True)
    subsidiaryfullname = models.CharField(max_length=255, null=True)
    subsidiaryid = models.CharField(max_length=255, null=True)
    
    # Department fields (if available on the transaction line)
    department = models.CharField(max_length=255, null=True)
    departmentid = models.CharField(max_length=255, null=True)
    
    # Fields from the transaction line (L)
    linesequencenumber = models.IntegerField(null=True)
    lineid = models.CharField(max_length=255, null=True)
    location = models.CharField(max_length=255, null=True)
    clas = models.CharField(max_length=255, null=True)  # renamed from "class"
    linenmemo = models.TextField(null=True)
    
    # Common header/line fields
    memo = models.TextField(null=True)
    externalid = models.CharField(max_length=255, null=True)
    entity_id = models.CharField(max_length=255, null=True)
    entity_title = models.CharField(max_length=255, null=True)
    legal_name = models.CharField(max_length=255, null=True)
    parent_entity = models.CharField(max_length=255, null=True)
    entity_email = models.EmailField(null=True)
    entity_phone = models.CharField(max_length=255, null=True)
    terms = models.CharField(max_length=255, null=True)
    daysopen = models.IntegerField(null=True)
    daysoverduesearch = models.IntegerField(null=True)
    duedate = models.DateField(null=True)
    closedate = models.DateField(null=True)
    
    # Fields coming from the accounting line and account lookup
    account = models.CharField(max_length=255, null=True)
    acctnumber = models.CharField(max_length=255, null=True)
    accountsearchdisplayname = models.CharField(max_length=255, null=True)
    expenseaccount = models.CharField(max_length=255, null=True)
    expenseaccountid = models.CharField(max_length=255, null=True)
    accttype = models.CharField(max_length=255, null=True)
    displaynamewithhierarchy = models.CharField(max_length=255, null=True)
    fullname = models.CharField(max_length=255, null=True)
    sspecacct = models.CharField(max_length=255, null=True)
    accountinglinetype = models.CharField(max_length=255, null=True)
    lineclosedate = models.DateField(null=True)
    documentnumber = models.CharField(max_length=255, null=True)
    iscclosed = models.CharField(max_length=50, null=True)
    linelastmodifieddate = models.DateField(null=True)
    mainline = models.CharField(max_length=50, null=True)
    taxline = models.CharField(max_length=255, null=True)
    transactiondiscount = models.CharField(max_length=255, null=True)
    billingstatus = models.CharField(max_length=255, null=True)
    
    # Monetary fields from the accounting line
    accountingbook = models.CharField(max_length=255, null=True)
    amount = models.DecimalField(max_digits=15, decimal_places=2, null=True)
    amountlinked = models.DecimalField(max_digits=15, decimal_places=2, null=True)
    debit = models.DecimalField(max_digits=15, decimal_places=2, null=True)
    credit = models.DecimalField(max_digits=15, decimal_places=2, null=True)
    netamount = models.DecimalField(max_digits=15, decimal_places=2, null=True)
    linenetamount = models.DecimalField(max_digits=15, decimal_places=2, null=True)
    paymentamountunused = models.DecimalField(max_digits=15, decimal_places=2, null=True)
    paymentamountused = models.DecimalField(max_digits=15, decimal_places=2, null=True)
    posting_field = models.CharField(max_length=255, null=True)  # renamed from "posting"
    amountpaid = models.DecimalField(max_digits=15, decimal_places=2, null=True)
    amountunpaid = models.DecimalField(max_digits=15, decimal_places=2, null=True)
    
    # Other header monetary fields
    custbody_report_timestamp = models.DateTimeField(null=True)
    currency = models.CharField(max_length=3, null=True)
    exchangerate = models.DecimalField(max_digits=19, decimal_places=6, null=True)
    foreignamountpaid = models.DecimalField(max_digits=15, decimal_places=2, null=True)
    foreignamountunpaid = models.DecimalField(max_digits=15, decimal_places=2, null=True)
    foreigntotal = models.DecimalField(max_digits=15, decimal_places=2, null=True)
    foreignlineamount = models.DecimalField(max_digits=15, decimal_places=2, null=True)
    record_date = models.DateTimeField(null=True)

    def __str__(self):
        return f"Transformed Transaction {self.transactionid} Line {self.linesequencenumber}"

    class Meta:
        unique_together = ("company_name", "transactionid", "linesequencenumber")
        

      
        