# from django.db import models
# from django.contrib.postgres.indexes import GinIndex, BTreeIndex


# class NetSuiteRawBase(models.Model):
#     consolidation_key = models.CharField(max_length=255, db_index=True)
#     raw_payload = models.JSONField()
#     ingestion_timestamp = models.DateTimeField(auto_now_add=True, db_index=True)
#     companies = models.ForeignKey('Companies', on_delete=models.CASCADE)


#     class Meta:
#         abstract = True
#         indexes = [
#             GinIndex(fields=['raw_payload']),
#             models.Index(fields=['consolidation_key', 'ingestion_timestamp'])
#         ]


# class Companies(models.Model):
#     company_name = models.CharField(max_length=255)
#     consolidation_key = models.CharField(max_length=255, unique=True)
   
#     class Meta:
#         db_table = 'netsuite.companies'
#         indexes = [
#             models.Index(fields=['consolidation_key'])
#         ]


# class NetSuiteRawAccounts(NetSuiteRawBase):
#     class Meta:
#         db_table = 'raw_data.netsuite_accounts'
#         indexes = [
#             BTreeIndex(fields=['consolidation_key']),
#             GinIndex(fields=['raw_payload']),
#             models.Index(fields=['ingestion_timestamp'])
#         ]


# class NetSuiteRawTransactions(NetSuiteRawBase):
#     class Meta:
#         db_table = 'raw_data.netsuite_transactions'
#         indexes = [
#             BTreeIndex(fields=['consolidation_key']),
#             GinIndex(fields=['raw_payload']),
#             models.Index(fields=['ingestion_timestamp'])
#         ]


# class NetSuiteRawTransactionLine(NetSuiteRawBase):
#     transaction = models.ForeignKey(NetSuiteRawTransactions,
#                                   on_delete=models.CASCADE,
#                                   related_name='transaction_lines')
   
#     class Meta:
#         db_table = 'raw_data.netsuite_transaction_line'
#         indexes = [
#             BTreeIndex(fields=['consolidation_key']),
#             GinIndex(fields=['raw_payload']),
#             models.Index(fields=['ingestion_timestamp'])
#         ]


# class NetSuiteRawTransactionAccountingLine(NetSuiteRawBase):
#     transaction = models.ForeignKey(NetSuiteRawTransactions,
#                                   on_delete=models.CASCADE,
#                                   related_name='accounting_lines')
   
#     class Meta:
#         db_table = 'raw_data.netsuite_transaction_accounting_line'
#         indexes = [
#             BTreeIndex(fields=['consolidation_key']),
#             GinIndex(fields=['raw_payload']),
#             models.Index(fields=['ingestion_timestamp'])
#         ]


# class NetSuiteRawSubsidiary(NetSuiteRawBase):
#     class Meta:
#         db_table = 'raw_data.netsuite_subsidiary'
#         indexes = [
#             BTreeIndex(fields=['consolidation_key']),
#             GinIndex(fields=['raw_payload']),
#             models.Index(fields=['ingestion_timestamp'])
#         ]


# class NetSuiteRawEntity(NetSuiteRawBase):
#     class Meta:
#         db_table = 'raw_data.netsuite_entity'
#         indexes = [
#             BTreeIndex(fields=['consolidation_key']),
#             GinIndex(fields=['raw_payload']),
#             models.Index(fields=['ingestion_timestamp'])
#         ]


# class NetSuiteRawAccountingPeriod(NetSuiteRawBase):
#     class Meta:
#         db_table = 'raw_data.netsuite_accounting_periods'
#         indexes = [
#             BTreeIndex(fields=['consolidation_key']),
#             GinIndex(fields=['raw_payload']),
#             models.Index(fields=['ingestion_timestamp'])
#         ]


# class NetSuiteRawDepartment(NetSuiteRawBase):
#     class Meta:
#         db_table = 'raw_data.netsuite_departments'
#         indexes = [
#             BTreeIndex(fields=['consolidation_key']),
#             GinIndex(fields=['raw_payload']),
#             models.Index(fields=['ingestion_timestamp'])
#         ]


# class NetSuiteRawBudget(NetSuiteRawBase):
#     class Meta:
#         db_table = 'raw_data.netsuite_budgets'
#         indexes = [
#             BTreeIndex(fields=['consolidation_key']),
#             GinIndex(fields=['raw_payload']),
#             models.Index(fields=['ingestion_timestamp'])
#         ]


# class NetSuiteRawBudgetLine(NetSuiteRawBase):
#     budget = models.ForeignKey(NetSuiteRawBudget,
#                              on_delete=models.CASCADE,
#                              related_name='budget_lines')
   
#     class Meta:
#         db_table = 'raw_data.netsuite_budget_lines'
#         indexes = [
#             BTreeIndex(fields=['consolidation_key']),
#             GinIndex(fields=['raw_payload']),
#             models.Index(fields=['ingestion_timestamp'])
#         ]


# class NetSuiteAccountingPeriodsRaw(models.Model):
#     company_name = models.CharField(max_length=255, db_index=True)
#     period_id = models.CharField(max_length=255, db_index=True)
#     period_name = models.CharField(max_length=255)
#     start_date = models.DateField(db_index=True)
#     end_date = models.DateField(db_index=True)
#     closed = models.BooleanField()
#     all_locked = models.BooleanField()
#     fiscal_calendar = models.CharField(max_length=255)
#     year = models.IntegerField(db_index=True)
#     quarter = models.IntegerField()
#     period = models.IntegerField()
#     record_date = models.DateTimeField()


#     class Meta:
#         db_table = 'netsuite.accounting_periods'
#         indexes = [
#             BTreeIndex(fields=['company_name', 'year']),
#             BTreeIndex(fields=['company_name', 'start_date']),
#             models.Index(fields=['period_name'])
#         ]
#         constraints = [
#             models.UniqueConstraint(
#                 fields=['company_name', 'period_id'],
#                 name='unique_accounting_period'
#             )
#         ]


# class NetSuiteDepartmentsraw(models.Model):
#     company_name = models.CharField(max_length=255, db_index=True)
#     department_id = models.CharField(max_length=255, db_index=True)
#     name = models.CharField(max_length=255)
#     full_name = models.CharField(max_length=255)
#     subsidiary = models.CharField(max_length=255, db_index=True)
#     is_inactive = models.BooleanField()
#     record_date = models.DateTimeField()


#     class Meta:
#         db_table = 'netsuite.departments'
#         indexes = [
#             BTreeIndex(fields=['company_name', 'department_id']),
#             BTreeIndex(fields=['company_name', 'name'])
#         ]
#         constraints = [
#             models.UniqueConstraint(
#                 fields=['company_name', 'department_id'],
#                 name='unique_department'
#             )
#         ]


# class NetSuiteSubsidiaries(models.Model):
#     company_name = models.CharField(max_length=255, db_index=True)
#     subsidiary_id = models.CharField(max_length=255, db_index=True)
#     name = models.CharField(max_length=255)
#     name_nohi = models.CharField(max_length=255)
#     full_name = models.CharField(max_length=255)
#     legal_name = models.CharField(max_length=255)
#     federal_number = models.CharField(max_length=255, null=True)
#     is_elimination = models.BooleanField()
#     currency = models.CharField(max_length=3)
#     country = models.CharField(max_length=255)
#     record_date = models.DateTimeField()


#     class Meta:
#         db_table = 'netsuite.subsidiaries'
#         indexes = [
#             BTreeIndex(fields=['company_name', 'subsidiary_id']),
#             BTreeIndex(fields=['company_name', 'name']),
#             models.Index(fields=['currency'])
#         ]
#         constraints = [
#             models.UniqueConstraint(
#                 fields=['company_name', 'subsidiary_id'],
#                 name='unique_subsidiary'
#             )
#         ]


# class NetSuiteVendors(models.Model):
#     company_name = models.CharField(max_length=255, db_index=True)
#     vendor_id = models.CharField(max_length=255, db_index=True)
#     entity_id = models.CharField(max_length=255)
#     company_name = models.CharField(max_length=255)  # Note: This is duplicated, might want to rename
#     is_person = models.BooleanField()
#     is_inactive = models.BooleanField()
#     email = models.EmailField(null=True)
#     phone = models.CharField(max_length=255, null=True)
#     currency = models.CharField(max_length=3)
#     subsidiary = models.CharField(max_length=255, db_index=True)
#     terms = models.CharField(max_length=255, null=True)
#     record_date = models.DateTimeField()


#     class Meta:
#         db_table = 'netsuite.vendors'
#         indexes = [
#             BTreeIndex(fields=['company_name', 'vendor_id']),
#             BTreeIndex(fields=['company_name', 'entity_id']),
#             models.Index(fields=['email'])
#         ]
#         constraints = [
#             models.UniqueConstraint(
#                 fields=['company_name', 'vendor_id'],
#                 name='unique_vendor'
#             )
#         ]


# class NetSuiteBudgetPeriodBalances(models.Model):
#     company_name = models.CharField(max_length=255, db_index=True)
#     budget_id = models.CharField(max_length=255, db_index=True)
#     budget_name = models.CharField(max_length=255)
#     budget_status = models.CharField(max_length=255)
#     budget_type = models.CharField(max_length=255)
#     account_id = models.CharField(max_length=255, db_index=True)
#     account_code = models.CharField(max_length=255)
#     account_name = models.CharField(max_length=255)
#     account_class = models.CharField(max_length=50)
#     department = models.CharField(max_length=255, null=True)
#     location = models.CharField(max_length=255, null=True)
#     period = models.CharField(max_length=255, db_index=True)
#     amount = models.DecimalField(max_digits=19, decimal_places=2)
#     notes = models.TextField(null=True)
#     updated_date_utc = models.DateTimeField(db_index=True)
#     ingestion_timestamp = models.DateTimeField(auto_now_add=True)
#     source_system = models.CharField(max_length=50, default='NETSUITE')


#     class Meta:
#         db_table = 'netsuite.budget_period_balances'
#         indexes = [
#             BTreeIndex(fields=['company_name', 'budget_id']),
#             BTreeIndex(fields=['company_name', 'account_id']),
#             BTreeIndex(fields=['company_name', 'period']),
#             models.Index(fields=['updated_date_utc'])
#         ]
#         constraints = [
#             models.UniqueConstraint(
#                 fields=['company_name', 'budget_id', 'account_id', 'period'],
#                 name='unique_budget_period_balance'
#             )
#         ]


# class NetSuiteEntity(models.Model):
#     company_name = models.CharField(max_length=255, db_index=True)
#     entity_id = models.CharField(max_length=255, db_index=True)
#     entity_number = models.CharField(max_length=255)
#     company_name = models.CharField(max_length=255)  # Note: This is duplicated, might want to rename
#     legal_name = models.CharField(max_length=255, null=True)
#     is_person = models.BooleanField()
#     is_inactive = models.BooleanField()
#     parent_entity = models.CharField(max_length=255, null=True)
#     email = models.EmailField(null=True)
#     phone = models.CharField(max_length=255, null=True)
#     currency = models.CharField(max_length=3)
#     subsidiary = models.CharField(max_length=255, db_index=True)
#     terms = models.CharField(max_length=255, null=True)
#     last_modified_date = models.DateTimeField(db_index=True)
#     record_date = models.DateTimeField()


#     class Meta:   
#         db_table = 'netsuite.entity'
#         indexes = [
#             BTreeIndex(fields=['company_name', 'entity_id']),
#             BTreeIndex(fields=['company_name', 'entity_number']),
#             models.Index(fields=['email']),
#             models.Index(fields=['last_modified_date'])
#         ]
#         constraints = [
#             models.UniqueConstraint(
#                 fields=['company_name', 'entity_id'],
#                 name='unique_entity'
#             )
#         ]
