from django.contrib import admin
from integrations.models.netsuite.temp import (
    NetSuiteTransactionAccountingLine1,
    NetSuiteTransactionLine1,
    NetSuiteTransactions1
)

class NetSuiteTransactionAccountingLine1Admin(admin.ModelAdmin):
    list_display = ('tenant_id', 'transaction', 'account', 'amount', 'lastmodifieddate', 'transaction_line')
    search_fields = ('transaction', 'account', 'amount')

class NetSuiteTransactionLine1Admin(admin.ModelAdmin):
    list_display = ('transaction_line_id', 'subsidiary', 'transactionid', 'closedate')
    search_fields = ('transaction_line_id', 'transactionid', 'subsidiary')

class NetSuiteTransactions1Admin(admin.ModelAdmin):
    list_display = ('tenant_id', 'approvalstatus', 'createdby', 'transactionid', 'createddate', 'memo')
    search_fields = ('abbrevtype', 'createdby', 'transactionid', 'memo', 'transactionnumber')

admin.site.register(NetSuiteTransactionAccountingLine1, NetSuiteTransactionAccountingLine1Admin)
admin.site.register(NetSuiteTransactionLine1, NetSuiteTransactionLine1Admin)
