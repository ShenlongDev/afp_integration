
from rest_framework import serializers
from integrations.models.xero.transformations import (
    XeroAccounts,
    XeroBankTransactionLineItems,
    XeroJournalLines,
    XeroJournalLineTrackingCategories,
    XeroInvoiceLineItems,
    XeroBudgetPeriodBalances,
    XeroGeneralLedger
)

class XeroAccountsSerializer(serializers.ModelSerializer):
    class Meta:
        model = XeroAccounts
        fields = '__all__'


class XeroBankTransactionLineItemsSerializer(serializers.ModelSerializer):
    class Meta:
        model = XeroBankTransactionLineItems
        fields = '__all__'


class XeroJournalLinesSerializer(serializers.ModelSerializer):
    class Meta:
        model = XeroJournalLines
        fields = '__all__'


class XeroJournalLineTrackingCategoriesSerializer(serializers.ModelSerializer):
    class Meta:
        model = XeroJournalLineTrackingCategories
        fields = '__all__'


class XeroInvoiceLineItemsSerializer(serializers.ModelSerializer):
    class Meta:
        model = XeroInvoiceLineItems
        fields = '__all__'


class XeroBudgetPeriodBalancesSerializer(serializers.ModelSerializer):
    class Meta:
        model = XeroBudgetPeriodBalances
        fields = '__all__'


class XeroGeneralLedgerSerializer(serializers.ModelSerializer):
    class Meta:
        model = XeroGeneralLedger
        fields = '__all__'
