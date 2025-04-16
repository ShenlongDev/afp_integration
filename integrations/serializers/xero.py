from rest_framework import serializers
from integrations.models.xero.transformations import (
    XeroJournalLines, XeroJournalLineTrackingCategories, XeroInvoiceLineItems
)
from integrations.models.xero.analytics import XeroBudgetPeriodBalancesAnalytics
from integrations.models.xero.raw import (
    XeroAccountsRaw, XeroBankTransactionsRaw, XeroBudgetPeriodBalancesRaw,
    XeroBudgetsRaw, XeroContactsRaw, XeroInvoicesRaw, XeroJournalsRaw
)

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

class XeroBudgetPeriodBalancesAnalyticsSerializer(serializers.ModelSerializer):
    class Meta:
        model = XeroBudgetPeriodBalancesAnalytics
        fields = '__all__'

class XeroAccountsRawSerializer(serializers.ModelSerializer):
    class Meta:
        model = XeroAccountsRaw
        fields = '__all__'

class XeroBankTransactionsRawSerializer(serializers.ModelSerializer):
    class Meta:
        model = XeroBankTransactionsRaw
        fields = '__all__'

class XeroBudgetPeriodBalancesRawSerializer(serializers.ModelSerializer):
    class Meta:
        model = XeroBudgetPeriodBalancesRaw
        fields = '__all__'

class XeroBudgetsRawSerializer(serializers.ModelSerializer):
    class Meta:
        model = XeroBudgetsRaw
        fields = '__all__'

class XeroContactsRawSerializer(serializers.ModelSerializer):
    class Meta:
        model = XeroContactsRaw
        fields = '__all__'

class XeroInvoicesRawSerializer(serializers.ModelSerializer):
    class Meta:
        model = XeroInvoicesRaw
        fields = '__all__'

class XeroJournalsRawSerializer(serializers.ModelSerializer):
    class Meta:
        model = XeroJournalsRaw
        fields = '__all__'
