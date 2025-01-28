from rest_framework import serializers
from .raw import (
    SquareOrders,
    XeroAccountsRaw,
    XeroBankTransactionsRaw,
    XeroBudgetPeriodBalancesRaw,
    XeroBudgetsRaw,
    XeroConnectionsRaw,
    XeroContactsRaw,
    XeroInvoicesRaw,
    XeroJournalsRaw
)

class SquareOrdersSerializer(serializers.ModelSerializer):
    class Meta:
        model = SquareOrders
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

class XeroConnectionsRawSerializer(serializers.ModelSerializer):
    class Meta:
        model = XeroConnectionsRaw
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
