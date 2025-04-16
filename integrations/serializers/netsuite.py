from rest_framework import serializers
from integrations.models.netsuite.analytics import (
    NetSuiteAccounts, NetSuiteTransactions, NetSuiteAccountingPeriods,
    NetSuiteDepartments, NetSuiteSubsidiaries, NetSuiteVendors, 
    NetSuiteBudgetPeriodBalances, NetSuiteEntity, NetSuiteJournals,
    NetSuiteTransactionAccountingLine, NetSuiteTransactionLine,
    NetSuiteTransformedTransaction, NetSuiteBudgets, NetSuiteLocations
)

class NetSuiteAccountsSerializer(serializers.ModelSerializer):
    class Meta:
        model = NetSuiteAccounts
        fields = '__all__'

class NetSuiteTransactionsSerializer(serializers.ModelSerializer):
    class Meta:
        model = NetSuiteTransactions
        fields = '__all__'

class NetSuiteAccountingPeriodsSerializer(serializers.ModelSerializer):
    class Meta:
        model = NetSuiteAccountingPeriods
        fields = '__all__'

class NetSuiteDepartmentsSerializer(serializers.ModelSerializer):
    class Meta:
        model = NetSuiteDepartments
        fields = '__all__'

class NetSuiteSubsidiariesSerializer(serializers.ModelSerializer):
    class Meta:
        model = NetSuiteSubsidiaries
        fields = '__all__'

class NetSuiteVendorsSerializer(serializers.ModelSerializer):
    class Meta:
        model = NetSuiteVendors
        fields = '__all__'

class NetSuiteBudgetPeriodBalancesSerializer(serializers.ModelSerializer):
    class Meta:
        model = NetSuiteBudgetPeriodBalances
        fields = '__all__'

class NetSuiteEntitySerializer(serializers.ModelSerializer):
    class Meta:
        model = NetSuiteEntity
        fields = '__all__'

class NetSuiteJournalsSerializer(serializers.ModelSerializer):
    class Meta:
        model = NetSuiteJournals
        fields = '__all__'

class NetSuiteTransactionAccountingLineSerializer(serializers.ModelSerializer):
    class Meta:
        model = NetSuiteTransactionAccountingLine
        fields = '__all__'

class NetSuiteTransactionLineSerializer(serializers.ModelSerializer):
    class Meta:
        model = NetSuiteTransactionLine
        fields = '__all__'

class NetSuiteTransformedTransactionSerializer(serializers.ModelSerializer):
    class Meta:
        model = NetSuiteTransformedTransaction
        fields = '__all__'

class NetSuiteBudgetsSerializer(serializers.ModelSerializer):
    class Meta:
        model = NetSuiteBudgets
        fields = '__all__'

class NetSuiteLocationsSerializer(serializers.ModelSerializer):
    class Meta:
        model = NetSuiteLocations
        fields = '__all__'
