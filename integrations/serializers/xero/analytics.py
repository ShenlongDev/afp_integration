from rest_framework import serializers
from .analytics import (
    XeroBudgetPeriodBalancesAnalytics,
    XeroGeneralLedger
)

class XeroBudgetPeriodBalancesAnalyticsSerializer(serializers.ModelSerializer):
    class Meta:
        model = XeroBudgetPeriodBalancesAnalytics
        fields = '__all__'

class XeroGeneralLedgerSerializer(serializers.ModelSerializer):
    class Meta:
        model = XeroGeneralLedger
        fields = '__all__'
