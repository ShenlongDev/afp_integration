from rest_framework.response import Response
from rest_framework import status
from rest_framework.views import APIView
from rest_framework import generics
from integrations.models.models import Integration
from integrations.services.netsuite.importer import NetSuiteImporter
from django.shortcuts import get_object_or_404
from django.utils import timezone
from rest_framework import viewsets
from integrations.models.netsuite.analytics import (
    NetSuiteAccounts, NetSuiteTransactions, NetSuiteAccountingPeriods,
    NetSuiteDepartments, NetSuiteSubsidiaries, NetSuiteVendors, 
    NetSuiteBudgetPeriodBalances, NetSuiteEntity, NetSuiteJournals,
    NetSuiteTransactionAccountingLine, NetSuiteTransactionLine,
    NetSuiteTransformedTransaction, NetSuiteBudgets, NetSuiteLocations
)
from serializers.netsuite import (
    NetSuiteAccountsSerializer, NetSuiteTransactionsSerializer, NetSuiteAccountingPeriodsSerializer,
    NetSuiteDepartmentsSerializer, NetSuiteSubsidiariesSerializer, NetSuiteVendorsSerializer, 
    NetSuiteBudgetPeriodBalancesSerializer, NetSuiteEntitySerializer, NetSuiteJournalsSerializer,
    NetSuiteTransactionAccountingLineSerializer, NetSuiteTransactionLineSerializer,
    NetSuiteTransformedTransactionSerializer, NetSuiteBudgetsSerializer, NetSuiteLocationsSerializer
)



class NetSuiteAccountsViewSet(viewsets.ModelViewSet):
    queryset = NetSuiteAccounts.objects.all()
    serializer_class = NetSuiteAccountsSerializer
    filterset_fields = ['tenant_id', 'account_id']

class NetSuiteTransactionsViewSet(viewsets.ModelViewSet):
    queryset = NetSuiteTransactions.objects.all()
    serializer_class = NetSuiteTransactionsSerializer
    filterset_fields = ['tenant_id', 'transactionid']

class NetSuiteAccountingPeriodsViewSet(viewsets.ModelViewSet):
    queryset = NetSuiteAccountingPeriods.objects.all()
    serializer_class = NetSuiteAccountingPeriodsSerializer
    filterset_fields = ['tenant_id', 'period_id']

class NetSuiteDepartmentsViewSet(viewsets.ModelViewSet):
    queryset = NetSuiteDepartments.objects.all()
    serializer_class = NetSuiteDepartmentsSerializer
    filterset_fields = ['tenant_id', 'department_id']

class NetSuiteSubsidiariesViewSet(viewsets.ModelViewSet):
    queryset = NetSuiteSubsidiaries.objects.all()
    serializer_class = NetSuiteSubsidiariesSerializer
    filterset_fields = ['tenant_id', 'subsidiary_id']

class NetSuiteVendorsViewSet(viewsets.ModelViewSet):
    queryset = NetSuiteVendors.objects.all()
    serializer_class = NetSuiteVendorsSerializer
    filterset_fields = ['tenant_id', 'vendor_id']

class NetSuiteBudgetPeriodBalancesViewSet(viewsets.ModelViewSet):
    queryset = NetSuiteBudgetPeriodBalances.objects.all()
    serializer_class = NetSuiteBudgetPeriodBalancesSerializer
    filterset_fields = ['tenant_id', 'budget_id', 'account_id']

class NetSuiteEntityViewSet(viewsets.ModelViewSet):
    queryset = NetSuiteEntity.objects.all()
    serializer_class = NetSuiteEntitySerializer
    filterset_fields = ['tenant_id', 'entity_id']

class NetSuiteJournalsViewSet(viewsets.ModelViewSet):
    queryset = NetSuiteJournals.objects.all()
    serializer_class = NetSuiteJournalsSerializer
    filterset_fields = ['tenant_id', 'journal_id']

class NetSuiteTransactionAccountingLineViewSet(viewsets.ModelViewSet):
    queryset = NetSuiteTransactionAccountingLine.objects.all()
    serializer_class = NetSuiteTransactionAccountingLineSerializer
    filterset_fields = ['tenant_id', 'transaction', 'account']

class NetSuiteTransactionLineViewSet(viewsets.ModelViewSet):
    queryset = NetSuiteTransactionLine.objects.all()
    serializer_class = NetSuiteTransactionLineSerializer
    filterset_fields = ['tenant_id', 'transaction_line_id']

class NetSuiteTransformedTransactionViewSet(viewsets.ModelViewSet):
    queryset = NetSuiteTransformedTransaction.objects.all()
    serializer_class = NetSuiteTransformedTransactionSerializer
    filterset_fields = ['tenant_id', 'transactionid']

class NetSuiteBudgetsViewSet(viewsets.ModelViewSet):
    queryset = NetSuiteBudgets.objects.all()
    serializer_class = NetSuiteBudgetsSerializer
    filterset_fields = ['tenant_id', 'budget_id']

class NetSuiteLocationsViewSet(viewsets.ModelViewSet):
    queryset = NetSuiteLocations.objects.all()
    serializer_class = NetSuiteLocationsSerializer
    filterset_fields = ['tenant_id', 'location_id']
