
from rest_framework import serializers
from integrations.models.xero.transformations import (
    XeroJournalLines,
    XeroJournalLineTrackingCategories,
    XeroInvoiceLineItems,
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
