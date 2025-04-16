from rest_framework import serializers
from integrations.models.toast.raw import (
    ToastOrder, ToastCheck, ToastSelection, ToastGeneralLocation,
    ToastDaySchedule, ToastWeeklySchedule, ToastJoinedOpeningHours,
    ToastRevenueCenter, ToastRestaurantService, ToastSalesCategory,
    ToastDiningOption, ToastServiceArea, ToastPayment
)

class ToastOrderSerializer(serializers.ModelSerializer):
    class Meta:
        model = ToastOrder
        fields = '__all__'

class ToastCheckSerializer(serializers.ModelSerializer):
    class Meta:
        model = ToastCheck
        fields = '__all__'

class ToastSelectionSerializer(serializers.ModelSerializer):
    class Meta:
        model = ToastSelection
        fields = '__all__'

class ToastGeneralLocationSerializer(serializers.ModelSerializer):
    class Meta:
        model = ToastGeneralLocation
        fields = '__all__'

class ToastDayScheduleSerializer(serializers.ModelSerializer):
    class Meta:
        model = ToastDaySchedule
        fields = '__all__'

class ToastWeeklyScheduleSerializer(serializers.ModelSerializer):
    class Meta:
        model = ToastWeeklySchedule
        fields = '__all__'

class ToastJoinedOpeningHoursSerializer(serializers.ModelSerializer):
    class Meta:
        model = ToastJoinedOpeningHours
        fields = '__all__'

class ToastRevenueCenterSerializer(serializers.ModelSerializer):
    class Meta:
        model = ToastRevenueCenter
        fields = '__all__'

class ToastRestaurantServiceSerializer(serializers.ModelSerializer):
    class Meta:
        model = ToastRestaurantService
        fields = '__all__'

class ToastSalesCategorySerializer(serializers.ModelSerializer):
    class Meta:
        model = ToastSalesCategory
        fields = '__all__'

class ToastDiningOptionSerializer(serializers.ModelSerializer):
    class Meta:
        model = ToastDiningOption
        fields = '__all__'

class ToastServiceAreaSerializer(serializers.ModelSerializer):
    class Meta:
        model = ToastServiceArea
        fields = '__all__'

class ToastPaymentSerializer(serializers.ModelSerializer):
    class Meta:
        model = ToastPayment
        fields = '__all__'
