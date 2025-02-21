from rest_framework import serializers
from integrations.models.toast.raw import (
    ToastOrder,
    ToastGeneralLocation,
    ToastDaySchedule,
    ToastWeeklySchedule,
    ToastJoinedOpeningHours,
)

class ToastOrderSerializer(serializers.ModelSerializer):
    class Meta:
        model = ToastOrder
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
