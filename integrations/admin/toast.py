from django.contrib import admin
from integrations.models.toast.raw import (
    ToastOrder,
    ToastGeneralLocation,
    ToastDaySchedule,
    ToastWeeklySchedule,
    ToastJoinedOpeningHours,
)

@admin.register(ToastOrder)
class ToastOrderAdmin(admin.ModelAdmin):
    list_display = ('order_guid', 'order_net_sales', 'import_id', 'ws_import_date')
    search_fields = ('order_guid',)
    ordering = ('-ws_import_date',)

@admin.register(ToastGeneralLocation)
class ToastGeneralLocationAdmin(admin.ModelAdmin):
    list_display = ('general_name', 'location_name', 'guid', 'archived')
    search_fields = ('general_name', 'location_name', 'guid')
    ordering = ('general_name',)

@admin.register(ToastDaySchedule)
class ToastDayScheduleAdmin(admin.ModelAdmin):
    list_display = ('guid', 'property_name', 'open_time', 'close_time', 'restaurant')
    search_fields = ('guid', 'property_name')
    ordering = ('guid',)

@admin.register(ToastWeeklySchedule)
class ToastWeeklyScheduleAdmin(admin.ModelAdmin):
    list_display = ('restaurant',)
    search_fields = ('restaurant__general_name',)
    ordering = ('restaurant__general_name',)

@admin.register(ToastJoinedOpeningHours)
class ToastJoinedOpeningHoursAdmin(admin.ModelAdmin):
    list_display = ('restaurant', 'monday_start_time', 'monday_end_time')
    search_fields = ('restaurant__general_name',)
    ordering = ('restaurant__general_name',)
