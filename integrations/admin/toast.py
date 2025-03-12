from django.contrib import admin
from integrations.models.toast.raw import (
    ToastOrder,
    ToastCheck,
    ToastGeneralLocation,
    ToastDaySchedule,
    ToastWeeklySchedule,
    ToastJoinedOpeningHours,
    ToastSelection,
)

class ToastSelectionAdmin(admin.ModelAdmin):
    list_display = ('display_name', 'pre_discount_price', 'discount_total', 'void_business_date', 'quantity', 'voided')
    search_fields = ( 'display_name',)

class ToastCheckAdmin(admin.ModelAdmin):
    list_display = ('check_guid', 'net_sales', 'service_charge_total', 'discount_total', 'opened_date', 'closed_date')
    search_fields = ('check_guid',)
    ordering = ('-opened_date',)

class ToastOrderAdmin(admin.ModelAdmin):
    list_display = ('order_guid', 'order_net_sales', 'import_id', 'opened_date','total_refunds')
    search_fields = ('order_guid',)
    ordering = ('-opened_date',)

class ToastGeneralLocationAdmin(admin.ModelAdmin):
    list_display = ('general_name', 'location_name', 'guid', 'archived')
    search_fields = ('general_name', 'location_name', 'guid')
    ordering = ('general_name',)

class ToastDayScheduleAdmin(admin.ModelAdmin):
    list_display = ('guid', 'property_name', 'open_time', 'close_time', 'restaurant')
    search_fields = ('guid', 'property_name')
    ordering = ('guid',)

class ToastWeeklyScheduleAdmin(admin.ModelAdmin):
    list_display = ('restaurant',)
    search_fields = ('restaurant__general_name',)
    ordering = ('restaurant__general_name',)

class ToastJoinedOpeningHoursAdmin(admin.ModelAdmin):
    list_display = ('restaurant', 'monday_start_time', 'monday_end_time')
    search_fields = ('restaurant__general_name',)
    ordering = ('restaurant__general_name',)

admin.site.register(ToastOrder, ToastOrderAdmin)
admin.site.register(ToastCheck, ToastCheckAdmin)
admin.site.register(ToastGeneralLocation, ToastGeneralLocationAdmin)
admin.site.register(ToastDaySchedule, ToastDayScheduleAdmin)
admin.site.register(ToastWeeklySchedule, ToastWeeklyScheduleAdmin)
admin.site.register(ToastJoinedOpeningHours, ToastJoinedOpeningHoursAdmin)
admin.site.register(ToastSelection, ToastSelectionAdmin)

