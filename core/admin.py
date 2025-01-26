from django.contrib import admin
from .models import Organisation


class OrganisationAdmin(admin.ModelAdmin):
    list_display = ('name',)
    search_fields = ('name',)
    
    
admin.site.register(Organisation, OrganisationAdmin)