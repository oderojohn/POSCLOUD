from django.contrib import admin
from .models import Repair, RepairPart

@admin.register(Repair)
class RepairAdmin(admin.ModelAdmin):
    list_display = ['id', 'customer', 'device_model', 'status', 'estimated_cost', 'actual_cost', 'technician', 'created_at']
    list_filter = ['status', 'created_at']
    search_fields = ['customer__name', 'device_model']

@admin.register(RepairPart)
class RepairPartAdmin(admin.ModelAdmin):
    list_display = ['repair', 'product', 'quantity', 'unit_cost']
    search_fields = ['product__name']
