from django.contrib import admin
from .models import Shift

@admin.register(Shift)
class ShiftAdmin(admin.ModelAdmin):
    list_display = ['id', 'cashier', 'start_time', 'end_time', 'status', 'opening_balance', 'closing_balance', 'total_sales', 'discrepancy']
    list_filter = ['status', 'start_time']
    search_fields = ['cashier__user__username']
