from django.contrib import admin
from .models import Customer, LoyaltyTransaction

@admin.register(Customer)
class CustomerAdmin(admin.ModelAdmin):
    list_display = ['name', 'phone', 'email', 'loyalty_points', 'is_active']
    list_filter = ['is_active']
    search_fields = ['name', 'phone', 'email']

@admin.register(LoyaltyTransaction)
class LoyaltyTransactionAdmin(admin.ModelAdmin):
    list_display = ['customer', 'transaction_type', 'points', 'reason', 'created_at']
    list_filter = ['transaction_type', 'created_at']
    search_fields = ['customer__name', 'reason']
