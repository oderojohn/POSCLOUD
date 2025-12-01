from django.contrib import admin
from .models import Payment, PaymentLog

@admin.register(Payment)
class PaymentAdmin(admin.ModelAdmin):
    list_display = ['id', 'sale', 'payment_type', 'amount', 'status', 'created_at']
    list_filter = ['payment_type', 'status', 'created_at']
    search_fields = ['sale__receipt_number', 'reference_number']

@admin.register(PaymentLog)
class PaymentLogAdmin(admin.ModelAdmin):
    list_display = ['payment', 'timestamp']
    list_filter = ['timestamp']
    search_fields = ['payment__id']
