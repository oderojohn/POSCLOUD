from django.contrib import admin
from .models import Preorder, PreorderPayment

@admin.register(Preorder)
class PreorderAdmin(admin.ModelAdmin):
    list_display = ['id', 'customer', 'product', 'quantity', 'status', 'deposit_amount', 'outstanding_balance', 'preorder_date']
    list_filter = ['status', 'preorder_date']
    search_fields = ['customer__name', 'product__name']

@admin.register(PreorderPayment)
class PreorderPaymentAdmin(admin.ModelAdmin):
    list_display = ['preorder', 'amount', 'payment_date', 'reference']
    list_filter = ['payment_date']
    search_fields = ['preorder__id', 'reference']
