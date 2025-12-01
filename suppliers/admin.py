from django.contrib import admin
from .models import Supplier, SupplierPriceHistory, PurchaseOrder, PurchaseOrderItem

@admin.register(Supplier)
class SupplierAdmin(admin.ModelAdmin):
    list_display = ['name', 'contact_person', 'phone', 'email', 'is_active']
    list_filter = ['is_active']
    search_fields = ['name', 'phone', 'email']

@admin.register(SupplierPriceHistory)
class SupplierPriceHistoryAdmin(admin.ModelAdmin):
    list_display = ['supplier', 'product', 'price', 'date']
    list_filter = ['date']
    search_fields = ['supplier__name', 'product__name']

@admin.register(PurchaseOrder)
class PurchaseOrderAdmin(admin.ModelAdmin):
    list_display = ['id', 'supplier', 'order_date', 'status', 'total_amount']
    list_filter = ['status', 'order_date']
    search_fields = ['supplier__name']

@admin.register(PurchaseOrderItem)
class PurchaseOrderItemAdmin(admin.ModelAdmin):
    list_display = ['purchase_order', 'product', 'quantity', 'unit_price']
    search_fields = ['product__name']
