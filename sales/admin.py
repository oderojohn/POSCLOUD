from django.contrib import admin
from .models import Cart, CartItem, Sale, SaleItem

@admin.register(Cart)
class CartAdmin(admin.ModelAdmin):
    list_display = ['id', 'customer', 'status', 'created_at', 'cashier']
    list_filter = ['status', 'created_at']
    search_fields = ['customer__name']

@admin.register(CartItem)
class CartItemAdmin(admin.ModelAdmin):
    list_display = ['cart', 'product', 'quantity', 'unit_price', 'discount']
    search_fields = ['product__name']

@admin.register(Sale)
class SaleAdmin(admin.ModelAdmin):
    list_display = ['receipt_number', 'cart', 'total_amount', 'final_amount', 'sale_date']
    list_filter = ['sale_date']
    search_fields = ['receipt_number']

@admin.register(SaleItem)
class SaleItemAdmin(admin.ModelAdmin):
    list_display = ['sale', 'product', 'quantity', 'unit_price', 'discount']
    search_fields = ['product__name']
