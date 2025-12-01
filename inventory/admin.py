from django.contrib import admin
from .models import Category, Product, Batch, StockMovement

@admin.register(Category)
class CategoryAdmin(admin.ModelAdmin):
    list_display = ['name', 'description']
    search_fields = ['name']

@admin.register(Product)
class ProductAdmin(admin.ModelAdmin):
    list_display = ['sku', 'name', 'category', 'cost_price', 'selling_price', 'wholesale_price', 'wholesale_min_qty', 'stock_quantity', 'is_low_stock', 'is_active']
    list_filter = ['category', 'is_active']
    search_fields = ['sku', 'name']
    fieldsets = (
        ('Basic Information', {
            'fields': ('sku', 'name', 'description', 'category', 'serial_number', 'barcode')
        }),
        ('Pricing', {
            'fields': ('cost_price', 'selling_price', 'wholesale_price', 'wholesale_min_qty')
        }),
        ('Inventory', {
            'fields': ('stock_quantity', 'low_stock_threshold', 'is_active')
        }),
    )

@admin.register(Batch)
class BatchAdmin(admin.ModelAdmin):
    list_display = ['product', 'batch_number', 'quantity', 'supplier', 'purchase_date']
    list_filter = ['supplier', 'purchase_date']
    search_fields = ['batch_number', 'product__name']

@admin.register(StockMovement)
class StockMovementAdmin(admin.ModelAdmin):
    list_display = ['product', 'movement_type', 'quantity', 'created_at', 'user']
    list_filter = ['movement_type', 'created_at']
    search_fields = ['product__name']
