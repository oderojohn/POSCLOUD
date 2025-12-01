from rest_framework import serializers
from .models import (
    Report, SalesReport, ProductSalesHistory, CustomerAnalytics,
    InventoryAnalytics, ShiftAnalytics, ProfitLossReport
)

class ReportSerializer(serializers.ModelSerializer):
    generated_by_name = serializers.CharField(source='generated_by.user.username', read_only=True)

    class Meta:
        model = Report
        fields = '__all__'
        read_only_fields = ('generated_at', 'generated_by')

class SalesReportSerializer(serializers.ModelSerializer):
    class Meta:
        model = SalesReport
        fields = '__all__'

class ProductSalesHistorySerializer(serializers.ModelSerializer):
    product_name = serializers.CharField(source='product.name', read_only=True)
    product_sku = serializers.CharField(source='product.sku', read_only=True)
    category_name = serializers.CharField(source='product.category.name', read_only=True)

    class Meta:
        model = ProductSalesHistory
        fields = '__all__'

class CustomerAnalyticsSerializer(serializers.ModelSerializer):
    customer_name = serializers.CharField(source='customer.name', read_only=True)
    customer_phone = serializers.CharField(source='customer.phone', read_only=True)
    customer_email = serializers.CharField(source='customer.email', read_only=True)

    class Meta:
        model = CustomerAnalytics
        fields = '__all__'

class InventoryAnalyticsSerializer(serializers.ModelSerializer):
    product_name = serializers.CharField(source='product.name', read_only=True)
    product_sku = serializers.CharField(source='product.sku', read_only=True)
    category_name = serializers.CharField(source='product.category.name', read_only=True)

    class Meta:
        model = InventoryAnalytics
        fields = '__all__'

class ShiftAnalyticsSerializer(serializers.ModelSerializer):
    shift_date = serializers.DateField(source='shift.start_time.date', read_only=True)
    cashier_name = serializers.CharField(source='shift.cashier.user.username', read_only=True)
    shift_start = serializers.DateTimeField(source='shift.start_time', read_only=True)
    shift_end = serializers.DateTimeField(source='shift.end_time', read_only=True)

    class Meta:
        model = ShiftAnalytics
        fields = '__all__'

class ProfitLossReportSerializer(serializers.ModelSerializer):
    class Meta:
        model = ProfitLossReport
        fields = '__all__'

# Summary serializers for dashboard
class SalesSummarySerializer(serializers.Serializer):
    today_sales = serializers.DecimalField(max_digits=12, decimal_places=2)
    total_sales = serializers.DecimalField(max_digits=12, decimal_places=2)
    sales_data = serializers.ListField()
    payment_methods = serializers.ListField()
    top_products = serializers.ListField()
    recent_transactions = serializers.ListField()

class InventorySummarySerializer(serializers.Serializer):
    total_items = serializers.IntegerField()
    total_value = serializers.DecimalField(max_digits=12, decimal_places=2)
    low_stock_items = serializers.IntegerField()
    out_of_stock_items = serializers.IntegerField()
    stock_movement_data = serializers.ListField()

class CustomerSummarySerializer(serializers.Serializer):
    total_customers = serializers.IntegerField()
    active_customers = serializers.IntegerField()
    new_customers_today = serializers.IntegerField()
    top_customers = serializers.ListField()

class ShiftSummarySerializer(serializers.Serializer):
    active_shifts = serializers.IntegerField()
    completed_shifts_today = serializers.IntegerField()
    total_shift_sales = serializers.DecimalField(max_digits=12, decimal_places=2)