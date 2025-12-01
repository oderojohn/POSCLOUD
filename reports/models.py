from django.db import models
from django.utils import timezone
from decimal import Decimal

class Report(models.Model):
    REPORT_TYPES = [
        ('sales', 'Sales Report'),
        ('inventory', 'Inventory Report'),
        ('customer', 'Customer Report'),
        ('shift', 'Shift Report'),
        ('profit_loss', 'Profit & Loss Report'),
    ]

    report_type = models.CharField(max_length=20, choices=REPORT_TYPES)
    title = models.CharField(max_length=200)
    date_from = models.DateField()
    date_to = models.DateField()
    generated_by = models.ForeignKey('users.UserProfile', on_delete=models.SET_NULL, null=True)
    generated_at = models.DateTimeField(auto_now_add=True)
    data = models.JSONField()  # Store report data as JSON
    total_records = models.PositiveIntegerField(default=0)

    class Meta:
        ordering = ['-generated_at']

    def __str__(self):
        return f"{self.title} - {self.report_type}"

class SalesReport(models.Model):
    """Detailed sales report data"""
    date = models.DateField()
    total_sales = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    cash_sales = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    card_sales = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    mpesa_sales = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    transaction_count = models.PositiveIntegerField(default=0)
    gross_profit = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    net_profit = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    average_transaction = models.DecimalField(max_digits=10, decimal_places=2, default=0)

    class Meta:
        ordering = ['-date']
        unique_together = ['date']

class ProductSalesHistory(models.Model):
    """Product sales history and analytics"""
    product = models.ForeignKey('inventory.Product', on_delete=models.CASCADE)
    date = models.DateField()
    quantity_sold = models.PositiveIntegerField(default=0)
    revenue = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    cost_of_goods = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    gross_profit = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    net_profit = models.DecimalField(max_digits=12, decimal_places=2, default=0)

    class Meta:
        ordering = ['-date']
        unique_together = ['product', 'date']

class CustomerAnalytics(models.Model):
    """Customer analytics and insights"""
    customer = models.ForeignKey('customers.Customer', on_delete=models.CASCADE)
    total_purchases = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    total_orders = models.PositiveIntegerField(default=0)
    average_order_value = models.DecimalField(max_digits=10, decimal_places=2, default=0)
    last_purchase_date = models.DateTimeField(null=True, blank=True)
    loyalty_points_earned = models.PositiveIntegerField(default=0)
    loyalty_points_redeemed = models.PositiveIntegerField(default=0)
    customer_lifetime_value = models.DecimalField(max_digits=12, decimal_places=2, default=0)

    class Meta:
        ordering = ['-total_purchases']

class InventoryAnalytics(models.Model):
    """Inventory analytics and insights"""
    product = models.ForeignKey('inventory.Product', on_delete=models.CASCADE)
    date = models.DateField(null=True, blank=True)
    current_stock = models.PositiveIntegerField(default=0)
    stock_value = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    sold_today = models.PositiveIntegerField(default=0)
    received_today = models.PositiveIntegerField(default=0)
    average_daily_sales = models.DecimalField(max_digits=10, decimal_places=2, default=0)
    days_of_stock_remaining = models.DecimalField(max_digits=10, decimal_places=2, default=0)
    turnover_rate = models.DecimalField(max_digits=5, decimal_places=2, default=0)
    stock_status = models.CharField(max_length=20, choices=[
        ('in_stock', 'In Stock'),
        ('low_stock', 'Low Stock'),
        ('out_of_stock', 'Out of Stock'),
        ('overstock', 'Overstock')
    ], default='in_stock')

    class Meta:
        ordering = ['-date']
        unique_together = ['product', 'date']

class ShiftAnalytics(models.Model):
    """Shift performance analytics"""
    shift = models.ForeignKey('shifts.Shift', on_delete=models.CASCADE)
    total_sales = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    transaction_count = models.PositiveIntegerField(default=0)
    average_transaction_value = models.DecimalField(max_digits=10, decimal_places=2, default=0)
    cash_handled = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    discrepancy_amount = models.DecimalField(max_digits=10, decimal_places=2, default=0)
    shift_duration_hours = models.DecimalField(max_digits=5, decimal_places=2, default=0)
    sales_per_hour = models.DecimalField(max_digits=10, decimal_places=2, default=0)

class ProfitLossReport(models.Model):
    """Profit & Loss report data"""
    date = models.DateField()
    total_revenue = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    cost_of_goods_sold = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    gross_profit = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    operating_expenses = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    net_profit = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    profit_margin_percentage = models.DecimalField(max_digits=5, decimal_places=2, default=0)

    class Meta:
        ordering = ['-date']
        unique_together = ['date']
