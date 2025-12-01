from django.core.management.base import BaseCommand
from django.utils import timezone
from django.db.models import Sum, Count, F
from datetime import timedelta
from decimal import Decimal
from reports.models import SalesReport, ProductSalesHistory, InventoryAnalytics
from sales.models import Sale, SaleItem, Payment
from inventory.models import Product, StockMovement

class Command(BaseCommand):
    help = 'Populate daily sales and inventory reports for the last 30 days'

    def handle(self, *args, **options):
        self.stdout.write('Starting daily reports population...')

        # Get date range (last 30 days)
        end_date = timezone.now().date()
        start_date = end_date - timedelta(days=30)

        current_date = start_date
        while current_date <= end_date:
            self._populate_sales_report(current_date)
            self._populate_inventory_analytics(current_date)
            self._populate_product_sales_history(current_date)

            current_date += timedelta(days=1)

        self.stdout.write(
            self.style.SUCCESS('Successfully populated daily reports for the last 30 days')
        )

    def _populate_sales_report(self, date):
        """Populate sales report for a specific date"""
        # Get sales data for the date
        sales = Sale.objects.filter(sale_date__date=date)
        total_sales = sales.aggregate(total=Sum('final_amount'))['total'] or Decimal('0')
        transaction_count = sales.count()

        # Get payment method breakdown
        payments = Payment.objects.filter(
            sale__sale_date__date=date,
            status='completed'
        ).values('payment_type').annotate(amount=Sum('amount'))

        payment_breakdown = {p['payment_type']: p['amount'] for p in payments}

        # Calculate average transaction
        average_transaction = total_sales / transaction_count if transaction_count > 0 else Decimal('0')

        # Create or update sales report
        SalesReport.objects.update_or_create(
            date=date,
            defaults={
                'total_sales': total_sales,
                'cash_sales': payment_breakdown.get('cash', Decimal('0')),
                'card_sales': payment_breakdown.get('card', Decimal('0')),
                'mpesa_sales': payment_breakdown.get('mpesa', Decimal('0')),
                'transaction_count': transaction_count,
                'gross_profit': total_sales * Decimal('0.25'),  # Estimated 25% margin
                'net_profit': total_sales * Decimal('0.20'),    # Estimated 20% net profit
                'average_transaction': average_transaction,
            }
        )

    def _populate_inventory_analytics(self, date):
        """Populate inventory analytics for a specific date"""
        products = Product.objects.all()

        for product in products:
            # Calculate stock sold today
            sold_today = SaleItem.objects.filter(
                product=product,
                sale__sale_date__date=date
            ).aggregate(total=Sum('quantity'))['total'] or 0

            # Calculate stock received today
            received_today = StockMovement.objects.filter(
                product=product,
                movement_type='in',
                created_at__date=date
            ).aggregate(total=Sum('quantity'))['total'] or 0

            # Calculate stock status
            if product.stock_quantity == 0:
                status = 'out_of_stock'
            elif product.stock_quantity <= (product.low_stock_threshold or 10):
                status = 'low_stock'
            elif product.stock_quantity > (product.low_stock_threshold or 10) * 3:
                status = 'overstock'
            else:
                status = 'in_stock'

            # Calculate stock value
            stock_value = product.stock_quantity * product.cost_price

            # Create or update inventory analytics
            InventoryAnalytics.objects.update_or_create(
                product=product,
                date=date,
                defaults={
                    'current_stock': product.stock_quantity,
                    'stock_value': stock_value,
                    'sold_today': sold_today,
                    'received_today': received_today,
                    'stock_status': status,
                    # These would be calculated based on historical data
                    'average_daily_sales': Decimal('0'),  # TODO: Calculate from last 30 days
                    'days_of_stock_remaining': Decimal('0'),  # TODO: Calculate based on average sales
                    'turnover_rate': Decimal('0'),  # TODO: Calculate annual turnover
                }
            )

    def _populate_product_sales_history(self, date):
        """Populate product sales history for a specific date"""
        # Get products sold on this date
        product_sales = SaleItem.objects.filter(
            sale__sale_date__date=date
        ).values('product').annotate(
            quantity_sold=Sum('quantity'),
            revenue=Sum(F('unit_price') * F('quantity'))
        ).values(
            'product',
            'quantity_sold',
            'revenue'
        )

        for sale_data in product_sales:
            product_id = sale_data['product']
            quantity_sold = sale_data['quantity_sold']
            revenue = sale_data['revenue']

            # Get product for cost calculation
            try:
                product = Product.objects.get(id=product_id)
                cost_price = product.cost_price
                cost_of_goods = quantity_sold * cost_price
                gross_profit = revenue - cost_of_goods
                net_profit = gross_profit  # Simplified, no additional costs considered

                # Create or update product sales history
                ProductSalesHistory.objects.update_or_create(
                    product_id=product_id,
                    date=date,
                    defaults={
                        'quantity_sold': quantity_sold,
                        'revenue': revenue,
                        'cost_of_goods': cost_of_goods,
                        'gross_profit': gross_profit,
                        'net_profit': net_profit,
                    }
                )
            except Product.DoesNotExist:
                continue