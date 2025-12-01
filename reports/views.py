from rest_framework import viewsets, status, generics
from rest_framework.response import Response
from rest_framework.decorators import action
from django.db.models import Sum, Count, Avg, F, Q
from django.db.models.functions import TruncDate, ExtractHour
from django.utils import timezone
from datetime import timedelta, datetime
from decimal import Decimal
from django.http import HttpResponse
from django.conf import settings
from django.templatetags.static import static
import os
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter, A4
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from io import BytesIO
from inventory.models import Product
from .models import (
    Report, SalesReport, ProductSalesHistory, CustomerAnalytics,
    InventoryAnalytics, ShiftAnalytics, ProfitLossReport
)
from .serializers import (
    ReportSerializer, SalesReportSerializer, ProductSalesHistorySerializer,
    CustomerAnalyticsSerializer, InventoryAnalyticsSerializer,
    ShiftAnalyticsSerializer, ProfitLossReportSerializer,
    SalesSummarySerializer, InventorySummarySerializer,
    CustomerSummarySerializer, ShiftSummarySerializer
)

class ReportViewSet(viewsets.ModelViewSet):
    queryset = Report.objects.all()
    serializer_class = ReportSerializer

    @action(detail=False, methods=['post'])
    def generate_sales_report(self, request):
        """Generate sales report for date range"""
        date_from = request.data.get('date_from')
        date_to = request.data.get('date_to')

        if not date_from or not date_to:
            return Response({'error': 'Date range is required'}, status=status.HTTP_400_BAD_REQUEST)

        # Get sales data
        sales_data = self._get_sales_data(date_from, date_to)

        # Create report record
        report = Report.objects.create(
            report_type='sales',
            title=f'Sales Report ({date_from} to {date_to})',
            date_from=date_from,
            date_to=date_to,
            generated_by=request.user.userprofile if hasattr(request.user, 'userprofile') else None,
            data=sales_data,
            total_records=len(sales_data)
        )

        # Return the actual report data, not the Report object
        return Response(sales_data, status=status.HTTP_200_OK)

    @action(detail=False, methods=['post'])
    def generate_inventory_report(self, request):
        """Generate inventory report"""
        date = request.data.get('date')
        if date:
            try:
                date = datetime.strptime(date, '%Y-%m-%d').date()
            except ValueError:
                return Response({'error': 'Invalid date format. Use YYYY-MM-DD'}, status=status.HTTP_400_BAD_REQUEST)

        inventory_data = self._get_inventory_data(date)

        # Return the actual inventory data
        return Response(inventory_data, status=status.HTTP_200_OK)

    @action(detail=False, methods=['post'])
    def generate_customer_report(self, request):
        """Generate customer analytics report"""
        customer_data = self._get_customer_data()

        # Return the actual customer data
        return Response(customer_data, status=status.HTTP_200_OK)

    @action(detail=False, methods=['post'])
    def generate_profit_loss_report(self, request):
        """Generate profit & loss report"""
        date_from = request.data.get('date_from')
        date_to = request.data.get('date_to')

        if not date_from or not date_to:
            return Response({'error': 'Date range is required'}, status=status.HTTP_400_BAD_REQUEST)

        pl_data = self._calculate_profit_loss(date_from, date_to)

        # Return the actual profit & loss data
        return Response(pl_data, status=status.HTTP_200_OK)

    def _get_sales_data(self, date_from, date_to):
        """Get sales data for the specified date range"""
        from sales.models import Sale, Payment
        from django.db.models import Case, When, DecimalField

        sales = Sale.objects.filter(
            sale_date__date__range=[date_from, date_to],
            voided=False
        ).annotate(
            date=TruncDate('sale_date')
        ).values('date').annotate(
            total_sales=Sum('final_amount'),
            transaction_count=Count('id')
        ).order_by('date')

        # Get payment method breakdown per day
        payments_per_day = Payment.objects.filter(
            sale__sale_date__date__range=[date_from, date_to],
            sale__voided=False,
            status='completed'
        ).annotate(
            date=TruncDate('sale__sale_date')
        ).values('date', 'payment_type').annotate(
            amount=Sum('amount')
        ).order_by('date')

        # Group payments by date
        payments_by_date = {}
        for payment in payments_per_day:
            date_key = payment['date'].strftime('%Y-%m-%d')
            if date_key not in payments_by_date:
                payments_by_date[date_key] = {}
            payments_by_date[date_key][payment['payment_type']] = float(payment['amount'])

        result = []
        for sale in sales:
            date_str = sale['date'].strftime('%Y-%m-%d')
            day_payments = payments_by_date.get(date_str, {})
            result.append({
                'date': date_str,
                'total_sales': float(sale['total_sales']),
                'cash_sales': float(day_payments.get('cash', 0)),
                'card_sales': float(day_payments.get('card', 0)),
                'mobile_sales': float(day_payments.get('mpesa', 0)),  # Frontend expects 'mobile_sales'
                'transactions': sale['transaction_count'],  # Frontend expects 'transactions'
                'gross_profit': float(sale['total_sales'] * Decimal('0.25')),  # Estimated 25% margin
                'net_profit': float(sale['total_sales'] * Decimal('0.20'))  # Estimated 20% net profit
            })

        return result

    def _get_daily_sales_summary(self, date=None):
        """Get comprehensive daily sales summary"""
        from sales.models import Sale, SaleItem, Payment
        from django.db.models import Sum, Count

        if date is None:
            date = timezone.now().date()

        # Total sales for the day
        total_sales = Sale.objects.filter(
            sale_date__date=date
        ).aggregate(
            total_amount=Sum('final_amount'),
            transaction_count=Count('id')
        )

        # Payment method breakdown
        payments = Payment.objects.filter(
            sale__sale_date__date=date,
            status='completed'
        ).values('payment_type').annotate(
            amount=Sum('amount'),
            count=Count('id')
        )

        payment_breakdown = {}
        for payment in payments:
            payment_breakdown[payment['payment_type']] = {
                'amount': float(payment['amount']),
                'count': payment['count']
            }

        # Top products sold today
        top_products = SaleItem.objects.filter(
            sale__sale_date__date=date
        ).values(
            'product__name',
            'product__sku'
        ).annotate(
            quantity_sold=Sum('quantity'),
            revenue=Sum(F('unit_price') * F('quantity'))
        ).order_by('-quantity_sold')[:10]

        # Hourly sales breakdown
        hourly_sales = Sale.objects.filter(
            sale_date__date=date
        ).annotate(
            hour=ExtractHour('sale_date')
        ).values('hour').annotate(
            amount=Sum('final_amount'),
            count=Count('id')
        ).order_by('hour')

        return {
            'date': date.strftime('%Y-%m-%d'),
            'total_sales': float(total_sales['total_amount'] or 0),
            'total_transactions': total_sales['transaction_count'] or 0,
            'average_transaction': float(total_sales['total_amount'] or 0) / max(total_sales['transaction_count'] or 1, 1),
            'payment_methods': payment_breakdown,
            'top_products': [
                {
                    'name': item['product__name'],
                    'sku': item['product__sku'],
                    'quantity_sold': item['quantity_sold'],
                    'revenue': float(item['revenue'])
                }
                for item in top_products
            ],
            'hourly_breakdown': [
                {
                    'hour': item['hour'],
                    'amount': float(item['amount']),
                    'transactions': item['count']
                }
                for item in hourly_sales
            ]
        }

    def _get_inventory_data(self, date=None):
        """Get current inventory data"""
        from inventory.models import Product, StockMovement
        from sales.models import SaleItem
        from django.db.models import Sum

        products = Product.objects.all().select_related('category')

        # Default to today if no date provided
        if date is None:
            date = timezone.now().date()

        result = []
        for product in products:
            # Calculate stock status
            if product.stock_quantity == 0:
                status = 'out_of_stock'
            elif product.stock_quantity <= (product.low_stock_threshold or 10):
                status = 'low_stock'
            elif product.stock_quantity > (product.low_stock_threshold or 10) * 3:
                status = 'overstock'
            else:
                status = 'in_stock'

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

            result.append({
                'product': product.name,
                'category': product.category.name if product.category else 'Uncategorized',
                'stock_level': product.stock_quantity,
                'sold_today': sold_today,
                'received_today': received_today,
                'value': float(product.stock_quantity * product.cost_price),
            })

        return result

    def _get_customer_data(self):
        """Get customer analytics data"""
        from customers.models import Customer
        from sales.models import Sale

        customers = Customer.objects.all()

        result = []
        for customer in customers:
            customer_sales = Sale.objects.filter(customer=customer)
            total_purchases = customer_sales.aggregate(total=Sum('final_amount'))['total'] or 0
            order_count = customer_sales.count()

            last_purchase = customer_sales.order_by('-sale_date').first()
            last_purchase_date = last_purchase.sale_date if last_purchase else None

            result.append({
                'name': customer.name,
                'phone': customer.phone,
                'total_purchases': float(total_purchases),
                'last_visit': last_purchase_date.strftime('%Y-%m-%d') if last_purchase_date else None,
                'loyalty_points': customer.loyalty_points,
            })

        return sorted(result, key=lambda x: x['total_purchases'], reverse=True)

    def _calculate_profit_loss(self, date_from, date_to):
        """Calculate profit & loss for the period using the same logic as product performance"""
        from sales.models import SaleItem
        from django.db.models import Sum

        # Calculate total revenue and cost of goods sold from SaleItem data (exclude voided sales)
        sales_data = SaleItem.objects.filter(
            sale__sale_date__date__range=[date_from, date_to],
            sale__voided=False
        ).aggregate(
            total_revenue=Sum(F('unit_price') * F('quantity')),
            total_cost=Sum(F('product__cost_price') * F('quantity'))
        )

        total_revenue = float(sales_data['total_revenue'] or 0)
        cost_of_goods_sold = float(sales_data['total_cost'] or 0)

        gross_profit = total_revenue - cost_of_goods_sold

        # Estimated operating expenses (25% of revenue)
        operating_expenses = total_revenue * 0.25

        net_profit = gross_profit - operating_expenses
        profit_margin = (net_profit / total_revenue) * 100 if total_revenue > 0 else 0

        return {
            'date_from': date_from,
            'date_to': date_to,
            'total_revenue': total_revenue,
            'cost_of_goods_sold': cost_of_goods_sold,
            'gross_profit': gross_profit,
            'operating_expenses': operating_expenses,
            'net_profit': net_profit,
            'profit_margin_percentage': profit_margin
        }

class SalesSummaryView(generics.GenericAPIView):
    """Get sales summary for dashboard and reports"""

    def _get_shift_sales_data(self, shift_id):
        """Get sales data for a specific shift"""
        from sales.models import Sale, Cart
        from payments.models import Payment
        from shifts.models import Shift

        # Get the shift to find the cashier
        shift = Shift.objects.get(id=shift_id)
        cashier = shift.cashier

        # Get sales for the shift (exclude voided sales for totals)
        sales = Sale.objects.filter(
            shift_id=shift_id,
            voided=False
        ).annotate(
            date=TruncDate('sale_date')
        ).values('date').annotate(
            total_sales=Sum('final_amount'),
            transaction_count=Count('id')
        ).order_by('date')

        # Get payment method breakdown for the shift
        payments = Payment.objects.filter(
            sale__shift_id=shift_id,
            sale__voided=False,
            status='completed'
        ).select_related('sale')

        payment_breakdown = {}
        for payment in payments:
            payment_type = payment.payment_type
            amount = float(payment.amount)
            if payment_type == 'split' and payment.split_data:
                # For split payments, add amounts to respective methods
                for method, split_amount in payment.split_data.items():
                    payment_breakdown[method] = payment_breakdown.get(method, 0) + float(split_amount)
            else:
                payment_breakdown[payment_type] = payment_breakdown.get(payment_type, 0) + amount

        # Get total sales and transactions for the shift
        total_sales = sum(float(sale['total_sales']) for sale in sales)
        total_transactions = sum(sale['transaction_count'] for sale in sales)

        # Calculate actual gross profit for the shift
        # Gross profit = sum((selling_price - cost_price) * quantity) for all items sold in shift
        from sales.models import SaleItem
        sales_items = SaleItem.objects.filter(
            sale__shift_id=shift_id,
            sale__voided=False
        ).select_related('product')

        gross_profit = 0
        for item in sales_items:
            if item.product and item.product.cost_price:
                profit_per_item = (item.unit_price - item.product.cost_price) * item.quantity
                gross_profit += float(profit_per_item)

        # Calculate cost of goods sold
        cost_of_goods_sold = sum(
            float(item.product.cost_price * item.quantity)
            for item in sales_items
            if item.product and item.product.cost_price
        )

        # Net profit (estimated as gross profit minus 5% operating costs)
        net_profit = gross_profit * 0.95

        # Get all completed sales for the shift with payment info (exclude voided)
        all_sales = Sale.objects.filter(
            shift_id=shift_id,
            voided=False
        ).select_related('customer').prefetch_related('payment_set', 'saleitem_set__product').order_by('-sale_date')

        # Get voided sales for the shift
        voided_sales = Sale.objects.filter(
            shift_id=shift_id,
            voided=True
        ).select_related('customer').prefetch_related('saleitem_set__product').order_by('-sale_date')

        # Get held orders for the cashier (during this shift period)
        held_orders = Cart.objects.filter(
            cashier=cashier,
            status='held',
            created_at__gte=shift.start_time
        ).select_related('customer').prefetch_related('cartitem_set__product').order_by('-created_at')

        result = {
            'total_sales': total_sales,
            'total_transactions': total_transactions,
            'average_sale': total_sales / total_transactions if total_transactions > 0 else 0,
            'today_sales': total_sales,  # Since it's for the shift
            'gross_profit': gross_profit,
            'net_profit': net_profit,
            'cost_of_goods_sold': cost_of_goods_sold,
            'sales_by_payment_method': payment_breakdown,
            'recent_sales': [
                {
                    'id': sale.id,
                    'customer': sale.customer.name if sale.customer else 'Walk-in',
                    'total_amount': float(sale.total_amount),
                    'receipt_number': sale.receipt_number,
                    'created_at': sale.sale_date.isoformat(),
                    'payment_method': self._determine_payment_method(sale),
                    'sale_type': sale.sale_type,
                    'mpesa_number': sale.payment_set.filter(payment_type='mpesa', status='completed').first().mpesa_number if sale.payment_set.filter(payment_type='mpesa', status='completed').exists() else None,
                    'items': [
                        {
                            'product_name': item.product.name,
                            'quantity': item.quantity,
                            'unit_price': float(item.unit_price),
                            'total': float(item.unit_price * item.quantity)
                        }
                        for item in sale.saleitem_set.all()
                    ],
                    'split_data': self._get_split_data_for_sale(sale)
                }
                for sale in all_sales
            ],
            'voided_sales': [
                {
                    'id': sale.id,
                    'customer': sale.customer.name if sale.customer else 'Walk-in',
                    'total_amount': float(sale.total_amount),
                    'receipt_number': sale.receipt_number,
                    'created_at': sale.sale_date.isoformat(),
                    'void_reason': sale.void_reason,
                    'voided_at': sale.voided_at.isoformat() if sale.voided_at else None,
                    'items': [
                        {
                            'product_name': item.product.name,
                            'quantity': item.quantity,
                            'unit_price': float(item.unit_price),
                            'total': float(item.unit_price * item.quantity)
                        }
                        for item in sale.saleitem_set.all()
                    ]
                }
                for sale in voided_sales
            ],
            'held_orders': [
                {
                    'id': cart.id,
                    'customer': cart.customer.name if cart.customer else 'Walk-in',
                    'total_amount': float(sum(item.unit_price * item.quantity for item in cart.cartitem_set.all())),
                    'created_at': cart.created_at.isoformat(),
                    'status': cart.status,
                    'void_reason': cart.void_reason,
                    'items': [
                        {
                            'product_name': item.product.name,
                            'quantity': item.quantity,
                            'unit_price': float(item.unit_price),
                            'total': float(item.unit_price * item.quantity)
                        }
                        for item in cart.cartitem_set.all()
                    ]
                }
                for cart in held_orders
            ]
        }

        return result

    def get(self, request, sale_id=None):
        # Check if this is a request for a specific sale chit
        if sale_id:
            chit_details = self._get_sale_chit_details(sale_id)
            return Response(chit_details)

        # Check if detailed transactions are requested
        if request.query_params.get('detailed_transactions'):
            date_from = request.query_params.get('date_from')
            date_to = request.query_params.get('date_to')
            shift_id = request.query_params.get('shift_id')

            if shift_id:
                detailed_transactions = self._get_detailed_transactions_for_shift(shift_id)
            elif date_from and date_to:
                detailed_transactions = self._get_detailed_transactions_for_range(date_from, date_to)
            else:
                # Default to today
                today = timezone.now().date()
                detailed_transactions = self._get_detailed_transactions_for_date(today)

            return Response(detailed_transactions)

        # Check if chit details for a specific sale are requested (legacy support)
        sale_id_param = request.query_params.get('sale_id')
        if sale_id_param:
            chit_details = self._get_sale_chit_details(sale_id_param)
            return Response(chit_details)

        # Check if products sold today is requested
        if request.query_params.get('products_today'):
            date = request.query_params.get('date')
            if date:
                try:
                    date = datetime.strptime(date, '%Y-%m-%d').date()
                except ValueError:
                    return Response({'error': 'Invalid date format. Use YYYY-MM-DD'}, status=status.HTTP_400_BAD_REQUEST)
            products_sold = self._get_products_sold_today(date)
            return Response(products_sold)

        # Check if product performance report is requested
        if request.query_params.get('product_performance'):
            date_from = request.query_params.get('date_from')
            date_to = request.query_params.get('date_to')
            if not date_from or not date_to:
                return Response({'error': 'Date range is required for product performance report'}, status=status.HTTP_400_BAD_REQUEST)
            product_performance = self._get_product_performance(date_from, date_to)
            return Response(product_performance)

        # Check if today's summary is requested
        if request.query_params.get('today_summary'):
            today_summary = self._get_today_summary()
            return Response(today_summary)

        # Check if daily sales summary is requested
        if request.query_params.get('daily_summary'):
            date = request.query_params.get('date')
            if date:
                try:
                    date = datetime.strptime(date, '%Y-%m-%d').date()
                except ValueError:
                    return Response({'error': 'Invalid date format. Use YYYY-MM-DD'}, status=status.HTTP_400_BAD_REQUEST)
            daily_summary = self._get_daily_sales_summary(date)
            return Response(daily_summary)

        # Check if shift_id is provided (for shift-specific sales summary)
        shift_id = request.query_params.get('shift_id')

        if shift_id:
            # Return sales data for the specified shift
            sales_data = self._get_shift_sales_data(shift_id)
            return Response(sales_data)

        # Check if date range is provided (for reports)
        date_from = request.query_params.get('date_from')
        date_to = request.query_params.get('date_to')

        # Check if no shift_id and no date range (when viewing sales summary for today)
        if not shift_id and not date_from and not date_to:
            # Return sales for current user's current shift
            from users.models import UserProfile
            from shifts.models import Shift

            try:
                user_profile = request.user.userprofile
                current_shift = Shift.objects.get(cashier=user_profile, status='open')
                sales_data = self._get_shift_sales_data(current_shift.id)
                return Response(sales_data)
            except (UserProfile.DoesNotExist, Shift.DoesNotExist):
                # If no current shift, return empty data
                return Response({
                    'total_sales': 0,
                    'total_transactions': 0,
                    'average_sale': 0,
                    'today_sales': 0,
                    'gross_profit': 0,
                    'net_profit': 0,
                    'cost_of_goods_sold': 0,
                    'sales_by_payment_method': {},
                    'recent_sales': []
                })

        if date_from and date_to:
            # Return historical sales data for the specified range
            sales_data = self._get_sales_data_for_range(date_from, date_to)
            return Response(sales_data)
        else:
            # Return dashboard summary data
            today = timezone.now().date()
            week_ago = today - timedelta(days=7)

            # Today's sales
            today_sales = self._get_today_sales()

            # Total sales
            total_sales = self._get_total_sales()

            # Sales data for chart (last 7 days)
            sales_data = self._get_sales_trend_data(week_ago, today)

            # Payment methods distribution
            payment_methods = self._get_payment_methods_data(today)

            # Top products
            top_products = self._get_top_products(week_ago, today)

            # Recent transactions
            recent_transactions = self._get_recent_transactions()

            data = {
                'today_sales': today_sales,
                'total_sales': total_sales,
                'sales_data': sales_data,
                'payment_methods': payment_methods,
                'top_products': top_products,
                'recent_transactions': recent_transactions
            }

            serializer = SalesSummarySerializer(data)
            return Response(serializer.data)

    def _determine_payment_method(self, sale):
        """Determine payment method based on payment records"""
        payments = list(sale.payment_set.filter(status='completed'))
        if not payments:
            return 'N/A'

        # Collect all payment methods from all payments, expanding split payments
        payment_methods = set()
        for payment in payments:
            if payment.payment_type == 'MPESA' and payment.split_data:
                for method, amount in payment.split_data.items():
                    if float(amount) > 0:
                        payment_methods.add(method)
            else:
                payment_methods.add(payment.payment_type)

        # If multiple payment methods, it's a split payment
        if len(payment_methods) > 1:
            return 'split'
        elif len(payment_methods) == 1:
            return list(payment_methods)[0]
        else:
            return 'cash'

    def _get_split_data_for_sale(self, sale):
        """Get split data for a sale, reconstructing from payment records if needed"""
        # For split payments, reconstruct split_data from payment records
        payments = list(sale.payment_set.filter(status='completed'))
        if len(payments) > 1:
            split_data = {}
            for payment in payments:
                split_data[payment.payment_type] = float(payment.amount)
            return split_data

        # Fallback to old logic for legacy split payments
        split_payment = sale.payment_set.filter(payment_type='split').first()
        if split_payment and split_payment.split_data:
            # Only return if it's truly split (both amounts > 0)
            split_data = {k: v for k, v in split_payment.split_data.items() if float(v) > 0}
            if len(split_data) > 1:
                return split_data
        return None

    def _get_today_sales(self):
        from sales.models import Sale
        today = timezone.now().date()
        today_sales = Sale.objects.filter(
            sale_date__date=today
        ).aggregate(total=Sum('final_amount'))['total'] or Decimal('0')
        return float(today_sales)

    def _get_total_sales(self):
        from sales.models import Sale
        total_sales = Sale.objects.aggregate(total=Sum('final_amount'))['total'] or Decimal('0')
        return float(total_sales)

    def _get_sales_trend_data(self, start_date, end_date):
        from sales.models import Sale
        from django.db.models.functions import TruncDate

        sales_trend = Sale.objects.filter(
            sale_date__date__range=[start_date, end_date]
        ).annotate(
            date=TruncDate('sale_date')
        ).values('date').annotate(
            amount=Sum('final_amount')
        ).order_by('date')

        result = [
            {
                'date': item['date'].strftime('%Y-%m-%d'),
                'amount': float(item['amount'] or 0)
            }
            for item in sales_trend
        ]

        # If no data, return empty array (frontend will handle fallback)
        return result

    def _get_payment_methods_data(self, date):
        from payments.models import Payment

        payments = Payment.objects.filter(
            created_at__date=date,
            status='completed'
        )

        payment_totals = {}
        total_amount = 0
        for payment in payments:
            payment_type = payment.payment_type
            amount = float(payment.amount)
            if payment_type == 'split' and payment.split_data:
                # For split payments, add amounts to respective methods
                for method, split_amount in payment.split_data.items():
                    split_amt = float(split_amount)
                    payment_totals[method] = payment_totals.get(method, 0) + split_amt
                    total_amount += split_amt
            else:
                payment_totals[payment_type] = payment_totals.get(payment_type, 0) + amount
                total_amount += amount

        return [
            {
                'method': method,
                'amount': amount,
                'percentage': (amount / total_amount * 100) if total_amount > 0 else 0
            }
            for method, amount in payment_totals.items()
        ]

    def _get_top_products(self, start_date, end_date):
        from sales.models import SaleItem
        from django.db.models import Sum

        top_products = SaleItem.objects.filter(
            sale__sale_date__date__range=[start_date, end_date]
        ).values(
            'product__name'
        ).annotate(
            sold=Sum('quantity'),
            revenue=Sum(F('unit_price') * F('quantity'))
        ).order_by('-sold')[:5]

        return [
            {
                'name': item['product__name'],
                'sold': item['sold'],
                'revenue': float(item['revenue'])
            }
            for item in top_products
        ]

    def _get_products_sold_today(self, date=None):
        """Get products sold on a specific date"""
        from sales.models import SaleItem
        from django.db.models import Sum

        if date is None:
            date = timezone.now().date()

        products_sold = SaleItem.objects.filter(
            sale__sale_date__date=date
        ).values(
            'product__name',
            'product__sku'
        ).annotate(
            quantity_sold=Sum('quantity'),
            revenue=Sum(F('unit_price') * F('quantity'))
        ).order_by('-quantity_sold')

        return [
            {
                'product_name': item['product__name'],
                'sku': item['product__sku'],
                'quantity_sold': item['quantity_sold'],
                'revenue': float(item['revenue'])
            }
            for item in products_sold
        ]

    def _get_today_summary(self):
        """Get comprehensive today's business summary"""
        today = timezone.now().date()

        # Today's sales summary
        today_sales = self._get_today_sales()

        # Today's transactions count
        from sales.models import Sale
        today_transactions = Sale.objects.filter(
            sale_date__date=today
        ).count()

        # Today's profit (estimated)
        today_profit = float(today_sales) * 0.20  # 20% estimated profit margin

        # Products sold today
        products_today = self._get_products_sold_today(today)

        # Inventory alerts
        from inventory.models import Product
        low_stock_count = Product.objects.filter(stock_quantity__lte=10).count()
        out_of_stock_count = Product.objects.filter(stock_quantity=0).count()

        # Today's stock movements
        from inventory.models import StockMovement
        stock_received_today = StockMovement.objects.filter(
            created_at__date=today,
            movement_type='in'
        ).aggregate(total=Sum('quantity'))['total'] or 0

        stock_sold_today = sum(product['quantity_sold'] for product in products_today)

        return {
            'date': today.strftime('%Y-%m-%d'),
            'sales_today': float(today_sales),
            'transactions_today': today_transactions,
            'profit_today': float(today_profit),
            'products_sold_today': len(products_today),
            'top_products_today': products_today[:5],  # Top 5 products
            'inventory_alerts': {
                'low_stock': low_stock_count,
                'out_of_stock': out_of_stock_count
            },
            'stock_movements': {
                'received_today': stock_received_today,
                'sold_today': stock_sold_today
            }
        }

    def _get_recent_transactions(self):
        from sales.models import Sale

        recent_sales = Sale.objects.select_related('customer').order_by('-sale_date')[:5]

        return [
            {
                'id': sale.id,
                'customer': sale.customer.name if sale.customer else 'Walk-in',
                'amount': float(sale.final_amount),
                'time': sale.sale_date.strftime('%H:%M')
            }
            for sale in recent_sales
        ]

    def _get_sales_data_for_range(self, date_from, date_to):
        """Get sales data for the specified date range (for reports)"""
        from sales.models import Sale, SaleItem
        from payments.models import Payment

        sales = Sale.objects.filter(
            sale_date__date__range=[date_from, date_to],
            voided=False
        ).annotate(
            date=TruncDate('sale_date')
        ).values('date').annotate(
            total_sales=Sum('final_amount'),
            transaction_count=Count('id')
        ).order_by('date')

        # Get payment method breakdown per day
        payments_per_day = Payment.objects.filter(
            sale__sale_date__date__range=[date_from, date_to],
            sale__voided=False,
            status='completed'
        ).annotate(
            date=TruncDate('sale__sale_date')
        ).values('date', 'payment_type').annotate(
            amount=Sum('amount')
        ).order_by('date')

        # Group payments by date
        payments_by_date = {}
        for payment in payments_per_day:
            date_key = payment['date'].strftime('%Y-%m-%d')
            if date_key not in payments_by_date:
                payments_by_date[date_key] = {}
            payments_by_date[date_key][payment['payment_type']] = float(payment['amount'])

        result = []
        for sale in sales:
            date_str = sale['date'].strftime('%Y-%m-%d')
            day_payments = payments_by_date.get(date_str, {})
            # Calculate actual gross profit for the day
            sales_items_for_day = SaleItem.objects.filter(
                sale__sale_date__date=sale['date'],
                sale__voided=False
            ).select_related('product')

            day_gross_profit = 0
            day_cost_of_goods_sold = 0
            for item in sales_items_for_day:
                if item.product and item.product.cost_price:
                    profit_per_item = (item.unit_price - item.product.cost_price) * item.quantity
                    day_gross_profit += float(profit_per_item)
                    day_cost_of_goods_sold += float(item.product.cost_price * item.quantity)

            # Net profit (estimated as gross profit minus 5% operating costs)
            day_net_profit = day_gross_profit * 0.95

            result.append({
                'date': date_str,
                'total_sales': float(sale['total_sales']),
                'cash_sales': float(day_payments.get('cash', 0)),
                'card_sales': float(day_payments.get('card', 0)),
                'mobile_sales': float(day_payments.get('mpesa', 0)),
                'transactions': sale['transaction_count'],
                'gross_profit': day_gross_profit,
                'net_profit': day_net_profit,
                'cost_of_goods_sold': day_cost_of_goods_sold
            })

        return result

class ProductPriceListPDFView(generics.GenericAPIView):
    """Generate professional Excel-like PDF with product price lists organized by category"""

    def get(self, request):
        # Get price type from query parameters
        price_type = request.query_params.get('price_type', 'both')

        # Validate price_type
        if price_type not in ['retail', 'wholesale', 'both']:
            return Response({'error': 'Invalid price_type. Must be retail, wholesale, or both'}, status=status.HTTP_400_BAD_REQUEST)

        # Get all active products with categories, ordered by category then name
        products = Product.objects.filter(is_active=True).select_related('category').order_by('category__name', 'name')

        # Group products by category
        from collections import defaultdict
        products_by_category = defaultdict(list)

        for product in products:
            category_name = product.category.name if product.category else 'Uncategorized'
            products_by_category[category_name].append(product)

        # Create PDF buffer
        buffer = BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=A4, leftMargin=30, rightMargin=30, topMargin=20, bottomMargin=30)
        elements = []

        # Styles
        styles = getSampleStyleSheet()

        # Company Header Styles
        company_name_style = ParagraphStyle(
            'CompanyName',
            parent=styles['Heading1'],
            fontSize=18,
            spaceAfter=5,
            alignment=1,
            textColor=colors.HexColor('#2c3e50'),
            fontName='Helvetica-Bold'
        )

        receipt_style = ParagraphStyle(
            'ReceiptStyle',
            parent=styles['Heading2'],
            fontSize=14,
            spaceAfter=5,
            alignment=1,
            textColor=colors.HexColor('#e74c3c'),
            fontName='Helvetica-Bold'
        )

        business_info_style = ParagraphStyle(
            'BusinessInfo',
            parent=styles['Normal'],
            fontSize=12,
            alignment=1,
            textColor=colors.HexColor('#34495e'),
            fontName='Helvetica-Bold'
        )

        contact_style = ParagraphStyle(
            'ContactStyle',
            parent=styles['Normal'],
            fontSize=10,
            alignment=1,
            textColor=colors.HexColor('#7f8c8d')
        )

        # Company Header - Keep logo only
        try:
            # Try to load logo from staticfiles directory
            logo_path = os.path.join(settings.BASE_DIR, 'staticfiles', 'images', 'logo.png')
            if os.path.exists(logo_path):
                logo = Image(logo_path, width=60, height=60)
                logo.hAlign = 'CENTER'
                elements.append(logo)
                elements.append(Spacer(1, 10))
        except Exception as e:
            print(f"Logo loading error: {e}")
            pass  # Logo not found, continue without it

        # Company information - Removed as requested
        # elements.append(Paragraph("MWAMBA", company_name_style))
        # elements.append(Paragraph("RECEIPT", receipt_style))
        # elements.append(Paragraph("MWAMBA LIQUOR STORES", business_info_style))
        # elements.append(Paragraph("RONGO", business_info_style))
        # elements.append(Spacer(1, 5))
        # elements.append(Paragraph("Tel: +254 745 119 135", contact_style))
        # elements.append(Paragraph("Paybill: 522533", contact_style))
        # elements.append(Paragraph("Account: 8015580", contact_style))

        elements.append(Spacer(1, 20))

        # Document title
        title_style = ParagraphStyle(
            'DocumentTitle',
            parent=styles['Heading1'],
            fontSize=16,
            spaceAfter=15,
            alignment=1,
            textColor=colors.HexColor('#2c3e50'),
            fontName='Helvetica-Bold'
        )
        elements.append(Paragraph("PRODUCT PRICE LIST", title_style))

        # Date and summary
        date_style = ParagraphStyle(
            'DateStyle',
            parent=styles['Normal'],
            fontSize=9,
            spaceAfter=20,
            alignment=1,
            textColor=colors.HexColor('#7f8c8d')
        )
        total_products = products.count()
        total_categories = len(products_by_category)
        summary_text = f"Generated on: {timezone.now().strftime('%Y-%m-%d %H:%M:%S')} | Total Products: {total_products} | Categories: {total_categories}"
        elements.append(Paragraph(summary_text, date_style))

        # Prepare comprehensive table data (Excel-like)
        table_data = []

        # Header row
        header_row = ['Category', 'Product Name', 'SKU']
        if price_type in ['retail', 'both']:
            header_row.append('Retail Price')
        if price_type in ['wholesale', 'both']:
            header_row.append('Wholesale Price')
        table_data.append(header_row)

        # Add data rows grouped by category
        current_category = None
        for category_name, category_products in sorted(products_by_category.items()):
            # Add category separator row (merged cells)
            if current_category is not None:
                # Add empty row for spacing
                empty_row = [''] * len(header_row)
                table_data.append(empty_row)

            # Add category header row
            category_header = [f'CATEGORY: {category_name.upper()}'] + [''] * (len(header_row) - 1)
            table_data.append(category_header)

            # Add product rows for this category
            for product in category_products:
                row = [
                    '',  # Empty category column for products
                    product.name,
                    product.sku or 'N/A'
                ]

                if price_type in ['retail', 'both']:
                    row.append(f"Ksh {product.selling_price:.2f}" if product.selling_price else 'N/A')
                if price_type in ['wholesale', 'both']:
                    row.append(f"Ksh {product.wholesale_price:.2f}" if product.wholesale_price else 'N/A')

                table_data.append(row)

            current_category = category_name

        # Create the comprehensive table
        table = Table(table_data, colWidths=[70, 130, 130] + ([70] * (len(header_row) - 3)))

        # Table style - Excel-like
        table_style = TableStyle([
            # Header styling
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#34495e')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 10),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 8),
            ('TOPPADDING', (0, 0), (-1, 0), 8),

            # Category header rows
            ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#ecf0f1')),
            ('TEXTCOLOR', (0, 1), (-1, -1), colors.HexColor('#2c3e50')),
            ('FONTNAME', (0, 1), (-1, -1), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 1), (-1, -1), 9),
            ('ALIGN', (0, 1), (-1, -1), 'LEFT'),
            ('SPAN', (0, 1), (-1, 1)),  # Merge category header cells

            # Data rows
            ('FONTNAME', (0, 2), (-1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 2), (-1, -1), 8),
            ('ALIGN', (0, 2), (-1, -1), 'LEFT'),
            ('ALIGN', (3, 2), (-1, -1), 'RIGHT'),  # Right align price columns

            # Grid lines
            ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#bdc3c7')),

            # Alternating row colors for data
            ('BACKGROUND', (0, 3), (-1, 3), colors.HexColor('#f8f9fa')),
            ('BACKGROUND', (0, 5), (-1, 5), colors.HexColor('#f8f9fa')),
        ])

        # Apply alternating colors for all data rows
        row_idx = 2  # Start after header and first category
        while row_idx < len(table_data):
            if table_data[row_idx][0] == '':  # Data row (not category header)
                if (row_idx - 2) % 2 == 1:  # Alternate starting from row 3
                    table_style.add('BACKGROUND', (0, row_idx), (-1, row_idx), colors.HexColor('#f8f9fa'))
            row_idx += 1

        # Special styling for category headers
        category_rows = []
        for i, row in enumerate(table_data):
            if len(row) > 0 and str(row[0]).startswith('CATEGORY:'):
                category_rows.append(i)

        for row_idx in category_rows:
            table_style.add('BACKGROUND', (0, row_idx), (-1, row_idx), colors.HexColor('#95a5a6'))
            table_style.add('TEXTCOLOR', (0, row_idx), (-1, row_idx), colors.white)
            table_style.add('FONTNAME', (0, row_idx), (-1, row_idx), 'Helvetica-Bold')
            table_style.add('FONTSIZE', (0, row_idx), (-1, row_idx), 9)

        table.setStyle(table_style)

        # Add table to elements
        elements.append(table)

        # Footer - Removed as requested
        # footer_style = ParagraphStyle(
        #     'FooterStyle',
        #     parent=styles['Normal'],
        #     fontSize=8,
        #     spaceBefore=15,
        #     alignment=1,
        #     textColor=colors.HexColor('#95a5a6')
        # )
        # footer_text = "Thank you for your business | MWAMBA LIQUOR STORES"
        # elements.append(Paragraph(footer_text, footer_style))

        # Build PDF
        doc.build(elements)

        # Get PDF data
        pdf_data = buffer.getvalue()
        buffer.close()

        # Create response
        response = HttpResponse(pdf_data, content_type='application/pdf')
        filename = f"product_price_list_{price_type}_{timezone.now().strftime('%Y%m%d_%H%M%S')}.pdf"
        response['Content-Disposition'] = f'attachment; filename="{filename}"'

        return response

    def _get_today_all_sales(self, date):
        """Get all sales data for today"""
        from sales.models import Sale, SaleItem
        from payments.models import Payment
        from inventory.models import SalesHistory

        # Get all sales for today (exclude voided sales)
        sales = Sale.objects.filter(
            sale_date__date=date,
            voided=False
        ).annotate(
            date=TruncDate('sale_date')
        ).values('date').annotate(
            total_sales=Sum('final_amount'),
            transaction_count=Count('id')
        )

        # Get payment method breakdown for today
        payments = Payment.objects.filter(
            sale__sale_date__date=date,
            sale__voided=False,
            status='completed'
        ).select_related('sale')

        payment_breakdown = {}
        for payment in payments:
            payment_type = payment.payment_type
            amount = float(payment.amount)
            if payment_type == 'split' and payment.split_data:
                # For split payments, add amounts to respective methods
                for method, split_amount in payment.split_data.items():
                    payment_breakdown[method] = payment_breakdown.get(method, 0) + float(split_amount)
            else:
                payment_breakdown[payment_type] = payment_breakdown.get(payment_type, 0) + amount

        # Get total sales and transactions for today
        total_sales = sum(float(sale['total_sales']) for sale in sales)
        total_transactions = sum(sale['transaction_count'] for sale in sales)

        # Calculate actual gross profit for today
        # Gross profit = sum((selling_price - cost_price) * quantity) for all items sold today
        sales_items = SaleItem.objects.filter(
            sale__sale_date__date=date,
            sale__voided=False
        ).select_related('product')

        gross_profit = 0
        for item in sales_items:
            if item.product and item.product.cost_price:
                profit_per_item = (item.unit_price - item.product.cost_price) * item.quantity
                gross_profit += float(profit_per_item)

        # Calculate cost of goods sold
        cost_of_goods_sold = sum(
            float(item.product.cost_price * item.quantity)
            for item in sales_items
            if item.product and item.product.cost_price
        )

        # Net profit (estimated as gross profit minus 5% operating costs)
        net_profit = gross_profit * 0.95

        # Get all sales for today with payment info (exclude voided)
        all_sales = Sale.objects.filter(
            sale_date__date=date,
            voided=False
        ).select_related('customer').prefetch_related('payment_set', 'saleitem_set__product').order_by('-sale_date')

        result = {
            'total_sales': total_sales,
            'total_transactions': total_transactions,
            'average_sale': total_sales / total_transactions if total_transactions > 0 else 0,
            'today_sales': total_sales,
            'gross_profit': gross_profit,
            'net_profit': net_profit,
            'cost_of_goods_sold': cost_of_goods_sold,
            'sales_by_payment_method': payment_breakdown,
            'recent_sales': [
                {
                    'id': sale.id,
                    'customer': sale.customer.name if sale.customer else 'Walk-in',
                    'total_amount': float(sale.final_amount),
                    'receipt_number': sale.receipt_number,
                    'created_at': sale.sale_date.isoformat(),
                    'payment_method': self._determine_payment_method(sale),
                    'sale_type': sale.sale_type,
                    'items': [
                        {
                            'product_name': item.product.name,
                            'quantity': item.quantity,
                            'unit_price': float(item.unit_price),
                            'total': float(item.unit_price * item.quantity)
                        }
                        for item in sale.saleitem_set.all()
                    ],
                    'split_data': sale.payment_set.filter(payment_type='split').first().split_data if sale.payment_set.filter(payment_type='split').exists() and sale.payment_set.filter(payment_type='split').first().split_data else None
                }
                for sale in all_sales
            ]
        }

        return result

    def _get_product_performance(self, date_from, date_to):
        """Get product performance report for the specified date range"""
        from sales.models import SaleItem, Sale
        from django.db.models import Sum, Case, When, DecimalField

        # Get all products sold in the date range with their performance metrics
        # Separate retail and wholesale sales (exclude voided sales)
        product_performance = SaleItem.objects.filter(
            sale__sale_date__date__range=[date_from, date_to],
            sale__voided=False
        ).select_related('product', 'sale').values(
            'product__name',
            'product__sku',
            'product__cost_price',
            'product__selling_price',
            'product__wholesale_price'
        ).annotate(
            # Total quantity sold
            quantity_sold=Sum('quantity'),

            # Retail sales (sale_type='retail')
            retail_quantity=Sum(
                Case(
                    When(sale__sale_type='retail', then='quantity'),
                    default=0,
                    output_field=DecimalField()
                )
            ),
            retail_revenue=Sum(
                Case(
                    When(sale__sale_type='retail', then=F('unit_price') * F('quantity')),
                    default=0,
                    output_field=DecimalField()
                )
            ),

            # Wholesale sales (sale_type='wholesale')
            wholesale_quantity=Sum(
                Case(
                    When(sale__sale_type='wholesale', then='quantity'),
                    default=0,
                    output_field=DecimalField()
                )
            ),
            wholesale_revenue=Sum(
                Case(
                    When(sale__sale_type='wholesale', then=F('unit_price') * F('quantity')),
                    default=0,
                    output_field=DecimalField()
                )
            ),

            # Total revenue (retail + wholesale)
            total_revenue=Sum(F('unit_price') * F('quantity')),

            # Total cost (based on cost_price for all sales)
            total_cost=Sum(F('product__cost_price') * F('quantity'))
        ).order_by('-quantity_sold')

        result = []
        total_retail_quantity = 0
        total_wholesale_quantity = 0
        total_quantity = 0
        total_retail_revenue = 0
        total_wholesale_revenue = 0
        total_revenue = 0
        total_cost = 0
        total_retail_profit = 0
        total_wholesale_profit = 0
        total_profit = 0

        for item in product_performance:
            cost_price = float(item['product__cost_price'] or 0)
            retail_price = float(item['product__selling_price'] or 0)
            wholesale_price = float(item['product__wholesale_price'] or 0)

            # Quantities
            retail_quantity = float(item['retail_quantity'] or 0)
            wholesale_quantity = float(item['wholesale_quantity'] or 0)
            total_quantity_sold = float(item['quantity_sold'] or 0)

            # Revenues
            retail_revenue = float(item['retail_revenue'] or 0)
            wholesale_revenue = float(item['wholesale_revenue'] or 0)
            total_revenue_item = float(item['total_revenue'] or 0)

            # Costs and profits
            total_cost_item = float(item['total_cost'] or 0)
            retail_profit = retail_revenue - (cost_price * retail_quantity)
            wholesale_profit = wholesale_revenue - (cost_price * wholesale_quantity)
            total_profit_item = total_revenue_item - total_cost_item

            result.append({
                'product_name': item['product__name'],
                'sku': item['product__sku'],
                'buying_price': cost_price,
                'retail_price': retail_price,
                'wholesale_price': wholesale_price,
                'retail_quantity': retail_quantity,
                'wholesale_quantity': wholesale_quantity,
                'total_quantity': total_quantity_sold,
                'retail_revenue': retail_revenue,
                'wholesale_revenue': wholesale_revenue,
                'total_revenue': total_revenue_item,
                'total_cost': total_cost_item,
                'retail_profit': retail_profit,
                'wholesale_profit': wholesale_profit,
                'total_profit': total_profit_item
            })

            # Accumulate totals
            total_retail_quantity += retail_quantity
            total_wholesale_quantity += wholesale_quantity
            total_quantity += total_quantity_sold
            total_retail_revenue += retail_revenue
            total_wholesale_revenue += wholesale_revenue
            total_revenue += total_revenue_item
            total_cost += total_cost_item
            total_retail_profit += retail_profit
            total_wholesale_profit += wholesale_profit
            total_profit += total_profit_item

        # Add totals at the bottom
        result.append({
            'product_name': 'TOTAL',
            'sku': '',
            'buying_price': 0.0,
            'retail_price': 0.0,
            'wholesale_price': 0.0,
            'retail_quantity': total_retail_quantity,
            'wholesale_quantity': total_wholesale_quantity,
            'total_quantity': total_quantity,
            'retail_revenue': total_retail_revenue,
            'wholesale_revenue': total_wholesale_revenue,
            'total_revenue': total_revenue,
            'total_cost': total_cost,
            'retail_profit': total_retail_profit,
            'wholesale_profit': total_wholesale_profit,
            'total_profit': total_profit
        })

        return result

class InventorySummaryView(generics.GenericAPIView):
    """Get inventory summary for dashboard and reports"""

    def get(self, request):
        # Check if detailed report data is requested
        if request.query_params.get('report') == 'detailed':
            # Return detailed inventory report data
            date = request.query_params.get('date')
            if date:
                try:
                    date = datetime.strptime(date, '%Y-%m-%d').date()
                except ValueError:
                    return Response({'error': 'Invalid date format. Use YYYY-MM-DD'}, status=status.HTTP_400_BAD_REQUEST)
            inventory_data = self._get_inventory_report_data(date)
            return Response(inventory_data)
        else:
            # Return dashboard summary data
            from inventory.models import Product

            total_items = Product.objects.aggregate(total=Sum('stock_quantity'))['total'] or 0
            total_value = Product.objects.aggregate(
                value=Sum(F('stock_quantity') * F('cost_price'))
            )['value'] or 0

            low_stock_items = Product.objects.filter(
                stock_quantity__lte=F('low_stock_threshold')
            ).count()

            out_of_stock_items = Product.objects.filter(stock_quantity=0).count()

            # Real stock movement data by category for chart
            from inventory.models import StockMovement
            from django.db.models.functions import TruncWeek

            # Get stock movements for the last 12 weeks
            twelve_weeks_ago = timezone.now() - timedelta(weeks=12)
            stock_movements = StockMovement.objects.filter(
                created_at__gte=twelve_weeks_ago
            ).annotate(
                week=TruncWeek('created_at')
            ).values('week', 'product__category__name').annotate(
                total_movement=Sum('quantity')
            ).order_by('week', 'product__category__name')

            # Group by week and category
            movement_by_week = {}
            categories = set()

            for movement in stock_movements:
                # Format week as "Week of YYYY-MM-DD"
                week_start = movement['week']
                week_key = f"Week of {week_start.strftime('%Y-%m-%d')}"
                category_name = movement['product__category__name'] or 'Uncategorized'
                categories.add(category_name)

                if week_key not in movement_by_week:
                    movement_by_week[week_key] = {}

                movement_by_week[week_key][category_name] = abs(movement['total_movement'])

            # Create chart data with all categories for the last 12 weeks
            stock_movement_data = []
            current_date = timezone.now().date()

            for i in range(12):
                week_start = current_date - timedelta(weeks=11-i)
                week_key = f"Week of {week_start.strftime('%Y-%m-%d')}"
                week_data = {'week': week_key}
                for category in categories:
                    week_data[category] = movement_by_week.get(week_key, {}).get(category, 0)
                stock_movement_data.append(week_data)

            data = {
                'total_items': total_items,
                'total_value': float(total_value),
                'low_stock_items': low_stock_items,
                'out_of_stock_items': out_of_stock_items,
                'stock_movement_data': stock_movement_data
            }

            serializer = InventorySummarySerializer(data)
            return Response(serializer.data)

    def _get_inventory_report_data(self, date=None):
        """Get detailed inventory data for reports"""
        from inventory.models import Product, StockMovement
        from sales.models import SaleItem
        from django.db.models import Sum

        products = Product.objects.all().select_related('category')

        # Default to today if no date provided
        if date is None:
            date = timezone.now().date()

        result = []
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

            result.append({
                'product': product.name,
                'category': product.category.name if product.category else 'Uncategorized',
                'stock_level': product.stock_quantity,
                'sold_today': sold_today,
                'received_today': received_today,
                'value': float(product.stock_quantity * product.selling_price),
            })

        return result

class CustomerSummaryView(generics.GenericAPIView):
    """Get customer summary for dashboard and reports"""

    def get(self, request):
        # Check if detailed report data is requested
        if request.query_params.get('report') == 'detailed':
            # Return detailed customer report data
            customer_data = self._get_customer_report_data()
            return Response(customer_data)
        else:
            # Return dashboard summary data
            from customers.models import Customer
            from sales.models import Sale

            total_customers = Customer.objects.count()
            # Count customers who have made purchases in the last 30 days
            thirty_days_ago = timezone.now().date() - timedelta(days=30)
            active_customers = Customer.objects.filter(
                sale__sale_date__date__gte=thirty_days_ago
            ).distinct().count()

            new_customers_today = Customer.objects.filter(
                created_at__date=timezone.now().date()
            ).count()

            # Top customers by purchase value
            top_customers = Sale.objects.values(
                'customer__name'
            ).annotate(
                total_spent=Sum('final_amount')
            ).order_by('-total_spent')[:5]

            data = {
                'total_customers': total_customers,
                'active_customers': active_customers,
                'new_customers_today': new_customers_today,
                'top_customers': [
                    {
                        'name': customer['customer__name'],
                        'total_spent': float(customer['total_spent'])
                    }
                    for customer in top_customers
                ]
            }

            serializer = CustomerSummarySerializer(data)
            return Response(serializer.data)

    def _get_customer_report_data(self):
        """Get detailed customer data for reports"""
        from customers.models import Customer
        from sales.models import Sale

        customers = Customer.objects.all()

        result = []
        for customer in customers:
            customer_sales = Sale.objects.filter(customer=customer)
            total_purchases = customer_sales.aggregate(total=Sum('final_amount'))['total'] or 0
            order_count = customer_sales.count()

            last_purchase = customer_sales.order_by('-sale_date').first()
            last_purchase_date = last_purchase.sale_date if last_purchase else None

            result.append({
                'name': customer.name,
                'phone': customer.phone,
                'total_purchases': float(total_purchases),
                'last_visit': last_purchase_date.strftime('%Y-%m-%d') if last_purchase_date else None,
                'loyalty_points': customer.loyalty_points,
            })

        return sorted(result, key=lambda x: x['total_purchases'], reverse=True)

class ShiftSummaryView(generics.GenericAPIView):
    """Get shift summary for dashboard and reports"""

    def get(self, request):
        # Check if detailed report data is requested
        if request.query_params.get('report') == 'detailed':
            # Return detailed shift report data
            shift_data = self._get_shift_report_data()
            return Response(shift_data)
        else:
            # Return dashboard summary data
            from shifts.models import Shift

            active_shifts = Shift.objects.filter(status='open').count()

    def _get_detailed_transactions_for_date(self, date):
        """Get detailed transactions and items sold for a specific date"""
        from sales.models import Sale, SaleItem
        from payments.models import Payment

        # Get all non-voided sales for the date
        sales = Sale.objects.filter(
            sale_date__date=date,
            voided=False
        ).select_related('customer').prefetch_related('saleitem_set', 'payment_set').order_by('-sale_date')

        result = []
        for sale in sales:
            # Get payment info
            payments = sale.payment_set.filter(status='completed')
            payment_info = []
            for payment in payments:
                payment_info.append({
                    'payment_type': payment.payment_type,
                    'amount': float(payment.amount),
                    'reference_number': payment.reference_number,
                    'created_at': payment.created_at.isoformat()
                })

            # Get sale items
            items = []
            for item in sale.saleitem_set.all():
                items.append({
                    'product_name': item.product.name,
                    'product_sku': item.product.sku,
                    'quantity': item.quantity,
                    'unit_price': float(item.unit_price),
                    'discount': float(item.discount),
                    'total_price': float(item.unit_price * item.quantity - item.discount)
                })

            result.append({
                'transaction_id': sale.id,
                'receipt_number': sale.receipt_number,
                'sale_date': sale.sale_date.isoformat(),
                'customer': sale.customer.name if sale.customer else 'Walk-in',
                'sale_type': sale.sale_type,
                'total_amount': float(sale.final_amount),
                'tax_amount': float(sale.tax_amount),
                'discount_amount': float(sale.discount_amount),
                'final_amount': float(sale.final_amount),
                'payments': payment_info,
                'items': items
            })

        return result

    def _get_detailed_transactions_for_range(self, date_from, date_to):
        """Get detailed transactions and items sold for a date range"""
        from sales.models import Sale, SaleItem
        from payments.models import Payment

        # Get all non-voided sales for the date range
        sales = Sale.objects.filter(
            sale_date__date__range=[date_from, date_to],
            voided=False
        ).select_related('customer').prefetch_related('saleitem_set', 'payment_set').order_by('-sale_date')

        result = []
        for sale in sales:
            # Get payment info
            payments = sale.payment_set.filter(status='completed')
            payment_info = []
            for payment in payments:
                payment_info.append({
                    'payment_type': payment.payment_type,
                    'amount': float(payment.amount),
                    'reference_number': payment.reference_number,
                    'created_at': payment.created_at.isoformat()
                })

            # Get sale items
            items = []
            for item in sale.saleitem_set.all():
                items.append({
                    'product_name': item.product.name,
                    'product_sku': item.product.sku,
                    'quantity': item.quantity,
                    'unit_price': float(item.unit_price),
                    'discount': float(item.discount),
                    'total_price': float(item.unit_price * item.quantity - item.discount)
                })

            result.append({
                'transaction_id': sale.id,
                'receipt_number': sale.receipt_number,
                'sale_date': sale.sale_date.isoformat(),
                'customer': sale.customer.name if sale.customer else 'Walk-in',
                'sale_type': sale.sale_type,
                'total_amount': float(sale.final_amount),
                'tax_amount': float(sale.tax_amount),
                'discount_amount': float(sale.discount_amount),
                'final_amount': float(sale.final_amount),
                'payments': payment_info,
                'items': items
            })

        return result

    def _get_detailed_transactions_for_shift(self, shift_id):
        """Get detailed transactions and items sold for a specific shift"""
        from sales.models import Sale, SaleItem
        from payments.models import Payment

        # Get all non-voided sales for the shift
        sales = Sale.objects.filter(
            shift_id=shift_id,
            voided=False
        ).select_related('customer').prefetch_related('saleitem_set', 'payment_set').order_by('-sale_date')

        result = []
        for sale in sales:
            # Get payment info
            payments = sale.payment_set.filter(status='completed')
            payment_info = []
            for payment in payments:
                payment_info.append({
                    'payment_type': payment.payment_type,
                    'amount': float(payment.amount),
                    'reference_number': payment.reference_number,
                    'created_at': payment.created_at.isoformat()
                })

            # Get sale items
            items = []
            for item in sale.saleitem_set.all():
                items.append({
                    'product_name': item.product.name,
                    'product_sku': item.product.sku,
                    'quantity': item.quantity,
                    'unit_price': float(item.unit_price),
                    'discount': float(item.discount),
                    'total_price': float(item.unit_price * item.quantity - item.discount)
                })

            result.append({
                'transaction_id': sale.id,
                'receipt_number': sale.receipt_number,
                'sale_date': sale.sale_date.isoformat(),
                'customer': sale.customer.name if sale.customer else 'Walk-in',
                'sale_type': sale.sale_type,
                'total_amount': float(sale.final_amount),
                'tax_amount': float(sale.tax_amount),
                'discount_amount': float(sale.discount_amount),
                'final_amount': float(sale.final_amount),
                'payments': payment_info,
                'items': items
            })

        return result

    def get(self, request):
        # Check if detailed report data is requested
        if request.query_params.get('report') == 'detailed':
            # Return detailed shift report data
            shift_data = self._get_shift_report_data()
            return Response(shift_data)
        else:
            # Return dashboard summary data
            from shifts.models import Shift

            active_shifts = Shift.objects.filter(status='open').count()

            completed_shifts_today = Shift.objects.filter(
                end_time__date=timezone.now().date(),
                status='closed'
            ).count()

            total_shift_sales = Shift.objects.filter(
                end_time__date=timezone.now().date(),
                status='closed'
            ).aggregate(total=Sum('total_sales'))['total'] or 0

            data = {
                'active_shifts': active_shifts,
                'completed_shifts_today': completed_shifts_today,
                'total_shift_sales': float(total_shift_sales)
            }

            serializer = ShiftSummarySerializer(data)
            return Response(serializer.data)

        return result

    def _get_sale_chit_details(self, sale_id):
        """Get detailed chit information for a specific sale"""
        from sales.models import Sale, SaleItem
        from payments.models import Payment

        try:
            # Get the sale with related data
            sale = Sale.objects.select_related('customer', 'shift__cashier__user').prefetch_related(
                'saleitem_set__product', 'payment_set'
            ).get(id=sale_id, voided=False)

            # Get sale items with product details
            items = []
            for item in sale.saleitem_set.all():
                items.append({
                    'id': item.id,
                    'product_name': item.product.name,
                    'product_sku': item.product.sku,
                    'category': item.product.category.name if item.product.category else 'Uncategorized',
                    'quantity': item.quantity,
                    'unit_price': float(item.unit_price),
                    'discount': float(item.discount),
                    'line_total': float((item.unit_price * item.quantity) - item.discount),
                    'cost_price': float(item.product.cost_price) if item.product.cost_price else 0,
                    'profit': float(((item.unit_price - item.product.cost_price) * item.quantity) - item.discount) if item.product.cost_price else 0
                })

            # Get payment details
            payments = []
            payment_breakdown = {}
            for payment in sale.payment_set.filter(status='completed'):
                payments.append({
                    'id': payment.id,
                    'payment_type': payment.payment_type,
                    'amount': float(payment.amount),
                    'reference_number': payment.reference_number,
                    'created_at': payment.created_at.isoformat(),
                    'status': payment.status
                })
                # Aggregate payment amounts by type
                payment_breakdown[payment.payment_type] = payment_breakdown.get(payment.payment_type, 0) + float(payment.amount)

            # Calculate totals
            subtotal = sum(item['line_total'] for item in items)
            total_discount = sum(item['discount'] * item['quantity'] for item in items)
            total_cost = sum(item['cost_price'] * item['quantity'] for item in items)
            total_profit = sum(item['profit'] for item in items)

            chit_data = {
                'sale_id': sale.id,
                'receipt_number': sale.receipt_number,
                'sale_date': sale.sale_date.isoformat(),
                'sale_type': sale.sale_type,
                'customer': {
                    'id': sale.customer.id if sale.customer else None,
                    'name': sale.customer.name if sale.customer else 'Walk-in',
                    'phone': sale.customer.phone if sale.customer else None
                },
                'cashier': {
                    'id': sale.shift.cashier.id if sale.shift and sale.shift.cashier else None,
                    'name': sale.shift.cashier.user.get_full_name() if sale.shift and sale.shift.cashier else 'Unknown',
                    'username': sale.shift.cashier.user.username if sale.shift and sale.shift.cashier else 'Unknown'
                } if sale.shift else None,
                'shift': {
                    'id': sale.shift.id if sale.shift else None,
                    'start_time': sale.shift.start_time.isoformat() if sale.shift else None,
                    'status': sale.shift.status if sale.shift else None
                } if sale.shift else None,
                'items': items,
                'payments': payments,
                'payment_breakdown': payment_breakdown,
                'summary': {
                    'item_count': len(items),
                    'total_quantity': sum(item['quantity'] for item in items),
                    'subtotal': subtotal,
                    'tax_amount': float(sale.tax_amount),
                    'discount_amount': float(sale.discount_amount),
                    'final_amount': float(sale.final_amount),
                    'total_cost': total_cost,
                    'total_profit': total_profit,
                    'profit_margin': (total_profit / subtotal * 100) if subtotal > 0 else 0
                },
                'status': 'completed',
                'voided': sale.voided
            }

            return chit_data

        except Sale.DoesNotExist:
            return {'error': 'Sale not found or has been voided'}
        except Exception as e:
            return {'error': f'Error retrieving chit details: {str(e)}'}
    def _get_shift_report_data(self):
        """Get detailed shift data for reports"""
        from shifts.models import Shift

        shifts = Shift.objects.filter(status='closed').select_related('cashier__user').order_by('-end_time')

        result = []
        for shift in shifts:
            result.append({
                'cashier': shift.cashier.user.username,
                'shift_date': shift.end_time.date().strftime('%Y-%m-%d'),
                'start_time': shift.start_time.strftime('%H:%M') if shift.start_time else 'N/A',
                'end_time': shift.end_time.strftime('%H:%M') if shift.end_time else 'N/A',
                'opening_balance': float(shift.opening_balance),
                'closing_balance': float(shift.closing_balance),
                'total_sales': float(shift.total_sales),
                'discrepancy': float(shift.discrepancy) if shift.discrepancy else 0,
            })

        return result
