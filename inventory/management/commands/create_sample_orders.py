from django.core.management.base import BaseCommand
from django.utils import timezone
from inventory.models import Product, Batch
from suppliers.models import Supplier, PurchaseOrder, PurchaseOrderItem
from decimal import Decimal
import random
from datetime import timedelta

class Command(BaseCommand):
    help = 'Create sample purchase orders and batches'

    def handle(self, *args, **options):
        self.stdout.write('Creating sample purchase orders and batches...')

        # Get suppliers and products
        suppliers = list(Supplier.objects.all())
        products = list(Product.objects.all())

        if not suppliers or not products:
            self.stdout.write(self.style.ERROR('No suppliers or products found. Run populate_liquor_data first.'))
            return

        # Create sample purchase orders
        orders_data = [
            {
                'supplier': suppliers[0],  # Kenya Wine Agencies Ltd
                'order_date': timezone.now().date() - timedelta(days=5),
                'expected_delivery_date': timezone.now().date() + timedelta(days=7),
                'notes': 'Monthly wine restock order',
                'status': 'pending',
                'items': [
                    {'product': products[0], 'quantity': 12, 'unit_price': products[0].cost_price},  # Robert Mondavi
                    {'product': products[1], 'quantity': 8, 'unit_price': products[1].cost_price},   # Cloudy Bay
                    {'product': products[3], 'quantity': 24, 'unit_price': products[3].cost_price},  # Mateus Rosé
                ]
            },
            {
                'supplier': suppliers[1],  # East African Spirits Ltd
                'order_date': timezone.now().date() - timedelta(days=3),
                'expected_delivery_date': timezone.now().date() + timedelta(days=5),
                'notes': 'Spirit restock - priority delivery needed',
                'status': 'shipped',
                'items': [
                    {'product': products[4], 'quantity': 18, 'unit_price': products[4].cost_price},  # Jameson
                    {'product': products[5], 'quantity': 12, 'unit_price': products[5].cost_price},  # Johnnie Walker
                    {'product': products[7], 'quantity': 30, 'unit_price': products[7].cost_price},  # Absolut Vodka
                    {'product': products[9], 'quantity': 24, 'unit_price': products[9].cost_price},  # Gordon's Gin
                ]
            },
            {
                'supplier': suppliers[2],  # Premium Imports Kenya
                'order_date': timezone.now().date() - timedelta(days=7),
                'expected_delivery_date': timezone.now().date() - timedelta(days=1),
                'notes': 'Premium champagne and rum order',
                'status': 'delivered',
                'items': [
                    {'product': products[14], 'quantity': 6, 'unit_price': products[14].cost_price},  # Moët & Chandon
                    {'product': products[15], 'quantity': 4, 'unit_price': products[15].cost_price},  # Veuve Clicquot
                    {'product': products[11], 'quantity': 36, 'unit_price': products[11].cost_price}, # Bacardi
                    {'product': products[13], 'quantity': 12, 'unit_price': products[13].cost_price}, # Myers's Dark Rum
                ]
            },
        ]

        for order_data in orders_data:
            # Create purchase order
            order = PurchaseOrder.objects.create(
                supplier=order_data['supplier'],
                order_date=order_data['order_date'],
                expected_delivery_date=order_data['expected_delivery_date'],
                notes=order_data['notes'],
                status=order_data['status']
            )

            # Create order items and batches
            for item_data in order_data['items']:
                # Create purchase order item
                order_item = PurchaseOrderItem.objects.create(
                    purchase_order=order,
                    product=item_data['product'],
                    quantity=item_data['quantity'],
                    unit_price=item_data['unit_price']
                )

                # Create batch for this order item
                batch_number = f"PO{order.id:04d}-{item_data['product'].sku[-3:]}"

                # Set batch status based on order status
                if order_data['status'] == 'delivered':
                    batch_status = 'received'
                    received_date = timezone.now().date() - timedelta(days=1)
                elif order_data['status'] == 'shipped':
                    batch_status = 'ordered'
                    received_date = None
                else:
                    batch_status = 'ordered'
                    received_date = None

                # Create expiry date (liquor typically expires in 2-5 years)
                expiry_years = random.randint(2, 5)
                expiry_date = timezone.now().date() + timedelta(days=365 * expiry_years)

                batch = Batch.objects.create(
                    product=item_data['product'],
                    supplier=order.supplier,
                    batch_number=batch_number,
                    quantity=item_data['quantity'],
                    cost_price=item_data['unit_price'],
                    expiry_date=expiry_date,
                    purchase_date=order.order_date,
                    received_date=received_date,
                    status=batch_status,
                    purchase_order_item=order_item
                )

                # If batch is received, add to stock
                if batch_status == 'received':
                    batch.receive_batch()

                self.stdout.write(f'Created batch: {batch.batch_number} for {item_data["product"].name} ({batch.status})')

            self.stdout.write(f'Created order: {order.order_number} - {order.status} - {len(order_data["items"])} items')

        self.stdout.write(self.style.SUCCESS('Sample orders and batches created successfully!'))

        # Summary
        total_orders = PurchaseOrder.objects.count()
        total_batches = Batch.objects.count()
        received_batches = Batch.objects.filter(status='received').count()
        ordered_batches = Batch.objects.filter(status='ordered').count()

        self.stdout.write(f'Total Orders: {total_orders}')
        self.stdout.write(f'Total Batches: {total_batches}')
        self.stdout.write(f'Received Batches: {received_batches}')
        self.stdout.write(f'Ordered Batches: {ordered_batches}')