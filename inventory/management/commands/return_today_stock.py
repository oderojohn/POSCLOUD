from django.core.management.base import BaseCommand
from django.utils import timezone
from django.db import transaction
from decimal import Decimal
from inventory.models import Product, Batch, StockMovement, SalesHistory
from sales.models import Sale, SaleItem


class Command(BaseCommand):
    help = 'Return stock for all sales made today (automatic end-of-day stock reconciliation)'

    def add_arguments(self, parser):
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Show what would be done without actually making changes',
        )

    def handle(self, *args, **options):
        dry_run = options['dry_run']

        # Always process today's date for automatic execution
        target_date = timezone.now().date()

        self.stdout.write(
            self.style.WARNING(f'Processing automatic stock return for today: {target_date}')
        )
        if dry_run:
            self.stdout.write(self.style.WARNING('DRY RUN MODE - No changes will be made'))

        # Get all non-voided sales for the target date
        today_sales = Sale.objects.filter(
            sale_date__date=target_date,
            voided=False
        ).select_related('shift').prefetch_related('saleitem_set__product')

        if not today_sales.exists():
            self.stdout.write(
                self.style.SUCCESS(f'No sales found for {target_date}')
            )
            return

        total_sales = today_sales.count()
        total_items = 0
        stock_returned = {}

        self.stdout.write(f'Found {total_sales} sales for {target_date}')

        try:
            with transaction.atomic():
                for sale in today_sales:
                    self.stdout.write(f'Processing sale: {sale.receipt_number}')

                    for sale_item in sale.saleitem_set.all():
                        product = sale_item.product
                        quantity_to_return = sale_item.quantity
                        total_items += 1

                        if product.name not in stock_returned:
                            stock_returned[product.name] = 0
                        stock_returned[product.name] += quantity_to_return

                        if not dry_run:
                            # Return stock to product
                            from decimal import Decimal
                            product.stock_quantity = Decimal(str(product.stock_quantity)) + Decimal(str(quantity_to_return))
                            product.save(update_fields=['stock_quantity'])

                            # Create stock movement record
                            StockMovement.objects.create(
                                product=product,
                                movement_type='in',
                                quantity=quantity_to_return,
                                reason=f'Stock return for sale {sale.receipt_number} - End of day reconciliation',
                                user=sale.shift.cashier if sale.shift else None
                            )

                            # Update batch quantities if applicable
                            sales_history_records = SalesHistory.objects.filter(
                                product=product,
                                receipt_number=sale.receipt_number
                            )

                            for history_record in sales_history_records:
                                if history_record.batch:
                                    batch = history_record.batch
                                    batch.quantity = Decimal(str(batch.quantity)) + Decimal(str(history_record.quantity))
                                    batch.save(update_fields=['quantity'])

                if dry_run:
                    self.stdout.write(
                        self.style.SUCCESS(f'\nDRY RUN SUMMARY for {target_date}:')
                    )
                else:
                    self.stdout.write(
                        self.style.SUCCESS(f'\nSTOCK RETURN COMPLETED for {target_date}:')
                    )

                self.stdout.write(f'- Total sales processed: {total_sales}')
                self.stdout.write(f'- Total items returned: {total_items}')
                self.stdout.write(f'- Products affected: {len(stock_returned)}')

                if stock_returned:
                    self.stdout.write('\nStock returned by product:')
                    for product_name, quantity in sorted(stock_returned.items()):
                        self.stdout.write(f'  - {product_name}: {quantity} units')

                if not dry_run:
                    self.stdout.write(
                        self.style.SUCCESS('\n✅ Stock return completed successfully!')
                    )
                    self.stdout.write(
                        self.style.WARNING('Note: Sales records remain intact for audit purposes.')
                    )

        except Exception as e:
            self.stderr.write(
                self.style.ERROR(f'Error during stock return: {str(e)}')
            )
            if not dry_run:
                raise