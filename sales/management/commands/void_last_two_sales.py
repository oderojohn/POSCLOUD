from django.core.management.base import BaseCommand
from sales.models import Sale

class Command(BaseCommand):
    help = 'Void the last two sales with a default reason'

    def handle(self, *args, **options):
        # Get the last two non-voided sales
        last_two_sales = Sale.objects.filter(voided=False).order_by('-sale_date')[:2]

        if not last_two_sales:
            self.stdout.write(
                self.style.WARNING('No sales found to void')
            )
            return

        void_reason = 'Voided due to payment validation fix - stock and sale corrections'
        count = 0

        for sale in last_two_sales:
            try:
                # Use the existing void_sale logic
                from django.db import transaction
                with transaction.atomic():
                    # Mark sale as voided
                    sale.voided = True
                    sale.void_reason = void_reason
                    sale.voided_at = sale.sale_date  # Use sale date as voided_at
                    sale.save()

                    # Restore stock quantities
                    for sale_item in sale.saleitem_set.all():
                        product = sale_item.product
                        quantity_to_restore = sale_item.quantity

                        # Restore product stock
                        from decimal import Decimal
                        product.stock_quantity = Decimal(str(product.stock_quantity)) + Decimal(str(quantity_to_restore))
                        product.save(update_fields=['stock_quantity'])

                        # Create stock movement record
                        from inventory.models import StockMovement
                        StockMovement.objects.create(
                            product=product,
                            movement_type='in',
                            quantity=quantity_to_restore,
                            reason=f'Sale void {sale.receipt_number} - {void_reason}',
                            user=None  # No user context in management command
                        )

                        # Update batch quantities if applicable
                        from inventory.models import Batch, SalesHistory
                        sales_history_records = SalesHistory.objects.filter(
                            product=product,
                            receipt_number=sale.receipt_number
                        )

                        for history_record in sales_history_records:
                            if history_record.batch:
                                batch = history_record.batch
                                batch.quantity = Decimal(str(batch.quantity)) + Decimal(str(history_record.quantity))
                                batch.save(update_fields=['quantity'])

                    # Update shift totals (subtract the voided sale)
                    if sale.shift:
                        from decimal import Decimal
                        void_amount = Decimal(str(sale.final_amount))

                        # Get payment method from payment record
                        payment = sale.payment_set.first()
                        payment_method = payment.payment_type if payment else 'cash'

                        # Subtract from shift totals based on payment method
                        if payment_method == 'cash':
                            sale.shift.cash_sales = sale.shift.cash_sales - void_amount
                            sale.shift.save(update_fields=['cash_sales'])
                        elif payment_method == 'card':
                            sale.shift.card_sales = sale.shift.card_sales - void_amount
                            sale.shift.save(update_fields=['card_sales'])
                        elif payment_method in ['mpesa', 'mobile']:
                            sale.shift.mobile_sales = sale.shift.mobile_sales - void_amount
                            sale.shift.save(update_fields=['mobile_sales'])

                        # Subtract from total sales
                        sale.shift.total_sales = sale.shift.total_sales - void_amount
                        sale.shift.save(update_fields=['total_sales'])

                count += 1
                self.stdout.write(
                    self.style.SUCCESS(f'Successfully voided sale {sale.receipt_number}')
                )

            except Exception as e:
                self.stdout.write(
                    self.style.ERROR(f'Failed to void sale {sale.receipt_number}: {str(e)}')
                )

        self.stdout.write(
            self.style.SUCCESS(f'Successfully voided {count} sales')
        )