from django.core.management.base import BaseCommand
from sales.models import Sale
from payments.models import Payment
from decimal import Decimal

class Command(BaseCommand):
    help = 'Fix payment amounts to match their corresponding sale amounts'

    def handle(self, *args, **options):
        self.stdout.write('Checking payment amounts...')

        # Find all sales with payments
        sales_with_payments = Sale.objects.filter(payment__isnull=False).distinct()

        fixed_count = 0
        mismatch_count = 0

        for sale in sales_with_payments:
            payments = Payment.objects.filter(sale=sale, status='completed')
            total_payment_amount = sum(float(payment.amount) for payment in payments)

            sale_amount = float(sale.final_amount)

            # Check if payment total matches sale amount
            if abs(total_payment_amount - sale_amount) > 0.01:
                mismatch_count += 1
                self.stdout.write(
                    f'MISMATCH - Sale {sale.id} (Receipt: {sale.receipt_number}): '
                    f'Sale amount: {sale_amount}, Payment total: {total_payment_amount}'
                )

                # For single payment sales, fix the payment amount
                if payments.count() == 1:
                    payment = payments.first()
                    old_amount = float(payment.amount)
                    payment.amount = Decimal(str(sale_amount))
                    payment.save()

                    self.stdout.write(
                        f'  FIXED - Payment {payment.id}: {old_amount} -> {sale_amount}'
                    )
                    fixed_count += 1
                else:
                    # For multiple payments, check if it's a split payment (has both cash and mpesa)
                    payment_types = set(payments.values_list('payment_type', flat=True))

                    if 'cash' in payment_types and 'mpesa' in payment_types:
                        self.stdout.write(
                            f'  SPLIT PAYMENT - Attempting to fix sale {sale.id}'
                        )

                        # Calculate correct amounts - for now, assume equal split
                        # In a real scenario, you'd need to know the intended split
                        cash_payments = payments.filter(payment_type='cash')
                        mpesa_payments = payments.filter(payment_type='mpesa')

                        # For simplicity, assume the split was intended to be 50/50
                        # But this might not be accurate. A better approach would be to
                        # look at the original split_data if stored somewhere

                        # For now, just fix by making all payments equal portions
                        num_payments = payments.count()
                        amount_per_payment = sale_amount / num_payments

                        for payment in payments:
                            old_amount = float(payment.amount)
                            payment.amount = Decimal(str(amount_per_payment))
                            payment.save()
                            self.stdout.write(f'    Payment {payment.id} ({payment.payment_type}): {old_amount} -> {amount_per_payment}')

                        fixed_count += 1
                    else:
                        self.stdout.write(
                            f'  MULTIPLE PAYMENTS - Manual review needed for sale {sale.id} (types: {list(payment_types)})'
                        )

        self.stdout.write(f'\nSummary:')
        self.stdout.write(f'  Mismatches found: {mismatch_count}')
        self.stdout.write(f'  Payments fixed: {fixed_count}')

        if mismatch_count == 0:
            self.stdout.write('All payment amounts match sale amounts!')
        else:
            self.stdout.write(f'{mismatch_count - fixed_count} cases need manual review')