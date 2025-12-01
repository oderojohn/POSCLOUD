from django.core.management.base import BaseCommand
from sales.models import Sale
from payments.models import Payment

class Command(BaseCommand):
    help = 'Create default payment records for sales that don\'t have any payments'

    def handle(self, *args, **options):
        # Find all non-voided sales without payment records
        sales_without_payments = Sale.objects.filter(
            voided=False
        ).exclude(
            payment__isnull=False
        ).distinct()

        count = 0
        for sale in sales_without_payments:
            # Create a default cash payment
            Payment.objects.create(
                sale=sale,
                payment_type='cash',
                amount=sale.final_amount,
                status='completed',
                description='Auto-generated default payment for existing sale'
            )
            count += 1
            self.stdout.write(
                self.style.SUCCESS(f'Created payment for sale {sale.receipt_number}')
            )

        self.stdout.write(
            self.style.SUCCESS(f'Successfully created {count} default payment records')
        )