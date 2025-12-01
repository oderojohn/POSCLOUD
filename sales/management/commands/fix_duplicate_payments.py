from django.core.management.base import BaseCommand
from sales.models import Sale
from payments.models import Payment

class Command(BaseCommand):
    help = 'Fix duplicate payments for sales'

    def handle(self, *args, **options):
        sales = Sale.objects.all()
        fixed_count = 0

        for sale in sales:
            payments = list(sale.payment_set.filter(status='completed').order_by('created_at'))
            if len(payments) > 1:
                # Keep the first payment, delete the rest
                for payment in payments[1:]:
                    self.stdout.write(f'Deleting duplicate payment {payment.id} for sale {sale.id}')
                    payment.delete()
                fixed_count += 1

        self.stdout.write(f'Fixed {fixed_count} sales with duplicate payments')