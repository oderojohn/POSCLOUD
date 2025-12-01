from django.core.management.base import BaseCommand
from django.utils import timezone
from inventory.models import Batch, StockMovement


class Command(BaseCommand):
    help = 'Update expired batches and remove them from stock'

    def handle(self, *args, **options):
        today = timezone.now().date()

        # Find batches that have expired but are still marked as received
        expired_batches = Batch.objects.filter(
            expiry_date__lt=today,
            status='received',
            quantity__gt=0
        )

        updated_count = 0
        total_quantity_removed = 0

        for batch in expired_batches:
            # Mark batch as expired
            batch.status = 'expired'
            batch.save()

            # Remove from product stock
            batch.product.stock_quantity -= batch.quantity
            batch.product.save()

            # Create stock movement record
            StockMovement.objects.create(
                product=batch.product,
                movement_type='adjustment',
                quantity=-batch.quantity,
                reason=f'Batch {batch.batch_number} expired',
                user=None
            )

            updated_count += 1
            total_quantity_removed += batch.quantity

            self.stdout.write(
                f'Expired batch {batch.batch_number} for {batch.product.name} - removed {batch.quantity} units'
            )

        self.stdout.write(
            self.style.SUCCESS(
                f'Successfully updated {updated_count} expired batches, '
                f'removed {total_quantity_removed} units from stock'
            )
        )