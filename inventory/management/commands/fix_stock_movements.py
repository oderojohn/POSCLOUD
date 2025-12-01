from django.core.management.base import BaseCommand
from inventory.models import StockMovement, Product, Batch
from django.db.models import Sum

class Command(BaseCommand):
    help = 'Fix incorrect stock movements caused by negative quantity bug and recalculate stock levels'

    def handle(self, *args, **options):
        self.stdout.write('Starting stock movement fix...')

        # Step 1: Fix incorrect StockMovement records
        # Find 'out' movements with positive quantity (should be negative)
        incorrect_movements = StockMovement.objects.filter(
            movement_type='out',
            quantity__gt=0
        )

        fixed_count = 0
        for movement in incorrect_movements:
            old_quantity = movement.quantity
            movement.quantity = -abs(old_quantity)  # Ensure negative
            movement.save()
            fixed_count += 1
            self.stdout.write(
                f'Fixed movement {movement.id}: {old_quantity} -> {movement.quantity}'
            )

        self.stdout.write(f'Fixed {fixed_count} incorrect stock movements.')

        # Step 2: Recalculate product stock quantities based on corrected movements
        products = Product.objects.all()
        for product in products:
            # Calculate total stock from all movements
            total_movement = StockMovement.objects.filter(
                product=product
            ).aggregate(total=Sum('quantity'))['total'] or 0

            old_stock = product.stock_quantity
            product.stock_quantity = max(0, int(total_movement))  # Ensure non-negative
            product.save(update_fields=['stock_quantity'])

            if old_stock != product.stock_quantity:
                self.stdout.write(
                    f'Updated {product.name} stock: {old_stock} -> {product.stock_quantity}'
                )

        # Step 3: Recalculate batch quantities
        # This is more complex as batches are updated separately
        # We'll recalculate based on sales history and initial batch quantities
        batches = Batch.objects.filter(status='received')
        for batch in batches:
            # Get total sold from this batch
            from inventory.models import SalesHistory
            total_sold = SalesHistory.objects.filter(
                batch=batch
            ).aggregate(total=Sum('quantity'))['total'] or 0

            # Batch quantity should be initial - sold
            # But we don't have initial recorded, so we'll set it to max(0, current - adjustment)
            # For now, just ensure it's not negative
            if batch.quantity < 0:
                old_quantity = batch.quantity
                batch.quantity = 0
                batch.save(update_fields=['quantity'])
                self.stdout.write(
                    f'Fixed batch {batch.batch_number} quantity: {old_quantity} -> 0'
                )

        self.stdout.write('Stock movement fix completed.')