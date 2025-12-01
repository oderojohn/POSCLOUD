from django.core.management.base import BaseCommand
from inventory.models import Product, Batch
from suppliers.models import Supplier, PurchaseOrder, PurchaseOrderItem

class Command(BaseCommand):
    help = 'Check current data in the system'

    def handle(self, *args, **options):
        self.stdout.write('=== DATA CHECK ===')
        self.stdout.write(f'Products: {Product.objects.count()}')
        self.stdout.write(f'Batches: {Batch.objects.count()}')
        self.stdout.write(f'Suppliers: {Supplier.objects.count()}')
        self.stdout.write(f'Purchase Orders: {PurchaseOrder.objects.count()}')
        self.stdout.write(f'Purchase Order Items: {PurchaseOrderItem.objects.count()}')

        if Product.objects.exists():
            product = Product.objects.first()
            self.stdout.write(f'Sample product: {product.name} (Stock: {product.stock_quantity})')

        if Batch.objects.exists():
            batch = Batch.objects.first()
            self.stdout.write(f'Sample batch: {batch.batch_number} - Status: {batch.status}')

        if Supplier.objects.exists():
            supplier = Supplier.objects.first()
            self.stdout.write(f'Sample supplier: {supplier.name}')

        if PurchaseOrder.objects.exists():
            po = PurchaseOrder.objects.first()
            self.stdout.write(f'Sample PO: {po.order_number} - Status: {po.status}')

        self.stdout.write('=== END DATA CHECK ===')