from django.core.management.base import BaseCommand
from inventory.models import Category, Product

class Command(BaseCommand):
    help = 'Remove TEST category and all its products'

    def add_arguments(self, parser):
        parser.add_argument(
            '--confirm',
            action='store_true',
            help='Confirm deletion without prompting',
        )

    def handle(self, *args, **options):
        try:
            test_category = Category.objects.get(name='TEST')
        except Category.DoesNotExist:
            self.stdout.write(self.style.WARNING('TEST category does not exist.'))
            return

        products = Product.objects.filter(category=test_category)
        product_count = products.count()

        self.stdout.write(f'Found TEST category with {product_count} products:')
        for product in products:
            self.stdout.write(f'  - {product.name}')

        if not options['confirm']:
            confirm = input(f'\nAre you sure you want to delete the TEST category and all {product_count} products? (yes/no): ')
            if confirm.lower() != 'yes':
                self.stdout.write(self.style.WARNING('Operation cancelled.'))
                return

        # Delete products first (this will cascade to related models)
        deleted_products = products.delete()
        self.stdout.write(f'Deleted {deleted_products[0]} products.')

        # Delete the category
        test_category.delete()
        self.stdout.write(self.style.SUCCESS('TEST category deleted successfully.'))