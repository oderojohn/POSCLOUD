from django.core.management.base import BaseCommand
from inventory.models import Product, Category

class Command(BaseCommand):
    help = 'Populate brandy products'

    def handle(self, *args, **options):
        # Get the Brandy category
        try:
            brandy_category = Category.objects.get(name='Brandy')
        except Category.DoesNotExist:
            self.stdout.write(self.style.ERROR('Brandy category does not exist'))
            return

        # Brandy products data
        products_data = [
            {
                'name': 'Victory 250ml',
                'sku': 'BR-VIC-250',
                'cost_price': 447.74,
                'selling_price': 500.00,
                'wholesale_price': 464.00,
                'wholesale_min_qty': 1,
                'stock_quantity': 10,
            },
            {
                'name': 'Victory 350ml',
                'sku': 'BR-VIC-350',
                'cost_price': 653.18,
                'selling_price': 740.00,
                'wholesale_price': 676.00,
                'wholesale_min_qty': 1,
                'stock_quantity': 10,
            },
            {
                'name': 'Victory 750ml',
                'sku': 'BR-VIC-750',
                'cost_price': 1266.00,
                'selling_price': 1400.00,
                'wholesale_price': 1312.00,
                'wholesale_min_qty': 1,
                'stock_quantity': 10,
            },
            {
                'name': 'Napoleon 250ml',
                'sku': 'BR-NAP-250',
                'cost_price': 221.27,
                'selling_price': 270.00,
                'wholesale_price': 235.00,
                'wholesale_min_qty': 1,
                'stock_quantity': 10,
            },
            {
                'name': 'Napoleon 750ml',
                'sku': 'BR-NAP-750',
                'cost_price': 650.55,
                'selling_price': 750.00,
                'wholesale_price': 690.00,
                'wholesale_min_qty': 1,
                'stock_quantity': 10,
            },
            {
                'name': 'Richot 250ml',
                'sku': 'BR-RIC-250',
                'cost_price': 446.00,
                'selling_price': 500.00,
                'wholesale_price': 425.00,
                'wholesale_min_qty': 1,
                'stock_quantity': 10,
            },
            {
                'name': 'Richot 350ml',
                'sku': 'BR-RIC-350',
                'cost_price': 576.00,
                'selling_price': 700.00,
                'wholesale_price': 610.00,
                'wholesale_min_qty': 1,
                'stock_quantity': 10,
            },
            {
                'name': 'Richot 750ml',
                'sku': 'BR-RIC-750',
                'cost_price': 1240.00,
                'selling_price': 1450.00,
                'wholesale_price': 1297.00,
                'wholesale_min_qty': 1,
                'stock_quantity': 10,
            },
            {
                'name': 'V & A 250ml',
                'sku': 'BR-VA-250',
                'cost_price': 296.00,
                'selling_price': 350.00,
                'wholesale_price': 310.00,
                'wholesale_min_qty': 1,
                'stock_quantity': 10,
            },
            {
                'name': 'V & A 750ml',
                'sku': 'BR-VA-750',
                'cost_price': 768.00,
                'selling_price': 900.00,
                'wholesale_price': 800.00,
                'wholesale_min_qty': 1,
                'stock_quantity': 10,
            },
        ]

        created_count = 0
        for product_data in products_data:
            product, created = Product.objects.get_or_create(
                sku=product_data['sku'],
                defaults={
                    'name': product_data['name'],
                    'category': brandy_category,
                    'cost_price': product_data['cost_price'],
                    'selling_price': product_data['selling_price'],
                    'wholesale_price': product_data['wholesale_price'],
                    'wholesale_min_qty': product_data['wholesale_min_qty'],
                    'stock_quantity': product_data['stock_quantity'],
                    'low_stock_threshold': 10,
                    'is_active': True,
                }
            )
            if created:
                created_count += 1
                self.stdout.write(self.style.SUCCESS(f'Created product: {product.name}'))

        self.stdout.write(self.style.SUCCESS(f'Successfully created {created_count} brandy products'))