from django.core.management.base import BaseCommand
from inventory.models import Product, Category

class Command(BaseCommand):
    help = 'Populate brandy and spirits products'

    def handle(self, *args, **options):
        # Get the categories
        try:
            brandy_category = Category.objects.get(name='Brandy')
            spirits_category = Category.objects.get(name='Spirits')
        except Category.DoesNotExist as e:
            self.stdout.write(self.style.ERROR(f'Category does not exist: {e}'))
            return

        # Brandy products data
        brandy_products = [
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

        # Spirits products data
        spirits_products = [
            {
                'name': 'County 250ml',
                'sku': 'SP-COU-250',
                'cost_price': 239.44,
                'selling_price': 280.00,
                'wholesale_price': 245.00,
                'wholesale_min_qty': 1,
                'stock_quantity': 10,
            },
            {
                'name': 'County 750ml',
                'sku': 'SP-COU-750',
                'cost_price': 670.00,
                'selling_price': 750.00,
                'wholesale_price': 694.00,
                'wholesale_min_qty': 1,
                'stock_quantity': 10,
            },
            {
                'name': 'Captain Morgan 250ml',
                'sku': 'SP-CM-250',
                'cost_price': 336.00,
                'selling_price': 400.00,
                'wholesale_price': 349.00,
                'wholesale_min_qty': 1,
                'stock_quantity': 10,
            },
            {
                'name': 'Captain Morgan 750ml',
                'sku': 'SP-CM-750',
                'cost_price': 920.00,
                'selling_price': 1150.00,
                'wholesale_price': 942.00,
                'wholesale_min_qty': 1,
                'stock_quantity': 10,
            },
            {
                'name': 'KC Pineapple 250ml',
                'sku': 'SP-KC-PIN-250',
                'cost_price': 256.00,
                'selling_price': 300.00,
                'wholesale_price': 267.00,
                'wholesale_min_qty': 1,
                'stock_quantity': 10,
            },
            {
                'name': 'KC Africa 750ml',
                'sku': 'SP-KC-AFR-750',
                'cost_price': 672.00,
                'selling_price': 810.00,
                'wholesale_price': 710.00,
                'wholesale_min_qty': 1,
                'stock_quantity': 10,
            },
            {
                'name': 'KC Lemon & Ginger 250ml',
                'sku': 'SP-KC-LG-250',
                'cost_price': 256.00,
                'selling_price': 310.00,
                'wholesale_price': 267.00,
                'wholesale_min_qty': 1,
                'stock_quantity': 10,
            },
            {
                'name': 'KC Lemon & Ginger 750ml',
                'sku': 'SP-KC-LG-750',
                'cost_price': 672.00,
                'selling_price': 810.00,
                'wholesale_price': 717.00,
                'wholesale_min_qty': 1,
                'stock_quantity': 10,
            },
            {
                'name': 'KC Smooth 250ml',
                'sku': 'SP-KC-SM-250',
                'cost_price': 352.00,
                'selling_price': 410.00,
                'wholesale_price': 367.00,
                'wholesale_min_qty': 1,
                'stock_quantity': 10,
            },
            {
                'name': 'KC Smooth 750ml',
                'sku': 'SP-KC-SM-750',
                'cost_price': 672.00,
                'selling_price': 810.00,
                'wholesale_price': 710.00,
                'wholesale_min_qty': 1,
                'stock_quantity': 10,
            },
            {
                'name': 'General Meakins 250ml',
                'sku': 'SP-GM-250',
                'cost_price': 217.79,
                'selling_price': 270.00,
                'wholesale_price': 235.00,
                'wholesale_min_qty': 1,
                'stock_quantity': 10,
            },
            {
                'name': 'General Meakins 750ml',
                'sku': 'SP-GM-750',
                'cost_price': 644.88,
                'selling_price': 750.00,
                'wholesale_price': 675.00,
                'wholesale_min_qty': 1,
                'stock_quantity': 10,
            },
            {
                'name': 'Kenya King 250ml',
                'sku': 'SP-KK-250',
                'cost_price': 221.40,
                'selling_price': 270.00,
                'wholesale_price': 239.00,
                'wholesale_min_qty': 1,
                'stock_quantity': 10,
            },
            {
                'name': 'Kenya King 750ml',
                'sku': 'SP-KK-750',
                'cost_price': 650.35,
                'selling_price': 750.00,
                'wholesale_price': 690.00,
                'wholesale_min_qty': 1,
                'stock_quantity': 10,
            },
            {
                'name': 'Kane Extra 250ml',
                'sku': 'SP-KE-250',
                'cost_price': 192.00,
                'selling_price': 250.00,
                'wholesale_price': 210.00,
                'wholesale_min_qty': 1,
                'stock_quantity': 10,
            },
            {
                'name': 'Kane Extra 750ml',
                'sku': 'SP-KE-750',
                'cost_price': 540.00,
                'selling_price': 650.00,
                'wholesale_price': 565.00,
                'wholesale_min_qty': 1,
                'stock_quantity': 10,
            },
            {
                'name': 'Orijin 250ml',
                'sku': 'SP-ORI-250',
                'cost_price': 216.00,
                'selling_price': 280.00,
                'wholesale_price': 242.00,
                'wholesale_min_qty': 1,
                'stock_quantity': 10,
            },
        ]

        created_count = 0

        # Create brandy products
        for product_data in brandy_products:
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
                self.stdout.write(self.style.SUCCESS(f'Created brandy product: {product.name}'))

        # Create spirits products
        for product_data in spirits_products:
            product, created = Product.objects.get_or_create(
                sku=product_data['sku'],
                defaults={
                    'name': product_data['name'],
                    'category': spirits_category,
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
                self.stdout.write(self.style.SUCCESS(f'Created spirits product: {product.name}'))

        self.stdout.write(self.style.SUCCESS(f'Successfully created {created_count} liquor products'))