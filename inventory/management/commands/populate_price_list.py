from django.core.management.base import BaseCommand
from inventory.models import Product, Category

class Command(BaseCommand):
    help = 'Populate liquor shop products from price list'

    def handle(self, *args, **options):
        # Define categories
        categories_data = [
            {'name': 'GINS', 'description': 'Gin products'},
            {'name': 'WHISKEYS', 'description': 'Whiskey products'},
            {'name': 'VODKAS', 'description': 'Vodka products'},
            {'name': 'BEERS', 'description': 'Beer products'},
            {'name': 'ENERGY DRINKS', 'description': 'Energy drink products'},
            {'name': 'SOFT DRINKS', 'description': 'Soft drink products'},
            {'name': 'WINES', 'description': 'Wine products'},
        ]

        # Create categories
        categories = {}
        for cat_data in categories_data:
            category, created = Category.objects.get_or_create(
                name=cat_data['name'],
                defaults={'description': cat_data['description']}
            )
            categories[cat_data['name']] = category
            if created:
                self.stdout.write(self.style.SUCCESS(f'Created category: {category.name}'))

        # Product data
        gins_products = [
            {'name': 'Best Gin (250ml)', 'cost_price': 268, 'wholesale_price': 280, 'selling_price': 350},
            {'name': 'Best Gin (750ml)', 'cost_price': 753.96, 'wholesale_price': 785, 'selling_price': 950},
            {'name': 'Gilbeys Gin (250ml)', 'cost_price': 416, 'wholesale_price': 435, 'selling_price': 520},
            {'name': 'Gilbeys Gin (350ml)', 'cost_price': 576, 'wholesale_price': 603, 'selling_price': 720},
            {'name': 'Gilbeys Gin (750ml)', 'cost_price': 1240, 'wholesale_price': 1295, 'selling_price': 1500},
            {'name': 'Chrome Gin (250ml)', 'cost_price': 208, 'wholesale_price': 217, 'selling_price': 260},
            {'name': 'Chrome Gin (750ml)', 'cost_price': 560, 'wholesale_price': 582, 'selling_price': 700},
        ]

        whiskeys_products = [
            {'name': 'Bond 7 (250ml)', 'cost_price': 416, 'wholesale_price': 435, 'selling_price': 500},
            {'name': 'Bond 7 (350ml)', 'cost_price': 576, 'wholesale_price': 603, 'selling_price': 700},
            {'name': 'Bond 7 (750ml)', 'cost_price': 1240, 'wholesale_price': 1297, 'selling_price': 1400},
            {'name': 'Hunters (250ml)', 'cost_price': 308.45, 'wholesale_price': 328, 'selling_price': 380},
            {'name': 'Hunters (350ml)', 'cost_price': 444, 'wholesale_price': 459, 'selling_price': 500},
            {'name': 'Hunters (750ml)', 'cost_price': 938.7, 'wholesale_price': 970, 'selling_price': 1100},
            {'name': 'VAT 69 (750ml)', 'cost_price': 1400, 'wholesale_price': 1460, 'selling_price': 1650},
            {'name': 'Red Label (350ml)', 'cost_price': 864, 'wholesale_price': 898, 'selling_price': 1200},
            {'name': 'Black & White (350ml)', 'cost_price': 576, 'wholesale_price': 603, 'selling_price': 700},
            {'name': 'Black & White (750ml)', 'cost_price': 1120, 'wholesale_price': 1172, 'selling_price': 1400},
            {'name': 'Grants (750ml)', 'cost_price': 1472, 'wholesale_price': 1700, 'selling_price': 1900},
            {'name': 'V&A (350ml)', 'cost_price': 752, 'wholesale_price': 840, 'selling_price': 950},
        ]

        vodkas_products = [
            {'name': 'Chrome Vodka (250ml)', 'cost_price': 208, 'wholesale_price': 217, 'selling_price': 260},
            {'name': 'Chrome Vodka (750ml)', 'cost_price': 560, 'wholesale_price': 582, 'selling_price': 700},
            {'name': 'Triple Ace Vodka (750ml)', 'cost_price': 514, 'wholesale_price': 565, 'selling_price': 680},
            {'name': 'Triple Ace Vodka (250ml)', 'cost_price': 195, 'wholesale_price': 215, 'selling_price': 250},
            {'name': 'Kibao Vodka (250ml)', 'cost_price': 231.71, 'wholesale_price': 242, 'selling_price': 270},
            {'name': 'Kibao Vodka (750ml)', 'cost_price': 654, 'wholesale_price': 679, 'selling_price': 750},
        ]

        beers_products = [
            {'name': 'Guinness Can', 'cost_price': 5109, 'wholesale_price': 5200, 'selling_price': 230},
            {'name': 'Tusker Lager (500ml)', 'cost_price': 4225, 'wholesale_price': 4540, 'selling_price': 200},
            {'name': 'Tusker Cider (500ml)', 'cost_price': 5652, 'wholesale_price': 5660, 'selling_price': 250},
            {'name': 'Tusker Cider Small', 'cost_price': 234, 'wholesale_price': 245, 'selling_price': 250},
            {'name': 'Bell Lager', 'cost_price': 4225, 'wholesale_price': 4340, 'selling_price': 200},
            {'name': 'Whitecap Can', 'cost_price': 4888, 'wholesale_price': 4578, 'selling_price': 230},
            {'name': 'Whitecap Can Small', 'cost_price': 217, 'wholesale_price': 230, 'selling_price': 240},
            {'name': 'Tusker Lager Can Small', 'cost_price': 234, 'wholesale_price': 245, 'selling_price': 250},
            {'name': 'Guinness Can Small', 'cost_price': 217, 'wholesale_price': 235, 'selling_price': 250},
            {'name': 'Lemonus Can', 'cost_price': 217, 'wholesale_price': 235, 'selling_price': 250},
            {'name': 'Energy Can', 'cost_price': 176, 'wholesale_price': 200, 'selling_price': 230},
            {'name': 'Pineapple Punch', 'cost_price': 176, 'wholesale_price': 200, 'selling_price': 230},
            {'name': 'Orange Juice (1.5L)', 'cost_price': 345.98, 'wholesale_price': 370, 'selling_price': 400},
        ]

        energy_drinks_products = [
            {'name': 'Bravado', 'cost_price': 335, 'wholesale_price': 370, 'selling_price': 50},
            {'name': 'Topical', 'cost_price': 320, 'wholesale_price': 370, 'selling_price': 50},
            {'name': 'Delmonte Mango', 'cost_price': 247, 'wholesale_price': 260, 'selling_price': 300},
            {'name': 'Delmonte Tropical', 'cost_price': 246, 'wholesale_price': 260, 'selling_price': 300},
            {'name': 'Delmonte Pineapple', 'cost_price': 246, 'wholesale_price': 260, 'selling_price': 300},
        ]

        soft_drinks_products = [
            {'name': 'Alvaro (330ml)', 'cost_price': 122, 'wholesale_price': 140, 'selling_price': 150},
            {'name': 'Red Bull', 'cost_price': 182, 'wholesale_price': 200, 'selling_price': 230},
        ]

        wines_products = [
            {'name': 'Caprice Red', 'cost_price': 921.24, 'wholesale_price': 980, 'selling_price': 1100},
            {'name': 'Caprice White', 'cost_price': 921.13, 'wholesale_price': 980, 'selling_price': 1100},
            {'name': '4th Street', 'cost_price': 914, 'wholesale_price': 980, 'selling_price': 1200},
            {'name': 'Kingfisher', 'cost_price': 193, 'wholesale_price': 205, 'selling_price': 230},
        ]

        # Map categories to products
        products_data = {
            'GINS': gins_products,
            'WHISKEYS': whiskeys_products,
            'VODKAS': vodkas_products,
            'BEERS': beers_products,
            'ENERGY DRINKS': energy_drinks_products,
            'SOFT DRINKS': soft_drinks_products,
            'WINES': wines_products,
        }

        created_count = 0

        # Create products
        for cat_name, products in products_data.items():
            category = categories[cat_name]
            for product_data in products:
                # Generate SKU
                name_clean = product_data['name'].replace(' ', '-').replace('(', '').replace(')', '').replace('ml', '').replace('L', '').replace('.', '').upper()
                sku = f"{cat_name[:3].upper()}-{name_clean}"

                product, created = Product.objects.get_or_create(
                    sku=sku,
                    defaults={
                        'name': product_data['name'],
                        'category': category,
                        'cost_price': product_data['cost_price'],
                        'selling_price': product_data['selling_price'],
                        'wholesale_price': product_data['wholesale_price'],
                        'wholesale_min_qty': 10,
                        'stock_quantity': 0,
                        'low_stock_threshold': 10,
                        'is_active': True,
                    }
                )
                if created:
                    created_count += 1
                    self.stdout.write(self.style.SUCCESS(f'Created product: {product.name}'))

        self.stdout.write(self.style.SUCCESS(f'Successfully created {created_count} products'))