from django.core.management.base import BaseCommand
from inventory.models import Category, Product
from decimal import Decimal

class Command(BaseCommand):
    help = 'Add 10 categories with 10 products each for liquor shop'

    def handle(self, *args, **options):
        self.stdout.write('Adding 10 liquor categories with 10 products each...')

        # Define 10 categories
        categories_data = [
            {'name': 'Red Wines', 'description': 'Premium red wines from around the world'},
            {'name': 'White Wines', 'description': 'Crisp and refreshing white wines'},
            {'name': 'Rosé Wines', 'description': 'Light and fruity rosé wines'},
            {'name': 'Sparkling Wines', 'description': 'Champagne and sparkling wines'},
            {'name': 'Whiskey', 'description': 'Scotch, Irish, and American whiskeys'},
            {'name': 'Vodka', 'description': 'Premium vodkas from various distilleries'},
            {'name': 'Gin', 'description': 'London dry and flavored gins'},
            {'name': 'Rum', 'description': 'Light, dark, and spiced rums'},
            {'name': 'Tequila', 'description': 'Blanco, reposado, and añejo tequilas'},
            {'name': 'Brandy', 'description': 'Cognac, armagnac, and other brandies'},
        ]

        categories = {}
        for cat_data in categories_data:
            category, created = Category.objects.get_or_create(
                name=cat_data['name'],
                defaults={'description': cat_data['description']}
            )
            categories[cat_data['name']] = category
            if created:
                self.stdout.write(f'Created category: {category.name}')

        # Products data for each category
        products_data = []

        # Red Wines
        red_wines = [
            {'sku': 'RED-001', 'name': 'Cabernet Sauvignon Reserve', 'cost': 2500, 'sell': 3200, 'desc': 'Full-bodied red wine, 750ml'},
            {'sku': 'RED-002', 'name': 'Merlot Classic', 'cost': 2200, 'sell': 2900, 'desc': 'Smooth and velvety merlot, 750ml'},
            {'sku': 'RED-003', 'name': 'Pinot Noir Estate', 'cost': 2800, 'sell': 3600, 'desc': 'Elegant pinot noir, 750ml'},
            {'sku': 'RED-004', 'name': 'Shiraz Premium', 'cost': 2400, 'sell': 3100, 'desc': 'Bold and spicy shiraz, 750ml'},
            {'sku': 'RED-005', 'name': 'Malbec Reserve', 'cost': 2600, 'sell': 3400, 'desc': 'Rich malbec from Argentina, 750ml'},
            {'sku': 'RED-006', 'name': 'Zinfandel Old Vine', 'cost': 2700, 'sell': 3500, 'desc': 'Jammy zinfandel, 750ml'},
            {'sku': 'RED-007', 'name': 'Bordeaux Blend', 'cost': 3500, 'sell': 4500, 'desc': 'Classic Bordeaux blend, 750ml'},
            {'sku': 'RED-008', 'name': 'Sangiovese Riserva', 'cost': 2300, 'sell': 3000, 'desc': 'Italian sangiovese, 750ml'},
            {'sku': 'RED-009', 'name': 'Tempranillo Crianza', 'cost': 2100, 'sell': 2800, 'desc': 'Spanish tempranillo, 750ml'},
            {'sku': 'RED-010', 'name': 'Grenache Noir', 'cost': 2000, 'sell': 2700, 'desc': 'Fruity grenache, 750ml'},
        ]

        # White Wines
        white_wines = [
            {'sku': 'WHT-001', 'name': 'Chardonnay Reserve', 'cost': 2400, 'sell': 3100, 'desc': 'Buttery chardonnay, 750ml'},
            {'sku': 'WHT-002', 'name': 'Sauvignon Blanc', 'cost': 2200, 'sell': 2900, 'desc': 'Crisp sauvignon blanc, 750ml'},
            {'sku': 'WHT-003', 'name': 'Riesling Dry', 'cost': 2100, 'sell': 2800, 'desc': 'Off-dry riesling, 750ml'},
            {'sku': 'WHT-004', 'name': 'Pinot Grigio', 'cost': 2000, 'sell': 2700, 'desc': 'Light pinot grigio, 750ml'},
            {'sku': 'WHT-005', 'name': 'Gewürztraminer', 'cost': 2500, 'sell': 3300, 'desc': 'Aromatic gewürztraminer, 750ml'},
            {'sku': 'WHT-006', 'name': 'Semillon', 'cost': 2300, 'sell': 3000, 'desc': 'Elegant semillon, 750ml'},
            {'sku': 'WHT-007', 'name': 'Viognier', 'cost': 2600, 'sell': 3400, 'desc': 'Floral viognier, 750ml'},
            {'sku': 'WHT-008', 'name': 'Albariño', 'cost': 2400, 'sell': 3200, 'desc': 'Spanish albariño, 750ml'},
            {'sku': 'WHT-009', 'name': 'Verdicchio', 'cost': 2200, 'sell': 2900, 'desc': 'Italian verdicchio, 750ml'},
            {'sku': 'WHT-010', 'name': 'Grüner Veltliner', 'cost': 2300, 'sell': 3100, 'desc': 'Austrian grüner veltliner, 750ml'},
        ]

        # Rosé Wines
        rose_wines = [
            {'sku': 'ROS-001', 'name': 'Provence Rosé', 'cost': 2200, 'sell': 2900, 'desc': 'Classic Provence rosé, 750ml'},
            {'sku': 'ROS-002', 'name': 'Grenache Rosé', 'cost': 2000, 'sell': 2700, 'desc': 'Fruity grenache rosé, 750ml'},
            {'sku': 'ROS-003', 'name': 'Syrah Rosé', 'cost': 2100, 'sell': 2800, 'desc': 'Bold syrah rosé, 750ml'},
            {'sku': 'ROS-004', 'name': 'Tempranillo Rosé', 'cost': 1900, 'sell': 2600, 'desc': 'Spanish tempranillo rosé, 750ml'},
            {'sku': 'ROS-005', 'name': 'Zinfandel Rosé', 'cost': 2300, 'sell': 3000, 'desc': 'Blush zinfandel, 750ml'},
            {'sku': 'ROS-006', 'name': 'Sangiovese Rosé', 'cost': 2000, 'sell': 2700, 'desc': 'Italian sangiovese rosé, 750ml'},
            {'sku': 'ROS-007', 'name': 'Cabernet Rosé', 'cost': 2100, 'sell': 2800, 'desc': 'Light cabernet rosé, 750ml'},
            {'sku': 'ROS-008', 'name': 'Pinot Noir Rosé', 'cost': 2400, 'sell': 3200, 'desc': 'Elegant pinot noir rosé, 750ml'},
            {'sku': 'ROS-009', 'name': 'Malbec Rosé', 'cost': 2000, 'sell': 2700, 'desc': 'Argentinian malbec rosé, 750ml'},
            {'sku': 'ROS-010', 'name': 'Merlot Rosé', 'cost': 1900, 'sell': 2600, 'desc': 'Smooth merlot rosé, 750ml'},
        ]

        # Sparkling Wines
        sparkling_wines = [
            {'sku': 'SPK-001', 'name': 'Champagne Brut', 'cost': 5000, 'sell': 6500, 'desc': 'French champagne brut, 750ml'},
            {'sku': 'SPK-002', 'name': 'Cava Brut', 'cost': 2500, 'sell': 3300, 'desc': 'Spanish cava brut, 750ml'},
            {'sku': 'SPK-003', 'name': 'Prosecco Extra Dry', 'cost': 2200, 'sell': 2900, 'desc': 'Italian prosecco, 750ml'},
            {'sku': 'SPK-004', 'name': 'Crémant d\'Alsace', 'cost': 2800, 'sell': 3700, 'desc': 'French crémant, 750ml'},
            {'sku': 'SPK-005', 'name': 'Asti Spumante', 'cost': 2000, 'sell': 2700, 'desc': 'Sweet Italian sparkling, 750ml'},
            {'sku': 'SPK-006', 'name': 'Franciacorta', 'cost': 3500, 'sell': 4600, 'desc': 'Italian metodo classico, 750ml'},
            {'sku': 'SPK-007', 'name': 'English Sparkling', 'cost': 3000, 'sell': 4000, 'desc': 'English sparkling wine, 750ml'},
            {'sku': 'SPK-008', 'name': 'California Sparkling', 'cost': 2600, 'sell': 3400, 'desc': 'American sparkling wine, 750ml'},
            {'sku': 'SPK-009', 'name': 'Australian Sparkling', 'cost': 2400, 'sell': 3200, 'desc': 'Australian sparkling shiraz, 750ml'},
            {'sku': 'SPK-010', 'name': 'New Zealand Sparkling', 'cost': 2700, 'sell': 3600, 'desc': 'New Zealand méthode, 750ml'},
        ]

        # Whiskey
        whiskeys = [
            {'sku': 'WHK-001', 'name': 'Scotch Whisky 12YO', 'cost': 3500, 'sell': 4500, 'desc': '12-year-old scotch whisky, 750ml'},
            {'sku': 'WHK-002', 'name': 'Irish Whiskey Triple', 'cost': 3200, 'sell': 4200, 'desc': 'Triple distilled Irish whiskey, 750ml'},
            {'sku': 'WHK-003', 'name': 'Bourbon Classic', 'cost': 2800, 'sell': 3700, 'desc': 'Kentucky straight bourbon, 750ml'},
            {'sku': 'WHK-004', 'name': 'Rye Whiskey', 'cost': 3000, 'sell': 4000, 'desc': 'Canadian rye whiskey, 750ml'},
            {'sku': 'WHK-005', 'name': 'Single Malt 15YO', 'cost': 4500, 'sell': 5800, 'desc': '15-year-old single malt, 750ml'},
            {'sku': 'WHK-006', 'name': 'Blended Scotch', 'cost': 2500, 'sell': 3300, 'desc': 'Premium blended scotch, 750ml'},
            {'sku': 'WHK-007', 'name': 'Tennessee Whiskey', 'cost': 2900, 'sell': 3800, 'desc': 'Charcoal mellowed whiskey, 750ml'},
            {'sku': 'WHK-008', 'name': 'Japanese Whisky', 'cost': 3600, 'sell': 4700, 'desc': 'Japanese single malt, 750ml'},
            {'sku': 'WHK-009', 'name': 'Single Grain', 'cost': 2700, 'sell': 3600, 'desc': 'Single grain scotch, 750ml'},
            {'sku': 'WHK-010', 'name': 'Small Batch Bourbon', 'cost': 3300, 'sell': 4300, 'desc': 'Small batch bourbon, 750ml'},
        ]

        # Vodka
        vodkas = [
            {'sku': 'VOD-001', 'name': 'Premium Vodka', 'cost': 2200, 'sell': 2900, 'desc': 'Ultra-premium vodka, 750ml'},
            {'sku': 'VOD-002', 'name': 'Wheat Vodka', 'cost': 2000, 'sell': 2700, 'desc': 'Wheat-based vodka, 750ml'},
            {'sku': 'VOD-003', 'name': 'Potato Vodka', 'cost': 2400, 'sell': 3200, 'desc': 'Potato distilled vodka, 750ml'},
            {'sku': 'VOD-004', 'name': 'Citrus Infused', 'cost': 2500, 'sell': 3300, 'desc': 'Citrus-flavored vodka, 750ml'},
            {'sku': 'VOD-005', 'name': 'Vanilla Vodka', 'cost': 2300, 'sell': 3100, 'desc': 'Vanilla-infused vodka, 750ml'},
            {'sku': 'VOD-006', 'name': 'Pepper Vodka', 'cost': 2600, 'sell': 3400, 'desc': 'Black pepper vodka, 750ml'},
            {'sku': 'VOD-007', 'name': 'Organic Vodka', 'cost': 2700, 'sell': 3600, 'desc': 'Organic grain vodka, 750ml'},
            {'sku': 'VOD-008', 'name': 'Small Batch Vodka', 'cost': 2800, 'sell': 3700, 'desc': 'Small batch distilled vodka, 750ml'},
            {'sku': 'VOD-009', 'name': 'Filtered Vodka', 'cost': 2100, 'sell': 2800, 'desc': 'Charcoal filtered vodka, 750ml'},
            {'sku': 'VOD-010', 'name': 'Estate Vodka', 'cost': 3000, 'sell': 4000, 'desc': 'Estate-bottled vodka, 750ml'},
        ]

        # Gin
        gins = [
            {'sku': 'GIN-001', 'name': 'London Dry Gin', 'cost': 2200, 'sell': 2900, 'desc': 'Classic London dry gin, 750ml'},
            {'sku': 'GIN-002', 'name': 'Old Tom Gin', 'cost': 2400, 'sell': 3200, 'desc': 'Traditional old tom gin, 750ml'},
            {'sku': 'GIN-003', 'name': 'Navy Strength', 'cost': 2600, 'sell': 3400, 'desc': '57% ABV navy strength gin, 750ml'},
            {'sku': 'GIN-004', 'name': 'Sloe Gin', 'cost': 2300, 'sell': 3100, 'desc': 'Blackthorn berry gin, 750ml'},
            {'sku': 'GIN-005', 'name': 'Pink Gin', 'cost': 2500, 'sell': 3300, 'desc': 'Rose petal infused gin, 750ml'},
            {'sku': 'GIN-006', 'name': 'Cucumber Gin', 'cost': 2400, 'sell': 3200, 'desc': 'Cucumber botanical gin, 750ml'},
            {'sku': 'GIN-007', 'name': 'Juniper Forward', 'cost': 2200, 'sell': 2900, 'desc': 'Bold juniper gin, 750ml'},
            {'sku': 'GIN-008', 'name': 'Citrus Gin', 'cost': 2300, 'sell': 3100, 'desc': 'Citrus-forward gin, 750ml'},
            {'sku': 'GIN-009', 'name': 'Barrel Aged Gin', 'cost': 2800, 'sell': 3700, 'desc': 'Oak-aged gin, 750ml'},
            {'sku': 'GIN-010', 'name': 'Small Batch Gin', 'cost': 2700, 'sell': 3600, 'desc': 'Artisanal small batch gin, 750ml'},
        ]

        # Rum
        rums = [
            {'sku': 'RUM-001', 'name': 'White Rum Light', 'cost': 1800, 'sell': 2400, 'desc': 'Light white rum, 750ml'},
            {'sku': 'RUM-002', 'name': 'Gold Rum', 'cost': 2000, 'sell': 2700, 'desc': 'Golden aged rum, 750ml'},
            {'sku': 'RUM-003', 'name': 'Dark Rum', 'cost': 2200, 'sell': 2900, 'desc': 'Rich dark rum, 750ml'},
            {'sku': 'RUM-004', 'name': 'Spiced Rum', 'cost': 2100, 'sell': 2800, 'desc': 'Vanilla spiced rum, 750ml'},
            {'sku': 'RUM-005', 'name': 'Aged Rum 5YO', 'cost': 2500, 'sell': 3300, 'desc': '5-year-old aged rum, 750ml'},
            {'sku': 'RUM-006', 'name': 'Overproof Rum', 'cost': 2300, 'sell': 3100, 'desc': 'High-proof white rum, 750ml'},
            {'sku': 'RUM-007', 'name': 'Coconut Rum', 'cost': 1900, 'sell': 2600, 'desc': 'Coconut infused rum, 750ml'},
            {'sku': 'RUM-008', 'name': 'Black Rum', 'cost': 2400, 'sell': 3200, 'desc': 'Blackstrap molasses rum, 750ml'},
            {'sku': 'RUM-009', 'name': 'Single Estate Rum', 'cost': 2600, 'sell': 3400, 'desc': 'Single estate rum, 750ml'},
            {'sku': 'RUM-010', 'name': 'Small Batch Rum', 'cost': 2700, 'sell': 3600, 'desc': 'Small batch distilled rum, 750ml'},
        ]

        # Tequila
        tequilas = [
            {'sku': 'TEQ-001', 'name': 'Blanco Tequila', 'cost': 2200, 'sell': 2900, 'desc': 'Silver blanco tequila, 750ml'},
            {'sku': 'TEQ-002', 'name': 'Reposado Tequila', 'cost': 2400, 'sell': 3200, 'desc': 'Rested reposado tequila, 750ml'},
            {'sku': 'TEQ-003', 'name': 'Añejo Tequila', 'cost': 2800, 'sell': 3700, 'desc': 'Aged añejo tequila, 750ml'},
            {'sku': 'TEQ-004', 'name': 'Extra Añejo', 'cost': 3500, 'sell': 4600, 'desc': 'Extra aged tequila, 750ml'},
            {'sku': 'TEQ-005', 'name': 'Joven Tequila', 'cost': 2300, 'sell': 3100, 'desc': 'Gold joven tequila, 750ml'},
            {'sku': 'TEQ-006', 'name': 'Cristalino Tequila', 'cost': 2600, 'sell': 3400, 'desc': 'Crystal clear tequila, 750ml'},
            {'sku': 'TEQ-007', 'name': 'Highland Tequila', 'cost': 2500, 'sell': 3300, 'desc': 'Highland agave tequila, 750ml'},
            {'sku': 'TEQ-008', 'name': 'Lowland Tequila', 'cost': 2200, 'sell': 2900, 'desc': 'Lowland agave tequila, 750ml'},
            {'sku': 'TEQ-009', 'name': 'Organic Tequila', 'cost': 2700, 'sell': 3600, 'desc': 'Organic agave tequila, 750ml'},
            {'sku': 'TEQ-010', 'name': 'Small Batch Tequila', 'cost': 2900, 'sell': 3800, 'desc': 'Small batch tequila, 750ml'},
        ]

        # Brandy
        brandies = [
            {'sku': 'BRD-001', 'name': 'Cognac VS', 'cost': 3500, 'sell': 4500, 'desc': 'Very special cognac, 750ml'},
            {'sku': 'BRD-002', 'name': 'Cognac VSOP', 'cost': 4200, 'sell': 5500, 'desc': 'Very superior old pale cognac, 750ml'},
            {'sku': 'BRD-003', 'name': 'Cognac XO', 'cost': 5500, 'sell': 7200, 'desc': 'Extra old cognac, 750ml'},
            {'sku': 'BRD-004', 'name': 'Armagnac VSOP', 'cost': 3800, 'sell': 5000, 'desc': 'Armagnac brandy, 750ml'},
            {'sku': 'BRD-005', 'name': 'Spanish Brandy', 'cost': 2200, 'sell': 2900, 'desc': 'Solera system brandy, 750ml'},
            {'sku': 'BRD-006', 'name': 'Apple Brandy', 'cost': 2400, 'sell': 3200, 'desc': 'Calvados apple brandy, 750ml'},
            {'sku': 'BRD-007', 'name': 'Pisco Brandy', 'cost': 2000, 'sell': 2700, 'desc': 'Peruvian pisco, 750ml'},
            {'sku': 'BRD-008', 'name': 'Fruit Brandy', 'cost': 2300, 'sell': 3100, 'desc': 'Plum fruit brandy, 750ml'},
            {'sku': 'BRD-009', 'name': 'Grape Brandy', 'cost': 2100, 'sell': 2800, 'desc': 'Pure grape brandy, 750ml'},
            {'sku': 'BRD-010', 'name': 'Aged Brandy', 'cost': 2600, 'sell': 3400, 'desc': 'Oak-aged brandy, 750ml'},
        ]

        # Compile all products
        category_products = {
            'Red Wines': red_wines,
            'White Wines': white_wines,
            'Rosé Wines': rose_wines,
            'Sparkling Wines': sparkling_wines,
            'Whiskey': whiskeys,
            'Vodka': vodkas,
            'Gin': gins,
            'Rum': rums,
            'Tequila': tequilas,
            'Brandy': brandies,
        }

        for cat_name, products_list in category_products.items():
            for prod in products_list:
                products_data.append({
                    'sku': prod['sku'],
                    'name': prod['name'],
                    'category': categories[cat_name],
                    'cost_price': Decimal(str(prod['cost'])),
                    'selling_price': Decimal(str(prod['sell'])),
                    'wholesale_price': Decimal(str(prod['cost'] * 0.9)),  # 10% discount for wholesale
                    'wholesale_min_qty': 6,
                    'stock_quantity': 24,  # Default stock
                    'low_stock_threshold': 6,
                    'description': prod['desc'],
                })

        # Create products
        products_created = 0
        for prod_data in products_data:
            product, created = Product.objects.get_or_create(
                sku=prod_data['sku'],
                defaults=prod_data
            )
            if created:
                products_created += 1
                self.stdout.write(f'Created product: {product.name}')

        self.stdout.write(self.style.SUCCESS(f'Successfully added {len(categories)} categories and {products_created} products!'))