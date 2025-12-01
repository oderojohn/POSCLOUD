from django.core.management.base import BaseCommand
from inventory.models import Category, Product
from decimal import Decimal

class Command(BaseCommand):
    help = 'Populate inventory with 10 categories and 10 products each'

    def handle(self, *args, **options):
        # Define 10 categories
        categories_data = [
            {'name': 'Electronics', 'description': 'Electronic devices and gadgets'},
            {'name': 'Clothing', 'description': 'Apparel and fashion items'},
            {'name': 'Groceries', 'description': 'Food and household items'},
            {'name': 'Books', 'description': 'Books and educational materials'},
            {'name': 'Home & Garden', 'description': 'Home improvement and gardening'},
            {'name': 'Sports', 'description': 'Sports equipment and apparel'},
            {'name': 'Beauty', 'description': 'Cosmetics and personal care'},
            {'name': 'Automotive', 'description': 'Car parts and accessories'},
            {'name': 'Toys', 'description': 'Toys and games for children'},
            {'name': 'Health', 'description': 'Health and wellness products'},
        ]

        # Create categories
        categories = []
        for i, cat_data in enumerate(categories_data, 1):
            category, created = Category.objects.get_or_create(
                name=cat_data['name'],
                defaults={'description': cat_data['description']}
            )
            categories.append(category)
            if created:
                self.stdout.write(f'Created category: {category.name}')
            else:
                self.stdout.write(f'Category already exists: {category.name}')

        # Define products for each category
        products_data = {
            'Electronics': [
                {'name': 'Wireless Headphones', 'code': 'ELEC001', 'price': 2500.00, 'cost_price': 1800.00, 'stock': 50},
                {'name': 'Smartphone Charger', 'code': 'ELEC002', 'price': 800.00, 'cost_price': 500.00, 'stock': 100},
                {'name': 'Bluetooth Speaker', 'code': 'ELEC003', 'price': 1500.00, 'cost_price': 1000.00, 'stock': 30},
                {'name': 'USB Flash Drive 32GB', 'code': 'ELEC004', 'price': 600.00, 'cost_price': 350.00, 'stock': 75},
                {'name': 'Power Bank 10000mAh', 'code': 'ELEC005', 'price': 1200.00, 'cost_price': 800.00, 'stock': 40},
                {'name': 'Wireless Mouse', 'code': 'ELEC006', 'price': 450.00, 'cost_price': 250.00, 'stock': 60},
                {'name': 'HDMI Cable 2M', 'code': 'ELEC007', 'price': 300.00, 'cost_price': 150.00, 'stock': 80},
                {'name': 'Phone Case', 'code': 'ELEC008', 'price': 250.00, 'cost_price': 120.00, 'stock': 120},
                {'name': 'Screen Protector', 'code': 'ELEC009', 'price': 150.00, 'cost_price': 70.00, 'stock': 150},
                {'name': 'Earphones', 'code': 'ELEC010', 'price': 400.00, 'cost_price': 200.00, 'stock': 90},
            ],
            'Clothing': [
                {'name': 'Cotton T-Shirt', 'code': 'CLOTH001', 'price': 350.00, 'cost_price': 180.00, 'stock': 200},
                {'name': 'Jeans Pants', 'code': 'CLOTH002', 'price': 1200.00, 'cost_price': 700.00, 'stock': 80},
                {'name': 'Winter Jacket', 'code': 'CLOTH003', 'price': 2500.00, 'cost_price': 1500.00, 'stock': 40},
                {'name': 'Running Shoes', 'code': 'CLOTH004', 'price': 1800.00, 'cost_price': 1000.00, 'stock': 60},
                {'name': 'Baseball Cap', 'code': 'CLOTH005', 'price': 250.00, 'cost_price': 120.00, 'stock': 100},
                {'name': 'Leather Belt', 'code': 'CLOTH006', 'price': 400.00, 'cost_price': 200.00, 'stock': 70},
                {'name': 'Wool Socks Pack', 'code': 'CLOTH007', 'price': 150.00, 'cost_price': 80.00, 'stock': 150},
                {'name': 'Summer Dress', 'code': 'CLOTH008', 'price': 800.00, 'cost_price': 400.00, 'stock': 50},
                {'name': 'Denim Jacket', 'code': 'CLOTH009', 'price': 1600.00, 'cost_price': 900.00, 'stock': 35},
                {'name': 'Swim Trunks', 'code': 'CLOTH010', 'price': 450.00, 'cost_price': 220.00, 'stock': 65},
            ],
            'Groceries': [
                {'name': 'Rice 5KG Bag', 'code': 'GROC001', 'price': 280.00, 'cost_price': 220.00, 'stock': 100},
                {'name': 'Cooking Oil 1L', 'code': 'GROC002', 'price': 180.00, 'cost_price': 130.00, 'stock': 120},
                {'name': 'Sugar 2KG', 'code': 'GROC003', 'price': 120.00, 'cost_price': 90.00, 'stock': 150},
                {'name': 'Tea Bags Pack', 'code': 'GROC004', 'price': 85.00, 'cost_price': 55.00, 'stock': 200},
                {'name': 'Coffee Powder 200G', 'code': 'GROC005', 'price': 250.00, 'cost_price': 180.00, 'stock': 80},
                {'name': 'Milk Powder 400G', 'code': 'GROC006', 'price': 320.00, 'cost_price': 250.00, 'stock': 90},
                {'name': 'Bread Loaf', 'code': 'GROC007', 'price': 45.00, 'cost_price': 30.00, 'stock': 300},
                {'name': 'Eggs Tray (12)', 'code': 'GROC008', 'price': 180.00, 'cost_price': 140.00, 'stock': 50},
                {'name': 'Tomato Sauce 500G', 'code': 'GROC009', 'price': 95.00, 'cost_price': 65.00, 'stock': 110},
                {'name': 'Spaghetti 500G', 'code': 'GROC010', 'price': 70.00, 'cost_price': 45.00, 'stock': 130},
            ],
            'Books': [
                {'name': 'Python Programming', 'code': 'BOOK001', 'price': 1200.00, 'cost_price': 800.00, 'stock': 25},
                {'name': 'Web Development Guide', 'code': 'BOOK002', 'price': 950.00, 'cost_price': 650.00, 'stock': 30},
                {'name': 'Data Science Handbook', 'code': 'BOOK003', 'price': 1500.00, 'cost_price': 1000.00, 'stock': 20},
                {'name': 'Business Management', 'code': 'BOOK004', 'price': 1100.00, 'cost_price': 750.00, 'stock': 35},
                {'name': 'English Grammar', 'code': 'BOOK005', 'price': 400.00, 'cost_price': 250.00, 'stock': 60},
                {'name': 'Mathematics Textbook', 'code': 'BOOK006', 'price': 800.00, 'cost_price': 550.00, 'stock': 40},
                {'name': 'History of Africa', 'code': 'BOOK007', 'price': 650.00, 'cost_price': 400.00, 'stock': 45},
                {'name': 'Cooking Recipes', 'code': 'BOOK008', 'price': 350.00, 'cost_price': 200.00, 'stock': 70},
                {'name': 'Children Stories', 'code': 'BOOK009', 'price': 250.00, 'cost_price': 150.00, 'stock': 80},
                {'name': 'Art and Design', 'code': 'BOOK010', 'price': 900.00, 'cost_price': 600.00, 'stock': 28},
            ],
            'Home & Garden': [
                {'name': 'Garden Hose 10M', 'code': 'HOME001', 'price': 450.00, 'cost_price': 300.00, 'stock': 40},
                {'name': 'Paint Brush Set', 'code': 'HOME002', 'price': 180.00, 'cost_price': 120.00, 'stock': 60},
                {'name': 'Hammer 16oz', 'code': 'HOME003', 'price': 350.00, 'cost_price': 220.00, 'stock': 45},
                {'name': 'Screwdriver Set', 'code': 'HOME004', 'price': 280.00, 'cost_price': 180.00, 'stock': 55},
                {'name': 'Extension Cord 5M', 'code': 'HOME005', 'price': 200.00, 'cost_price': 130.00, 'stock': 70},
                {'name': 'Wall Clock', 'code': 'HOME006', 'price': 400.00, 'cost_price': 250.00, 'stock': 35},
                {'name': 'Picture Frame 8x10', 'code': 'HOME007', 'price': 150.00, 'cost_price': 90.00, 'stock': 80},
                {'name': 'Floor Mat', 'code': 'HOME008', 'price': 300.00, 'cost_price': 180.00, 'stock': 50},
                {'name': 'Laundry Basket', 'code': 'HOME009', 'price': 250.00, 'cost_price': 150.00, 'stock': 65},
                {'name': 'Storage Box', 'code': 'HOME010', 'price': 180.00, 'cost_price': 110.00, 'stock': 75},
            ],
            'Sports': [
                {'name': 'Basketball', 'code': 'SPORT001', 'price': 800.00, 'cost_price': 550.00, 'stock': 30},
                {'name': 'Football', 'code': 'SPORT002', 'price': 450.00, 'cost_price': 300.00, 'stock': 40},
                {'name': 'Tennis Racket', 'code': 'SPORT003', 'price': 1200.00, 'cost_price': 800.00, 'stock': 25},
                {'name': 'Yoga Mat', 'code': 'SPORT004', 'price': 350.00, 'cost_price': 220.00, 'stock': 50},
                {'name': 'Dumbbells 5KG Pair', 'code': 'SPORT005', 'price': 600.00, 'cost_price': 400.00, 'stock': 35},
                {'name': 'Swimming Goggles', 'code': 'SPORT006', 'price': 150.00, 'cost_price': 90.00, 'stock': 60},
                {'name': 'Badminton Set', 'code': 'SPORT007', 'price': 700.00, 'cost_price': 450.00, 'stock': 28},
                {'name': 'Cycling Helmet', 'code': 'SPORT008', 'price': 550.00, 'cost_price': 350.00, 'stock': 32},
                {'name': 'Boxing Gloves', 'code': 'SPORT009', 'price': 650.00, 'cost_price': 420.00, 'stock': 25},
                {'name': 'Skipping Rope', 'code': 'SPORT010', 'price': 120.00, 'cost_price': 70.00, 'stock': 80},
            ],
            'Beauty': [
                {'name': 'Shampoo 500ML', 'code': 'BEAU001', 'price': 180.00, 'cost_price': 120.00, 'stock': 90},
                {'name': 'Face Cream 50G', 'code': 'BEAU002', 'price': 250.00, 'cost_price': 160.00, 'stock': 70},
                {'name': 'Lipstick', 'code': 'BEAU003', 'price': 150.00, 'cost_price': 90.00, 'stock': 100},
                {'name': 'Nail Polish', 'code': 'BEAU004', 'price': 80.00, 'cost_price': 45.00, 'stock': 120},
                {'name': 'Hair Brush', 'code': 'BEAU005', 'price': 120.00, 'cost_price': 70.00, 'stock': 85},
                {'name': 'Body Lotion 200ML', 'code': 'BEAU006', 'price': 220.00, 'cost_price': 140.00, 'stock': 65},
                {'name': 'Perfume 50ML', 'code': 'BEAU007', 'price': 450.00, 'cost_price': 280.00, 'stock': 40},
                {'name': 'Makeup Mirror', 'code': 'BEAU008', 'price': 180.00, 'cost_price': 110.00, 'stock': 55},
                {'name': 'Facial Mask Pack', 'code': 'BEAU009', 'price': 95.00, 'cost_price': 55.00, 'stock': 110},
                {'name': 'Hair Clips Set', 'code': 'BEAU010', 'price': 60.00, 'cost_price': 30.00, 'stock': 140},
            ],
            'Automotive': [
                {'name': 'Car Air Freshener', 'code': 'AUTO001', 'price': 80.00, 'cost_price': 45.00, 'stock': 100},
                {'name': 'Windshield Wipers', 'code': 'AUTO002', 'price': 350.00, 'cost_price': 220.00, 'stock': 40},
                {'name': 'Car Wash Soap', 'code': 'AUTO003', 'price': 120.00, 'cost_price': 75.00, 'stock': 80},
                {'name': 'Tire Pressure Gauge', 'code': 'AUTO004', 'price': 150.00, 'cost_price': 90.00, 'stock': 60},
                {'name': 'Car Vacuum Cleaner', 'code': 'AUTO005', 'price': 450.00, 'cost_price': 300.00, 'stock': 25},
                {'name': 'Seat Covers', 'code': 'AUTO006', 'price': 650.00, 'cost_price': 400.00, 'stock': 30},
                {'name': 'Car Battery 12V', 'code': 'AUTO007', 'price': 2800.00, 'cost_price': 2000.00, 'stock': 15},
                {'name': 'Engine Oil 1L', 'code': 'AUTO008', 'price': 320.00, 'cost_price': 220.00, 'stock': 50},
                {'name': 'Car Mats Set', 'code': 'AUTO009', 'price': 400.00, 'cost_price': 250.00, 'stock': 35},
                {'name': 'Jump Starter', 'code': 'AUTO010', 'price': 850.00, 'cost_price': 550.00, 'stock': 20},
            ],
            'Toys': [
                {'name': 'Building Blocks Set', 'code': 'TOYS001', 'price': 350.00, 'cost_price': 220.00, 'stock': 45},
                {'name': 'Stuffed Teddy Bear', 'code': 'TOYS002', 'price': 280.00, 'cost_price': 160.00, 'stock': 60},
                {'name': 'Puzzle 500 Pieces', 'code': 'TOYS003', 'price': 180.00, 'cost_price': 110.00, 'stock': 40},
                {'name': 'Remote Control Car', 'code': 'TOYS004', 'price': 450.00, 'cost_price': 300.00, 'stock': 35},
                {'name': 'Board Game', 'code': 'TOYS005', 'price': 320.00, 'cost_price': 200.00, 'stock': 50},
                {'name': 'Coloring Book Set', 'code': 'TOYS006', 'price': 120.00, 'cost_price': 70.00, 'stock': 80},
                {'name': 'Action Figure', 'code': 'TOYS007', 'price': 150.00, 'cost_price': 90.00, 'stock': 70},
                {'name': 'Doll House', 'code': 'TOYS008', 'price': 650.00, 'cost_price': 400.00, 'stock': 25},
                {'name': 'Balloon Set', 'code': 'TOYS009', 'price': 85.00, 'cost_price': 50.00, 'stock': 90},
                {'name': 'Kite', 'code': 'TOYS010', 'price': 180.00, 'cost_price': 110.00, 'stock': 55},
            ],
            'Health': [
                {'name': 'Multivitamin Tablets', 'code': 'HLTH001', 'price': 280.00, 'cost_price': 180.00, 'stock': 100},
                {'name': 'Pain Relief Tablets', 'code': 'HLTH002', 'price': 45.00, 'cost_price': 25.00, 'stock': 200},
                {'name': 'Blood Pressure Monitor', 'code': 'HLTH003', 'price': 850.00, 'cost_price': 550.00, 'stock': 30},
                {'name': 'Thermometer Digital', 'code': 'HLTH004', 'price': 150.00, 'cost_price': 90.00, 'stock': 60},
                {'name': 'First Aid Kit', 'code': 'HLTH005', 'price': 320.00, 'cost_price': 200.00, 'stock': 40},
                {'name': 'Face Mask Pack', 'code': 'HLTH006', 'price': 65.00, 'cost_price': 35.00, 'stock': 150},
                {'name': 'Hand Sanitizer 500ML', 'code': 'HLTH007', 'price': 95.00, 'cost_price': 60.00, 'stock': 120},
                {'name': 'Vitamin C Tablets', 'code': 'HLTH008', 'price': 120.00, 'cost_price': 75.00, 'stock': 90},
                {'name': 'Wound Dressing', 'code': 'HLTH009', 'price': 35.00, 'cost_price': 20.00, 'stock': 180},
                {'name': 'Health Scale', 'code': 'HLTH010', 'price': 450.00, 'cost_price': 280.00, 'stock': 35},
            ],
        }

        # Create products for each category
        total_products_created = 0
        for category in categories:
            category_products = products_data.get(category.name, [])
            for product_data in category_products:
                product, created = Product.objects.get_or_create(
                    sku=product_data['code'],  # Use 'sku' field instead of 'code'
                    defaults={
                        'name': product_data['name'],
                        'category': category,
                        'selling_price': Decimal(str(product_data['price'])),
                        'cost_price': Decimal(str(product_data['cost_price'])),
                        'stock_quantity': product_data['stock'],
                    }
                )
                if created:
                    self.stdout.write(f'Created product: {product.sku} - {product.name}')
                    total_products_created += 1
                else:
                    self.stdout.write(f'Product already exists: {product.sku} - {product.name}')

        self.stdout.write(
            self.style.SUCCESS(
                f'Successfully populated inventory with {len(categories)} categories and {total_products_created} products!'
            )
        )