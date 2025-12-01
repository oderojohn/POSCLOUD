from django.test import TestCase
from django.urls import reverse
from rest_framework.test import APITestCase
from rest_framework import status
from decimal import Decimal
from datetime import date, datetime
from .models import Category, Product, Batch, StockMovement, Supplier, Purchase, PriceHistory, SalesHistory
from .serializers import CategorySerializer, ProductSerializer, BatchSerializer, StockMovementSerializer, PurchaseSerializer, PriceHistorySerializer, SalesHistorySerializer
from suppliers.models import Supplier as SupplierModel
from users.models import UserProfile
from customers.models import Customer
from django.contrib.auth.models import User


class CategoryModelTest(TestCase):
    def test_category_creation(self):
        category = Category.objects.create(name="Electronics", description="Electronic items")
        self.assertEqual(category.name, "Electronics")
        self.assertEqual(str(category), "Electronics")

    def test_category_unique_name(self):
        Category.objects.create(name="Electronics")
        with self.assertRaises(Exception):
            Category.objects.create(name="Electronics")


class ProductModelTest(TestCase):
    def setUp(self):
        self.category = Category.objects.create(name="Electronics")

    def test_product_creation(self):
        product = Product.objects.create(
            sku="PROD001",
            name="Laptop",
            category=self.category,
            cost_price=Decimal('500.00'),
            selling_price=Decimal('600.00'),
            stock_quantity=50,
            low_stock_threshold=10
        )
        self.assertEqual(product.sku, "PROD001")
        self.assertEqual(str(product), "PROD001 - Laptop")
        self.assertFalse(product.is_low_stock)

    def test_product_low_stock(self):
        product = Product.objects.create(
            sku="PROD002",
            name="Mouse",
            category=self.category,
            cost_price=Decimal('10.00'),
            selling_price=Decimal('15.00'),
            stock_quantity=5,
            low_stock_threshold=10
        )
        self.assertTrue(product.is_low_stock)


class BatchModelTest(TestCase):
    def setUp(self):
        self.category = Category.objects.create(name="Electronics")
        self.product = Product.objects.create(
            sku="PROD001",
            name="Laptop",
            category=self.category,
            cost_price=Decimal('500.00'),
            selling_price=Decimal('600.00')
        )
        self.supplier = SupplierModel.objects.create(
            name="Tech Supplier",
            phone="1234567890"
        )

    def test_batch_creation(self):
        batch = Batch.objects.create(
            product=self.product,
            batch_number="BATCH001",
            quantity=100,
            purchase_date=date.today(),
            supplier=self.supplier
        )
        self.assertEqual(str(batch), "Laptop - Batch BATCH001")


class StockMovementModelTest(TestCase):
    def setUp(self):
        self.category = Category.objects.create(name="Electronics")
        self.product = Product.objects.create(
            sku="PROD001",
            name="Laptop",
            category=self.category,
            cost_price=Decimal('500.00'),
            selling_price=Decimal('600.00')
        )
        user = User.objects.create_user(
            username="testuser",
            email="test@example.com",
            password="password123"
        )
        self.user = UserProfile.objects.create(user=user, role='storekeeper')

    def test_stock_movement_creation(self):
        movement = StockMovement.objects.create(
            product=self.product,
            movement_type='in',
            quantity=50,
            reason="New stock",
            user=self.user
        )
        self.assertEqual(str(movement), "Laptop - in - 50")


class SupplierModelTest(TestCase):
    def test_supplier_creation(self):
        supplier = Supplier.objects.create(
            name="ABC Supplies",
            contact_person="John Doe",
            phone="1234567890",
            email="john@abc.com"
        )
        self.assertEqual(str(supplier), "ABC Supplies")


class PurchaseModelTest(TestCase):
    def setUp(self):
        self.category = Category.objects.create(name="Electronics")
        self.product = Product.objects.create(
            sku="PROD001",
            name="Laptop",
            category=self.category,
            cost_price=Decimal('500.00'),
            selling_price=Decimal('600.00')
        )
        self.supplier = Supplier.objects.create(
            name="Tech Supplier",
            phone="1234567890"
        )

    def test_purchase_creation(self):
        purchase = Purchase.objects.create(
            product=self.product,
            supplier=self.supplier,
            quantity=10,
            unit_price=Decimal('500.00'),
            total_price=Decimal('5000.00'),
            batch_number="BATCH001"
        )
        self.assertEqual(str(purchase), "Purchase Laptop - 10")


class PriceHistoryModelTest(TestCase):
    def setUp(self):
        self.category = Category.objects.create(name="Electronics")
        self.product = Product.objects.create(
            sku="PROD001",
            name="Laptop",
            category=self.category,
            cost_price=Decimal('500.00'),
            selling_price=Decimal('600.00')
        )
        self.supplier = Supplier.objects.create(
            name="Tech Supplier",
            phone="1234567890"
        )

    def test_price_history_creation(self):
        history = PriceHistory.objects.create(
            supplier=self.supplier,
            product=self.product,
            price=Decimal('500.00')
        )
        self.assertIn("Tech Supplier - Laptop - 500.00", str(history))


class SalesHistoryModelTest(TestCase):
    def setUp(self):
        self.category = Category.objects.create(name="Electronics")
        self.product = Product.objects.create(
            sku="PROD001",
            name="Laptop",
            category=self.category,
            cost_price=Decimal('500.00'),
            selling_price=Decimal('600.00')
        )
        self.customer = Customer.objects.create(
            name="John Doe",
            phone="1234567890"
        )

    def test_sales_history_creation(self):
        sale = SalesHistory.objects.create(
            product=self.product,
            customer=self.customer,
            quantity=1,
            unit_price=Decimal('600.00'),
            total_price=Decimal('600.00'),
            receipt_number="RCP001"
        )
        self.assertEqual(str(sale), "Sale RCP001 - Laptop")


class CategorySerializerTest(TestCase):
    def test_category_serialization(self):
        category = Category.objects.create(name="Electronics", description="Electronic items")
        serializer = CategorySerializer(category)
        data = serializer.data
        self.assertEqual(data['name'], "Electronics")
        self.assertEqual(data['description'], "Electronic items")


class ProductSerializerTest(TestCase):
    def setUp(self):
        self.category = Category.objects.create(name="Electronics")

    def test_product_serialization(self):
        product = Product.objects.create(
            sku="PROD001",
            name="Laptop",
            category=self.category,
            cost_price=Decimal('500.00'),
            selling_price=Decimal('600.00'),
            stock_quantity=50,
            low_stock_threshold=10
        )
        serializer = ProductSerializer(product)
        data = serializer.data
        self.assertEqual(data['sku'], "PROD001")
        self.assertEqual(data['category_name'], "Electronics")
        self.assertFalse(data['is_low_stock'])


class CategoryViewSetTest(APITestCase):
    def setUp(self):
        self.user = User.objects.create_user(username='testuser', password='testpass')
        self.client.force_authenticate(user=self.user)
        self.category = Category.objects.create(name="Electronics", description="Electronic items")

    def test_list_categories(self):
        url = '/api/inventory/categories/'
        response = self.client.get(url)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)

    def test_create_category(self):
        url = '/api/inventory/categories/'
        data = {'name': 'Books', 'description': 'Reading materials'}
        response = self.client.post(url, data, format='json')
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(Category.objects.count(), 2)

    def test_retrieve_category(self):
        url = f'/api/inventory/categories/{self.category.pk}/'
        response = self.client.get(url)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data['name'], 'Electronics')


class ProductViewSetTest(APITestCase):
    def setUp(self):
        self.user = User.objects.create_user(username='testuser', password='testpass')
        self.client.force_authenticate(user=self.user)
        self.category = Category.objects.create(name="Electronics")
        self.product = Product.objects.create(
            sku="PROD001",
            name="Laptop",
            category=self.category,
            cost_price=Decimal('500.00'),
            selling_price=Decimal('600.00'),
            stock_quantity=50
        )

    def test_list_products(self):
        url = '/api/inventory/products/'
        response = self.client.get(url)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)

    def test_create_product(self):
        url = '/api/inventory/products/'
        data = {
            'sku': 'PROD002',
            'name': 'Mouse',
            'category': self.category.pk,
            'cost_price': '10.00',
            'selling_price': '15.00',
            'stock_quantity': 100
        }
        response = self.client.post(url, data, format='json')
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(Product.objects.count(), 2)


class LowStockViewTest(APITestCase):
    def setUp(self):
        self.user = User.objects.create_user(username='testuser', password='testpass')
        self.client.force_authenticate(user=self.user)
        self.category = Category.objects.create(name="Electronics")
        Product.objects.create(
            sku="PROD001",
            name="Laptop",
            category=self.category,
            cost_price=Decimal('500.00'),
            selling_price=Decimal('600.00'),
            stock_quantity=50,
            low_stock_threshold=10
        )
        Product.objects.create(
            sku="PROD002",
            name="Mouse",
            category=self.category,
            cost_price=Decimal('10.00'),
            selling_price=Decimal('15.00'),
            stock_quantity=5,
            low_stock_threshold=10
        )

    def test_low_stock_list(self):
        url = '/api/products/low-stock/'
        response = self.client.get(url)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertGreater(len(response.data), 0)  # At least one low stock product


class SupplierViewSetTest(APITestCase):
    def setUp(self):
        self.user = User.objects.create_user(username='testuser', password='testpass')
        self.client.force_authenticate(user=self.user)
        self.supplier = Supplier.objects.create(
            name="Tech Supplier",
            phone="1234567890",
            email="contact@tech.com"
        )

    def test_list_suppliers(self):
        url = '/api/inventory/suppliers/'
        response = self.client.get(url)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)

    def test_supplier_products_action(self):
        # Create related data
        category = Category.objects.create(name="Electronics")
        product = Product.objects.create(
            sku="PROD001",
            name="Laptop",
            category=category,
            cost_price=Decimal('500.00'),
            selling_price=Decimal('600.00')
        )
        Purchase.objects.create(
            product=product,
            supplier=self.supplier,
            quantity=10,
            unit_price=Decimal('500.00'),
            total_price=Decimal('5000.00')
        )
        url = f'/api/inventory/suppliers/{self.supplier.pk}/products/'
        response = self.client.get(url)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)


class StockReportViewTest(APITestCase):
    def setUp(self):
        self.user = User.objects.create_user(username='testuser', password='testpass')
        self.client.force_authenticate(user=self.user)
        category = Category.objects.create(name="Electronics")
        Product.objects.create(
            sku="PROD001",
            name="Laptop",
            category=category,
            cost_price=Decimal('500.00'),
            selling_price=Decimal('600.00'),
            stock_quantity=50,
            low_stock_threshold=10
        )

    def test_stock_report(self):
        url = '/api/inventory/reports/stock/'
        response = self.client.get(url)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertIn('product', response.data[0])
        self.assertIn('stock_quantity', response.data[0])


class PurchaseReportViewTest(APITestCase):
    def setUp(self):
        self.user = User.objects.create_user(username='testuser', password='testpass')
        self.client.force_authenticate(user=self.user)
        category = Category.objects.create(name="Electronics")
        product = Product.objects.create(
            sku="PROD001",
            name="Laptop",
            category=category,
            cost_price=Decimal('500.00'),
            selling_price=Decimal('600.00')
        )
        supplier = Supplier.objects.create(name="Tech Supplier", phone="1234567890")
        Purchase.objects.create(
            product=product,
            supplier=supplier,
            quantity=10,
            unit_price=Decimal('500.00'),
            total_price=Decimal('5000.00')
        )

    def test_purchase_report(self):
        url = '/api/inventory/reports/purchases/'
        response = self.client.get(url)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertGreater(len(response.data), 0)


class InventoryValuationViewTest(APITestCase):
    def setUp(self):
        self.user = User.objects.create_user(username='testuser', password='testpass')
        self.client.force_authenticate(user=self.user)
        category = Category.objects.create(name="Electronics")
        Product.objects.create(
            sku="PROD001",
            name="Laptop",
            category=category,
            cost_price=Decimal('500.00'),
            selling_price=Decimal('600.00'),
            stock_quantity=10
        )

    def test_inventory_valuation(self):
        url = '/api/inventory/reports/valuation/'
        response = self.client.get(url)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertIn('total_inventory_valuation', response.data)
        self.assertEqual(response.data['total_inventory_valuation'], 5000.00)


class BatchViewSetTest(APITestCase):
    def setUp(self):
        self.user = User.objects.create_user(username='testuser', password='testpass')
        self.client.force_authenticate(user=self.user)
        self.category = Category.objects.create(name="Electronics")
        self.product = Product.objects.create(
            sku="PROD001",
            name="Laptop",
            category=self.category,
            cost_price=Decimal('500.00'),
            selling_price=Decimal('600.00')
        )
        self.supplier = SupplierModel.objects.create(
            name="Tech Supplier",
            phone="1234567890"
        )
        self.batch = Batch.objects.create(
            product=self.product,
            batch_number="BATCH001",
            quantity=100,
            purchase_date=date.today(),
            supplier=self.supplier
        )

    def test_list_batches(self):
        url = '/api/inventory/batches/'
        response = self.client.get(url)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)

    def test_create_batch(self):
        url = '/api/inventory/batches/'
        data = {
            'product': self.product.pk,
            'batch_number': 'BATCH002',
            'quantity': 50,
            'purchase_date': date.today().isoformat(),
            'supplier': self.supplier.pk
        }
        response = self.client.post(url, data, format='json')
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(Batch.objects.count(), 2)

    def test_retrieve_batch(self):
        url = f'/api/inventory/batches/{self.batch.pk}/'
        response = self.client.get(url)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data['batch_number'], 'BATCH001')


class StockMovementViewSetTest(APITestCase):
    def setUp(self):
        self.user = User.objects.create_user(username='testuser', password='testpass')
        self.client.force_authenticate(user=self.user)
        self.category = Category.objects.create(name="Electronics")
        self.product = Product.objects.create(
            sku="PROD001",
            name="Laptop",
            category=self.category,
            cost_price=Decimal('500.00'),
            selling_price=Decimal('600.00')
        )
        user_profile = UserProfile.objects.create(user=self.user, role='storekeeper')
        self.movement = StockMovement.objects.create(
            product=self.product,
            movement_type='in',
            quantity=50,
            reason="New stock",
            user=user_profile
        )

    def test_list_stock_movements(self):
        url = '/api/inventory/stock-movements/'
        response = self.client.get(url)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)

    def test_create_stock_movement(self):
        url = '/api/inventory/stock-movements/'
        data = {
            'product': self.product.pk,
            'movement_type': 'out',
            'quantity': 10,
            'reason': 'Sale'
        }
        response = self.client.post(url, data, format='json')
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(StockMovement.objects.count(), 2)


class PurchaseViewSetTest(APITestCase):
    def setUp(self):
        self.user = User.objects.create_user(username='testuser', password='testpass')
        self.client.force_authenticate(user=self.user)
        self.category = Category.objects.create(name="Electronics")
        self.product = Product.objects.create(
            sku="PROD001",
            name="Laptop",
            category=self.category,
            cost_price=Decimal('500.00'),
            selling_price=Decimal('600.00')
        )
        self.supplier = Supplier.objects.create(
            name="Tech Supplier",
            phone="1234567890"
        )
        self.purchase = Purchase.objects.create(
            product=self.product,
            supplier=self.supplier,
            quantity=10,
            unit_price=Decimal('500.00'),
            total_price=Decimal('5000.00'),
            batch_number="BATCH001"
        )

    def test_list_purchases(self):
        url = '/api/inventory/purchases/'
        response = self.client.get(url)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)

    def test_create_purchase(self):
        url = '/api/inventory/purchases/'
        data = {
            'product': self.product.pk,
            'supplier': self.supplier.pk,
            'quantity': 5,
            'unit_price': '600.00',
            'total_price': '3000.00',
            'batch_number': 'BATCH002'
        }
        response = self.client.post(url, data, format='json')
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(Purchase.objects.count(), 2)


class PriceHistoryViewSetTest(APITestCase):
    def setUp(self):
        self.user = User.objects.create_user(username='testuser', password='testpass')
        self.client.force_authenticate(user=self.user)
        self.category = Category.objects.create(name="Electronics")
        self.product = Product.objects.create(
            sku="PROD001",
            name="Laptop",
            category=self.category,
            cost_price=Decimal('500.00'),
            selling_price=Decimal('600.00')
        )
        self.supplier = Supplier.objects.create(
            name="Tech Supplier",
            phone="1234567890"
        )
        self.price_history = PriceHistory.objects.create(
            supplier=self.supplier,
            product=self.product,
            price=Decimal('500.00')
        )

    def test_list_price_histories(self):
        url = '/api/inventory/price-history/'
        response = self.client.get(url)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)

    def test_by_product_action(self):
        url = f'/api/inventory/price-history/product/{self.product.pk}/'
        response = self.client.get(url)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)

    def test_by_supplier_action(self):
        url = f'/api/inventory/price-history/supplier/{self.supplier.pk}/'
        response = self.client.get(url)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)


class SalesHistoryViewSetTest(APITestCase):
    def setUp(self):
        self.user = User.objects.create_user(username='testuser', password='testpass')
        self.client.force_authenticate(user=self.user)
        self.category = Category.objects.create(name="Electronics")
        self.product = Product.objects.create(
            sku="PROD001",
            name="Laptop",
            category=self.category,
            cost_price=Decimal('500.00'),
            selling_price=Decimal('600.00')
        )
        self.customer = Customer.objects.create(
            name="John Doe",
            phone="1234567890"
        )
        self.sale = SalesHistory.objects.create(
            product=self.product,
            customer=self.customer,
            quantity=1,
            unit_price=Decimal('600.00'),
            total_price=Decimal('600.00'),
            receipt_number="RCP001"
        )

    def test_list_sales_histories(self):
        url = '/api/inventory/sales-history/'
        response = self.client.get(url)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)

    def test_by_product_action(self):
        url = f'/api/inventory/sales-history/product/{self.product.pk}/'
        response = self.client.get(url)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)

    def test_by_customer_action(self):
        url = f'/api/inventory/sales-history/customer/{self.customer.pk}/'
        response = self.client.get(url)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)

    def test_by_date_range_action(self):
        url = '/api/inventory/sales-history/date/?from=2023-01-01&to=2023-12-31'
        response = self.client.get(url)
        self.assertEqual(response.status_code, status.HTTP_200_OK)


class SupplierPerformanceViewTest(APITestCase):
    def setUp(self):
        self.user = User.objects.create_user(username='testuser', password='testpass')
        self.client.force_authenticate(user=self.user)
        category = Category.objects.create(name="Electronics")
        product = Product.objects.create(
            sku="PROD001",
            name="Laptop",
            category=category,
            cost_price=Decimal('500.00'),
            selling_price=Decimal('600.00')
        )
        supplier = Supplier.objects.create(name="Tech Supplier", phone="1234567890")
        Purchase.objects.create(
            product=product,
            supplier=supplier,
            quantity=10,
            unit_price=Decimal('500.00'),
            total_price=Decimal('5000.00')
        )

    def test_supplier_performance(self):
        url = '/api/inventory/reports/supplier/'
        response = self.client.get(url)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertGreater(len(response.data), 0)
