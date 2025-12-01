#!/usr/bin/env python
"""
Test script to check if sale serialization works properly after sale creation.
"""
import os
import sys
import django

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'myshop.settings')
django.setup()

from django.contrib.auth.models import User
from sales.models import Sale, SaleItem, Cart, CartItem
from sales.serializers import SaleSerializer
from inventory.models import Product, Category
from payments.models import Payment
from shifts.models import Shift
from users.models import UserProfile
from decimal import Decimal
import time

def test_sale_serialization():
    """Test that sale serialization works properly"""
    print("Testing sale serialization...")

    # Create test data
    import time
    username = f'test_user_{int(time.time())}'
    user = User.objects.create_user(username=username, password='test_pass')
    profile = UserProfile.objects.create(user=user, role='cashier')

    shift = Shift.objects.create(
        cashier=profile,
        status='open',
        opening_balance=Decimal('0.00')
    )

    category_name = f'Test Category {int(time.time())}'
    category = Category.objects.create(name=category_name)

    sku = f'TEST{int(time.time())}'
    product = Product.objects.create(
        sku=sku,
        name='Test Product',
        category=category,
        cost_price=Decimal('50.00'),
        selling_price=Decimal('100.00'),
        stock_quantity=10
    )

    # Create cart and cart item
    cart = Cart.objects.create(cashier=profile, status='closed')
    cart_item = CartItem.objects.create(
        cart=cart,
        product=product,
        quantity=2,
        unit_price=Decimal('100.00'),
        discount=Decimal('0.00')
    )

    # Create sale
    sale = Sale.objects.create(
        cart=cart,
        shift=shift,
        sale_type='retail',
        total_amount=Decimal('200.00'),
        tax_amount=Decimal('0.00'),
        discount_amount=Decimal('0.00'),
        final_amount=Decimal('200.00'),
        receipt_number=f'TEST-SERIALIZE-{int(time.time())}'
    )

    # Create sale item
    SaleItem.objects.create(
        sale=sale,
        product=product,
        quantity=2,
        unit_price=Decimal('100.00'),
        discount=Decimal('0.00')
    )

    # Create payment
    Payment.objects.create(
        sale=sale,
        payment_type='cash',
        amount=Decimal('200.00'),
        status='completed'
    )

    # Test serialization
    try:
        serializer = SaleSerializer(sale)
        data = serializer.data
        print("Sale serialization successful!")
        print(f"Payment method: {data.get('payment_method')}")
        print(f"Split data: {data.get('split_data')}")
        print(f"Customer name: {data.get('customer_name')}")
        print(f"Voided by name: {data.get('voided_by_name')}")

        # Test with split payment
        print("\nTesting split payment serialization...")

        # Create another cart for the split payment sale
        cart2 = Cart.objects.create(cashier=profile, status='closed')
        cart_item2 = CartItem.objects.create(
            cart=cart2,
            product=product,
            quantity=3,
            unit_price=Decimal('100.00'),
            discount=Decimal('0.00')
        )

        # Create another sale with split payment
        sale2 = Sale.objects.create(
            cart=cart2,
            shift=shift,
            sale_type='retail',
            total_amount=Decimal('300.00'),
            tax_amount=Decimal('0.00'),
            discount_amount=Decimal('0.00'),
            final_amount=Decimal('300.00'),
            receipt_number=f'TEST-SPLIT-{int(time.time())}'
        )

        SaleItem.objects.create(
            sale=sale2,
            product=product,
            quantity=3,
            unit_price=Decimal('100.00'),
            discount=Decimal('0.00')
        )

        # Create split payments
        Payment.objects.create(
            sale=sale2,
            payment_type='cash',
            amount=Decimal('150.00'),
            status='completed'
        )
        Payment.objects.create(
            sale=sale2,
            payment_type='mpesa',
            amount=Decimal('150.00'),
            status='completed'
        )

        serializer2 = SaleSerializer(sale2)
        data2 = serializer2.data
        print("Split payment sale serialization successful!")
        print(f"Payment method: {data2.get('payment_method')}")
        print(f"Split data: {data2.get('split_data')}")

    except Exception as e:
        print(f"Serialization failed: {e}")
        import traceback
        traceback.print_exc()
        return False

    # Cleanup
    sale.delete()
    sale2.delete()
    shift.delete()
    product.delete()
    category.delete()
    profile.delete()
    user.delete()

    print("Test completed successfully!")
    return True

if __name__ == '__main__':
    test_sale_serialization()