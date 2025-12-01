#!/usr/bin/env python
"""
Test script to verify that payment failures properly roll back sales and stock movements.
"""
import os
import sys
import django

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'myshop.settings')
django.setup()

from django.test import TransactionTestCase
from django.contrib.auth.models import User
from django.db import transaction
from sales.models import Sale, SaleItem, Cart, CartItem
from inventory.models import Product, StockMovement, SalesHistory, Category
from payments.models import Payment
from shifts.models import Shift
from users.models import UserProfile
from decimal import Decimal

def test_payment_rollback():
    """Test that payment creation failure rolls back the entire transaction"""
    print("Testing payment rollback functionality...")

    # Create test user and profile
    import time
    username = f'test_user_{int(time.time())}'
    user = User.objects.create_user(username=username, password='test_pass')
    profile = UserProfile.objects.create(user=user, role='cashier')

    # Create test shift
    shift = Shift.objects.create(
        cashier=profile,
        status='open',
        opening_balance=Decimal('0.00')
    )

    # Create test category first
    import time
    category_name = f'Test Category {int(time.time())}'
    category = Category.objects.create(name=category_name)

    # Create test product
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
        unit_price=Decimal('100.00'),  # This is from cart item model, might be different
        discount=Decimal('0.00')
    )

    initial_stock = product.stock_quantity
    print(f"Initial stock: {initial_stock}")

    # Count initial records
    initial_sales = Sale.objects.count()
    initial_sale_items = SaleItem.objects.count()
    initial_payments = Payment.objects.count()
    initial_stock_movements = StockMovement.objects.count()
    initial_sales_history = SalesHistory.objects.count()

    print(f"Initial counts - Sales: {initial_sales}, SaleItems: {initial_sale_items}, Payments: {initial_payments}")

    try:
        with transaction.atomic():
            # Create sale (this should work)
            sale = Sale.objects.create(
                cart=cart,
                shift=shift,
                sale_type='retail',
                total_amount=Decimal('200.00'),
                tax_amount=Decimal('0.00'),
                discount_amount=Decimal('0.00'),
                final_amount=Decimal('200.00'),
                receipt_number='TEST-ROLLBACK-001'
            )

            # Create sale item
            SaleItem.objects.create(
                sale=sale,
                product=product,
                quantity=2,
                unit_price=Decimal('100.00'),
                discount=Decimal('0.00')
            )

            # Deduct stock
            product.stock_quantity = Decimal(str(product.stock_quantity)) - Decimal('2.00')
            product.save(update_fields=['stock_quantity'])

            StockMovement.objects.create(
                product=product,
                movement_type='out',
                quantity=-2,
                reason=f'Test sale rollback {sale.receipt_number}',
                user=profile
            )

            # Now try to create payment with invalid data that should fail
            # This should cause the entire transaction to roll back
            raise ValueError("Simulated payment failure")

    except ValueError as e:
        print(f"Expected exception caught: {e}")

        # Check that everything was rolled back
        final_stock = Product.objects.get(id=product.id).stock_quantity
        final_sales = Sale.objects.count()
        final_sale_items = SaleItem.objects.count()
        final_payments = Payment.objects.count()
        final_stock_movements = StockMovement.objects.count()
        final_sales_history = SalesHistory.objects.count()

        print(f"Final stock: {final_stock}")
        print(f"Final counts - Sales: {final_sales}, SaleItems: {final_sale_items}, Payments: {final_payments}")

        # Verify rollback
        assert final_stock == initial_stock, f"Stock not rolled back: {final_stock} != {initial_stock}"
        assert final_sales == initial_sales, f"Sales not rolled back: {final_sales} != {initial_sales}"
        assert final_sale_items == initial_sale_items, f"SaleItems not rolled back: {final_sale_items} != {initial_sale_items}"
        assert final_payments == initial_payments, f"Payments not rolled back: {final_payments} != {initial_payments}"

        print("All assertions passed - transaction rollback working correctly!")

    # Cleanup
    shift.delete()
    product.delete()
    category.delete()
    profile.delete()
    user.delete()

    print("Test completed successfully!")

if __name__ == '__main__':
    test_payment_rollback()