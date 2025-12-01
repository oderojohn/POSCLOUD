from rest_framework import viewsets, status, serializers
from rest_framework.response import Response
from rest_framework.decorators import action
from django_filters.rest_framework import DjangoFilterBackend
from rest_framework.filters import SearchFilter, OrderingFilter
from django.utils import timezone
from django.db import transaction
from django.conf import settings
from .models import Cart, CartItem, Sale, SaleItem, Return, Invoice, InvoiceItem
from .serializers import CartSerializer, CartItemSerializer, SaleSerializer, SaleItemSerializer, ReturnSerializer, InvoiceSerializer, InvoiceItemSerializer
from inventory.models import Product, StockMovement, SalesHistory
from shifts.models import Shift
from payments.models import Payment

class CartViewSet(viewsets.ModelViewSet):
    queryset = Cart.objects.all()
    serializer_class = CartSerializer

class CartItemViewSet(viewsets.ModelViewSet):
    queryset = CartItem.objects.all()
    serializer_class = CartItemSerializer

class SaleViewSet(viewsets.ModelViewSet):
    queryset = Sale.objects.all()
    serializer_class = SaleSerializer

    @action(detail=False, methods=['get'])
    def held_orders(self, request):
        """Get all held orders for the current cashier's shift"""
        cashier = None
        if hasattr(request.user, 'userprofile'):
            cashier = request.user.userprofile

        if not cashier:
            return Response(
                {'error': 'User profile not found'},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            current_shift = Shift.objects.get(cashier=cashier, status='open')
        except Shift.DoesNotExist:
            return Response(
                {'error': 'No active shift found'},
                status=status.HTTP_400_BAD_REQUEST
            )

        held_carts = Cart.objects.filter(
            cashier=cashier,
            status='held'
        ).prefetch_related('cartitem_set__product').order_by('-created_at')

        serializer = CartSerializer(held_carts, many=True)
        return Response(serializer.data)

    @action(detail=True, methods=['post'])
    def complete_held_order(self, request, pk=None):
        """Complete a held order by creating the sale and processing payment"""
        try:
            cart = Cart.objects.get(id=pk, status='held')
        except Cart.DoesNotExist:
            return Response(
                {'error': 'Held order not found'},
                status=status.HTTP_404_NOT_FOUND
            )

        # Check if cashier has active shift
        cashier = None
        if hasattr(request.user, 'userprofile'):
            cashier = request.user.userprofile

        if not cashier or cart.cashier != cashier:
            return Response(
                {'error': 'Unauthorized to complete this order'},
                status=status.HTTP_403_FORBIDDEN
            )

        try:
            current_shift = Shift.objects.get(cashier=cashier, status='open')
        except Shift.DoesNotExist:
            return Response(
                {'error': 'No active shift found'},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            with transaction.atomic():
                # Calculate totals from cart items
                cart_items = cart.cartitem_set.all()
                subtotal = sum(float(item.unit_price) * int(item.quantity) for item in cart_items)
                tax_amount = float(request.data.get('tax_amount', 0))
                discount_amount = float(request.data.get('discount_amount', 0))
                total_amount = float(request.data.get('total_amount', subtotal + tax_amount - discount_amount))
                receipt_number = request.data.get('receipt_number', f'POS-{timezone.now().strftime("%Y%m%d%H%M%S")}')

                # Create sale
                from django.db.models.signals import post_save
                from .signals import create_default_payment

                # Temporarily disconnect the signal to prevent duplicate payments
                post_save.disconnect(create_default_payment, sender=Sale)

                sale = Sale.objects.create(
                    cart=cart,
                    customer=cart.customer,
                    shift=current_shift,
                    sale_type=request.data.get('sale_type', 'retail'),
                    total_amount=float(subtotal),
                    tax_amount=float(tax_amount),
                    discount_amount=float(discount_amount),
                    final_amount=float(total_amount),
                    receipt_number=receipt_number
                )

                # Reconnect the signal
                post_save.connect(create_default_payment, sender=Sale)

                # Update shift totals
                payment_method = request.data.get('payment_method', 'cash').lower()
                from decimal import Decimal
                sale_amount = Decimal(str(total_amount))

                from django.db.models import F
                update_fields = ['total_sales']
                if payment_method == 'split':
                    # For split payments, use the split data
                    split_data = request.data.get('split_data', {})
                    cash_amount = Decimal(str(split_data.get('cash', 0)))
                    mpesa_amount = Decimal(str(split_data.get('mpesa', 0)))
                    current_shift.cash_sales = F('cash_sales') + cash_amount
                    current_shift.mobile_sales = F('mobile_sales') + mpesa_amount
                    update_fields.extend(['cash_sales', 'mobile_sales'])
                elif payment_method == 'cash':
                    current_shift.cash_sales = F('cash_sales') + sale_amount
                    update_fields.append('cash_sales')
                elif payment_method in ['mpesa', 'mobile']:
                    current_shift.mobile_sales = F('mobile_sales') + sale_amount
                    update_fields.append('mobile_sales')

                current_shift.total_sales = F('total_sales') + sale_amount
                current_shift.save(update_fields=update_fields)

                # Create sale items
                for cart_item in cart_items:
                    SaleItem.objects.create(
                        sale=sale,
                        product=cart_item.product,
                        quantity=int(cart_item.quantity),
                        unit_price=float(cart_item.unit_price),
                        discount=float(cart_item.discount)
                    )

                # Deduct stock (same logic as in create method)
                stock_deductions = []
                for cart_item in cart_items:
                    product = cart_item.product
                    requested_quantity = int(cart_item.quantity)

                    # Validate quantity is positive
                    if requested_quantity <= 0:
                        return Response(
                            {'error': f'Invalid quantity for product "{product.name}". Quantity must be positive.'},
                            status=status.HTTP_400_BAD_REQUEST
                        )

                    if float(product.stock_quantity) < requested_quantity:
                        return Response(
                            {'error': f'Insufficient stock for product "{product.name}". Available: {product.stock_quantity}, Requested: {requested_quantity}'},
                            status=status.HTTP_400_BAD_REQUEST
                        )

                    stock_deductions.append({
                        'product': product,
                        'quantity': requested_quantity,
                        'cart_item': cart_item
                    })

                # Apply stock deductions
                for deduction in stock_deductions:
                    product = deduction['product']
                    remaining_quantity = deduction['quantity']

                    from inventory.models import Batch
                    available_batches = Batch.objects.filter(
                        product=product,
                        quantity__gt=0
                    ).exclude(
                        status__in=['damaged', 'expired'],
                        expiry_date__lt=timezone.now().date()
                    ).order_by('expiry_date', 'purchase_date')

                    batch_deductions = []
                    for batch in available_batches:
                        if remaining_quantity <= 0:
                            break

                        take_quantity = min(remaining_quantity, batch.quantity)
                        batch_deductions.append({
                            'batch': batch,
                            'quantity': take_quantity
                        })
                        remaining_quantity -= take_quantity

                    total_available = sum(d['quantity'] for d in batch_deductions)
                    if total_available < deduction['quantity']:
                        if not available_batches.exists() and float(product.stock_quantity) >= deduction['quantity']:
                            from decimal import Decimal
                            product.stock_quantity = Decimal(str(product.stock_quantity)) - Decimal(str(deduction['quantity']))
                            product.save(update_fields=['stock_quantity'])

                            StockMovement.objects.create(
                                product=product,
                                movement_type='out',
                                quantity=-deduction['quantity'],
                                reason=f'Sale {sale.receipt_number} - No batch tracking',
                                user=cashier
                            )

                            SalesHistory.objects.create(
                                product=product,
                                batch=None,
                                quantity=deduction['quantity'],
                                unit_price=deduction['cart_item'].unit_price,
                                cost_price=0,
                                total_price=deduction['cart_item'].unit_price * deduction['quantity'],
                                receipt_number=sale.receipt_number,
                                sale_date=sale.sale_date
                            )
                            continue
                        else:
                            return Response(
                                {'error': f'Insufficient stock for product "{product.name}". Available: {total_available}, Required: {deduction["quantity"]}'},
                                status=status.HTTP_400_BAD_REQUEST
                            )

                    # Apply batch deductions
                    for batch_deduction in batch_deductions:
                        batch = batch_deduction['batch']
                        quantity = batch_deduction['quantity']

                        from decimal import Decimal
                        batch.quantity = Decimal(str(batch.quantity)) - Decimal(str(quantity))
                        batch.save(update_fields=['quantity'])

                        product.stock_quantity = Decimal(str(product.stock_quantity)) - Decimal(str(quantity))
                        product.save(update_fields=['stock_quantity'])

                        StockMovement.objects.create(
                            product=product,
                            movement_type='out',
                            quantity=-quantity,
                            reason=f'Sale {sale.receipt_number} - Batch {batch.batch_number}',
                            user=cashier
                        )

                        SalesHistory.objects.create(
                            product=product,
                            batch=batch,
                            quantity=quantity,
                            unit_price=deduction['cart_item'].unit_price,
                            cost_price=batch.cost_price,
                            total_price=deduction['cart_item'].unit_price * quantity,
                            receipt_number=sale.receipt_number,
                            sale_date=sale.sale_date
                        )

                # Check if payments already exist for this sale (prevent duplicates)
                existing_payments = sale.payment_set.filter(status='completed')
                if existing_payments.exists():
                    created_payments = list(existing_payments)
                else:
                    # Check if payments already exist for this sale (prevent duplicates)
                    existing_payments = sale.payment_set.filter(status='completed')
                    if existing_payments.exists():
                        created_payments = list(existing_payments)
                    else:
                        # STRICT VALIDATION: Require payment method and ensure payment creation succeeds
                        payment_method = request.data.get('payment_method', '').strip().lower()
                        if not payment_method:
                            return Response(
                                {'error': 'Payment method is required for all transactions'},
                                status=status.HTTP_400_BAD_REQUEST
                            )
    
                        # Validate payment method
                        valid_payment_methods = ['cash', 'mpesa', 'mobile', 'split']
                        if payment_method not in valid_payment_methods:
                            return Response(
                                {'error': f'Invalid payment method. Must be one of: {", ".join(valid_payment_methods)}'},
                                status=status.HTTP_400_BAD_REQUEST
                            )
    
                        # Validate split payment data if split method selected
                        if payment_method == 'split':
                            split_data = request.data.get('split_data', {})
                            if not split_data or (split_data.get('cash', 0) == 0 and split_data.get('mpesa', 0) == 0):
                                raise ValueError('Split payment requires cash and/or mpesa amounts in split_data')
    
                        # Create payment record for the sale
                        created_payments = []
                        if payment_method == 'split':
                            # For split payments, create appropriate payment records
                            split_data = request.data.get('split_data', {})
                            cash_amount = split_data.get('cash', 0)
                            mpesa_amount = split_data.get('mpesa', 0)
    
                            # Check if it's truly split (both amounts > 0) or pure payment
                            if cash_amount > 0 and mpesa_amount > 0:
                                # True split payment - create two records
                                payment = Payment.objects.create(
                                    sale=sale,
                                    payment_type='cash',
                                    amount=cash_amount,
                                    status='completed'
                                )
                                created_payments.append(payment)
    
                                payment = Payment.objects.create(
                                    sale=sale,
                                    payment_type='mpesa',
                                    amount=mpesa_amount,
                                    mpesa_number=request.data.get('mpesa_number', ''),
                                    status='completed'
                                )
                                created_payments.append(payment)
                            elif mpesa_amount > 0:
                                # Pure M-Pesa payment
                                payment = Payment.objects.create(
                                    sale=sale,
                                    payment_type='mpesa',
                                    amount=mpesa_amount,
                                    mpesa_number=request.data.get('mpesa_number', ''),
                                    status='completed'
                                )
                                created_payments.append(payment)
                            elif cash_amount > 0:
                                # Pure cash payment
                                payment = Payment.objects.create(
                                    sale=sale,
                                    payment_type='cash',
                                    amount=cash_amount,
                                    status='completed'
                                )
                                created_payments.append(payment)
                        else:
                            # Single payment method
                            payment_type = 'cash'
                            if payment_method in ['mpesa', 'mobile']:
                                payment_type = 'mpesa'
    
                            payment = Payment.objects.create(
                                sale=sale,
                                payment_type=payment_type,
                                amount=total_amount,
                                mpesa_number=request.data.get('mpesa_number', '') if payment_type == 'mpesa' else '',
                                status='completed'
                            )
                            created_payments.append(payment)
    
                        # CRITICAL VALIDATION: Ensure at least one payment was created
                        if not created_payments:
                            raise ValueError("No payment records were created for this transaction")
    
                        # Verify payment amounts total matches sale amount
                        total_payment_amount = sum(float(p.amount) for p in created_payments)
                        if abs(total_payment_amount - float(total_amount)) > 0.01:  # Allow small floating point differences
                            raise ValueError(f"Payment amount mismatch: payments total {total_payment_amount}, sale total {total_amount}")

                # Update cart status to closed
                cart.status = 'closed'
                cart.save()

                # Serialize and return the sale
                serializer = self.get_serializer(sale)
                return Response(serializer.data, status=status.HTTP_201_CREATED)

        except Exception as e:
            print(f"Error completing held order: {str(e)}")
            import traceback
            traceback.print_exc()

            # Provide user-friendly error messages
            error_message = str(e)
            if 'stock' in error_message.lower():
                user_error = '❌ Insufficient Stock'
                user_message = 'One or more items in this order are out of stock.'
                user_details = 'Please check inventory levels and restock if necessary.'
            elif 'payment' in error_message.lower():
                user_error = '❌ Payment Error'
                user_message = 'There was an issue processing the payment.'
                user_details = 'Please check your payment details and try again.'
            elif 'split payment requires' in error_message.lower():
                user_error = '❌ Payment Error'
                user_message = 'Split payment requires cash and/or M-Pesa amounts.'
                user_details = 'Please enter valid amounts for both payment methods or choose a single payment method.'
            else:
                user_error = '❌ Order Completion Error'
                user_message = 'An unexpected error occurred while completing the order.'
                user_details = 'Please contact your administrator if this problem persists.'

            return Response({
                'error': user_error,
                'message': user_message,
                'details': user_details,
                'technical_error': error_message if settings.DEBUG else None
            }, status=status.HTTP_400_BAD_REQUEST)

    @action(detail=True, methods=['post'])
    def void_held_order(self, request, pk=None):
        """Void/cancel a held order with a reason"""
        try:
            cart = Cart.objects.get(id=pk, status='held')
        except Cart.DoesNotExist:
            return Response(
                {'error': 'Held order not found'},
                status=status.HTTP_404_NOT_FOUND
            )

        # Check if cashier has permission to void this order
        cashier = None
        if hasattr(request.user, 'userprofile'):
            cashier = request.user.userprofile

        if not cashier or cart.cashier != cashier:
            return Response(
                {'error': 'Unauthorized to void this order'},
                status=status.HTTP_403_FORBIDDEN
            )

        void_reason = request.data.get('void_reason', '').strip()
        if not void_reason:
            return Response(
                {'error': 'Void reason is required'},
                status=status.HTTP_400_BAD_REQUEST
            )

        # Void the order
        cart.status = 'voided'
        cart.void_reason = void_reason
        cart.save()

        return Response({
            'message': 'Held order voided successfully',
            'void_reason': void_reason
        }, status=status.HTTP_200_OK)

    @action(detail=True, methods=['post'])
    def void_sale(self, request, pk=None):
        """Void a completed sale with a reason"""
        try:
            sale = Sale.objects.get(id=pk)
        except Sale.DoesNotExist:
            return Response(
                {'error': 'Sale not found'},
                status=status.HTTP_404_NOT_FOUND
            )

        # Check if sale is already voided
        if sale.voided:
            return Response(
                {'error': 'Sale is already voided'},
                status=status.HTTP_400_BAD_REQUEST
            )

        void_reason = request.data.get('reason', '').strip()
        if not void_reason:
            return Response(
                {'error': 'Void reason is required'},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            with transaction.atomic():
                # Mark sale as voided
                sale.voided = True
                sale.void_reason = void_reason
                sale.voided_at = timezone.now()
                sale.voided_by = request.user.userprofile if hasattr(request.user, 'userprofile') else None
                sale.save()

                # Restore stock quantities
                for sale_item in sale.saleitem_set.all():
                    product = sale_item.product
                    quantity_to_restore = sale_item.quantity

                    # Validate quantity is positive
                    if quantity_to_restore <= 0:
                        return Response(
                            {'error': f'Invalid quantity for product "{product.name}" in voided sale. Quantity must be positive.'},
                            status=status.HTTP_400_BAD_REQUEST
                        )

                    # Restore product stock
                    from decimal import Decimal
                    product.stock_quantity = Decimal(str(product.stock_quantity)) + Decimal(str(quantity_to_restore))
                    product.save(update_fields=['stock_quantity'])

                    # Create stock movement record
                    from inventory.models import StockMovement
                    StockMovement.objects.create(
                        product=product,
                        movement_type='in',
                        quantity=quantity_to_restore,
                        reason=f'Sale void {sale.receipt_number} - {void_reason}',
                        user=request.user.userprofile if hasattr(request.user, 'userprofile') else None
                    )

                    # Update batch quantities if applicable
                    from inventory.models import Batch, SalesHistory
                    sales_history_records = SalesHistory.objects.filter(
                        product=product,
                        receipt_number=sale.receipt_number
                    )

                    for history_record in sales_history_records:
                        if history_record.batch:
                            batch = history_record.batch
                            batch.quantity = Decimal(str(batch.quantity)) + Decimal(str(history_record.quantity))
                            batch.save(update_fields=['quantity'])

                # Update shift totals (subtract the voided sale)
                if sale.shift:
                    from decimal import Decimal
                    void_amount = Decimal(str(sale.final_amount))

                    # Get payment method from payment record
                    payment = sale.payment_set.first()
                    payment_method = payment.payment_type if payment else 'cash'

                    # Subtract from shift totals based on payment method
                    if payment_method == 'cash':
                        sale.shift.cash_sales = F('cash_sales') - void_amount
                        sale.shift.save(update_fields=['cash_sales'])
                    elif payment_method == 'card':
                        sale.shift.card_sales = F('card_sales') - void_amount
                        sale.shift.save(update_fields=['card_sales'])
                    elif payment_method in ['mpesa', 'mobile']:
                        sale.shift.mobile_sales = F('mobile_sales') - void_amount
                        sale.shift.save(update_fields=['mobile_sales'])

                    # Subtract from total sales
                    sale.shift.total_sales = F('total_sales') - void_amount
                    sale.shift.save(update_fields=['total_sales'])

                return Response({
                    'message': 'Sale voided successfully',
                    'void_reason': void_reason,
                    'sale_id': sale.id
                }, status=status.HTTP_200_OK)

        except Exception as e:
            print(f"Error voiding sale: {str(e)}")
            import traceback
            traceback.print_exc()
            return Response({
                'error': 'Failed to void sale',
                'details': str(e)
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    def create(self, request, *args, **kwargs):
        """
        Custom create method to handle sale creation from frontend cart data
        """
        try:
            with transaction.atomic():
                # Check if this is a hold order request
                is_hold_order = request.data.get('hold_order', False)

                # Get items from request (frontend cart data)
                items_data = request.data.get('items', [])
                print(f"Received items data: {items_data}")
                print(f"Request data types: {[type(item.get('unit_price')) for item in items_data]}")
                if not items_data:
                    return Response(
                        {'error': 'No items provided'},
                        status=status.HTTP_400_BAD_REQUEST
                    )

                # Get cashier from authenticated user (required for shift validation)
                cashier = None
                if hasattr(request.user, 'userprofile'):
                    cashier = request.user.userprofile

                # Require active shift for all sales
                if not cashier:
                    return Response(
                        {'error': 'User profile not found. Please contact administrator.'},
                        status=status.HTTP_400_BAD_REQUEST
                    )

                try:
                    current_shift = Shift.objects.get(cashier=cashier, status='open')
                except Shift.DoesNotExist:
                    return Response(
                        {'error': 'No active shift found. Please start a shift before processing sales.'},
                        status=status.HTTP_400_BAD_REQUEST
                    )

                # Create cart first
                cart_status = 'held' if is_hold_order else 'closed'
                cart = Cart.objects.create(
                    cashier=cashier,
                    status=cart_status
                )

                # Create cart items from frontend data
                cart_items = []
                for item_data in items_data:
                    try:
                        print(f"Processing item: {item_data}")
                        print(f"Item types - product: {type(item_data.get('product'))}, quantity: {type(item_data.get('quantity'))}, unit_price: {type(item_data.get('unit_price'))}")

                        cart_item = CartItem.objects.create(
                            cart=cart,
                            product_id=item_data.get('product'),
                            quantity=int(item_data.get('quantity', 1)),
                            unit_price=float(item_data.get('unit_price', 0)),
                            discount=float(item_data.get('discount', 0))
                        )
                        cart_items.append(cart_item)
                    except Exception as item_error:
                        print(f"Error creating cart item: {str(item_error)}")
                        print(f"Item data: {item_data}")
                        return Response(
                            {'error': f'Error creating cart item for product {item_data.get("product")}: {str(item_error)}'},
                            status=status.HTTP_400_BAD_REQUEST
                        )

                # Get sale type and apply wholesale business rules
                sale_type = request.data.get('sale_type', 'retail')

                # Get customer if provided (needed for wholesale validation)
                customer_id = request.data.get('customer')

                # Validate stock availability and prepare stock deduction
                stock_deductions = []
                total_quantity = 0
                for cart_item in cart_items:
                    product = cart_item.product
                    requested_quantity = int(cart_item.quantity)

                    # Validate quantity is positive
                    if requested_quantity <= 0:
                        return Response(
                            {'error': f'Invalid quantity for product "{product.name}". Quantity must be positive.'},
                            status=status.HTTP_400_BAD_REQUEST
                        )

                    total_quantity += requested_quantity

                    if float(product.stock_quantity) < requested_quantity:
                        return Response(
                            {'error': f'Insufficient stock for product "{product.name}". Available: {product.stock_quantity}, Requested: {requested_quantity}'},
                            status=status.HTTP_400_BAD_REQUEST
                        )

                    # Wholesale validation: Check minimum quantity if wholesale price is used
                    if sale_type == 'wholesale' and hasattr(product, 'wholesale_price') and product.wholesale_price and product.wholesale_price > 0:
                        # Minimum quantity is always 1 for all sales (no wholesale minimum check)
                        min_quantity = 1
                        if requested_quantity < min_quantity:
                            return Response(
                                {'error': f'Product "{product.name}" requires minimum {min_quantity} units. Requested: {requested_quantity}'},
                                status=status.HTTP_400_BAD_REQUEST
                            )

                    stock_deductions.append({
                        'product': product,
                        'quantity': requested_quantity,
                        'cart_item': cart_item
                    })

                # Wholesale validation: Minimum total order quantity
                # Minimum quantity is always 1 for all sales (no wholesale minimum check)
                min_total_quantity = 1
                if sale_type == 'wholesale' and total_quantity < min_total_quantity:
                    return Response(
                        {'error': f'Orders require minimum {min_total_quantity} items total. Current total: {total_quantity}'},
                        status=status.HTTP_400_BAD_REQUEST
                    )

                # Calculate totals (ensure numeric types)
                print(f"Cart items for calculation: {[(item.unit_price, type(item.unit_price), item.quantity, type(item.quantity)) for item in cart_items]}")
                subtotal = sum(float(item.unit_price) * int(item.quantity) for item in cart_items)
                tax_amount = float(request.data.get('tax_amount', 0))
                discount_amount = float(request.data.get('discount_amount', 0))
                total_amount = float(request.data.get('total_amount', subtotal + tax_amount - discount_amount))
                print(f"Calculated amounts - subtotal: {subtotal}, total: {total_amount}")
                receipt_number = request.data.get('receipt_number', f'POS-{timezone.now().strftime("%Y%m%d%H%M%S")}')

                # Get customer if provided
                customer = None
                if customer_id:
                    from customers.models import Customer
                    try:
                        customer = Customer.objects.get(id=customer_id, is_active=True)
                        cart.customer = customer
                        cart.save()
                    except Customer.DoesNotExist:
                        return Response(
                            {'error': 'Customer not found or inactive'},
                            status=status.HTTP_400_BAD_REQUEST
                        )

                # If this is a hold order, don't create sale or deduct stock
                if is_hold_order:
                    # Serialize and return the cart
                    cart_serializer = CartSerializer(cart)
                    return Response(cart_serializer.data, status=status.HTTP_201_CREATED)

                # Create sale
                print(f"Creating sale with subtotal: {subtotal} (type: {type(subtotal)}), total_amount: {total_amount} (type: {type(total_amount)})")

                # Check credit limit for wholesale customers - REMOVED as per requirements

                from django.db.models.signals import post_save
                from .signals import create_default_payment

                # Temporarily disconnect the signal to prevent duplicate payments
                post_save.disconnect(create_default_payment, sender=Sale)

                sale = Sale.objects.create(
                    cart=cart,
                    customer=customer,
                    shift=current_shift,
                    sale_type=request.data.get('sale_type', 'retail'),
                    total_amount=float(subtotal),
                    tax_amount=float(tax_amount),
                    discount_amount=float(discount_amount),
                    final_amount=float(total_amount),
                    receipt_number=receipt_number
                )

                # Reconnect the signal
                post_save.connect(create_default_payment, sender=Sale)

                # Update shift totals if there's an active shift
                if current_shift:
                    from decimal import Decimal
                    payment_method = request.data.get('payment_method', 'cash').lower()
                    sale_amount = Decimal(str(total_amount))

                    # Update shift totals based on payment method using F expressions to avoid race conditions
                    from django.db.models import F
                    update_fields = ['total_sales']
                    if payment_method == 'split':
                        # For split payments, use the split data
                        split_data = request.data.get('split_data', {})
                        cash_amount = Decimal(str(split_data.get('cash', 0)))
                        mpesa_amount = Decimal(str(split_data.get('mpesa', 0)))
                        current_shift.cash_sales = F('cash_sales') + cash_amount
                        current_shift.mobile_sales = F('mobile_sales') + mpesa_amount
                        update_fields.extend(['cash_sales', 'mobile_sales'])
                    elif payment_method == 'cash':
                        current_shift.cash_sales = F('cash_sales') + sale_amount
                        update_fields.append('cash_sales')
                    elif payment_method in ['mpesa', 'mobile']:
                        current_shift.mobile_sales = F('mobile_sales') + sale_amount
                        update_fields.append('mobile_sales')

                    current_shift.total_sales = F('total_sales') + sale_amount
                    current_shift.save(update_fields=update_fields)

                # Create sale items from cart items
                for cart_item in cart_items:
                    print(f"Creating sale item: quantity={cart_item.quantity} (type: {type(cart_item.quantity)}), unit_price={cart_item.unit_price} (type: {type(cart_item.unit_price)})")
                    SaleItem.objects.create(
                        sale=sale,
                        product=cart_item.product,
                        quantity=int(cart_item.quantity),
                        unit_price=float(cart_item.unit_price),
                        discount=float(cart_item.discount)
                    )

                # Deduct stock using FIFO (First-In, First-Out) from batches
                for deduction in stock_deductions:
                    product = deduction['product']
                    remaining_quantity = deduction['quantity']

                    # Get available batches for this product, ordered by expiry date (FIFO)
                    from inventory.models import Batch
                    available_batches = Batch.objects.filter(
                        product=product,
                        quantity__gt=0
                    ).exclude(
                        status__in=['damaged', 'expired'],  # Exclude damaged/expired batches
                        expiry_date__lt=timezone.now().date()  # Don't use expired batches
                    ).order_by('expiry_date', 'purchase_date')  # FIFO: oldest first

                    batch_deductions = []
                    for batch in available_batches:
                        if remaining_quantity <= 0:
                            break

                        # Calculate how much to take from this batch
                        take_quantity = min(remaining_quantity, batch.quantity)

                        batch_deductions.append({
                            'batch': batch,
                            'quantity': take_quantity
                        })

                        remaining_quantity -= take_quantity

                    # Check if we have enough stock across all batches
                    total_available = sum(d['quantity'] for d in batch_deductions)
                    if total_available < deduction['quantity']:
                        # If no batches found but product has stock, allow the sale (fallback for missing batch data)
                        if not available_batches.exists() and float(product.stock_quantity) >= deduction['quantity']:
                            print(f"No batches found for product {product.name}, but product has {product.stock_quantity} stock. Allowing sale.")
                            # Skip batch deduction logic and just reduce product stock
                            from decimal import Decimal
                            product.stock_quantity = Decimal(str(product.stock_quantity)) - Decimal(str(deduction['quantity']))
                            product.save(update_fields=['stock_quantity'])

                            # Create stock movement record without batch
                            StockMovement.objects.create(
                                product=product,
                                movement_type='out',
                                quantity=-deduction['quantity'],
                                reason=f'Sale {sale.receipt_number} - No batch tracking',
                                user=cashier
                            )

                            # Create sales history record without batch
                            SalesHistory.objects.create(
                                product=product,
                                batch=None,  # No batch
                                quantity=deduction['quantity'],
                                unit_price=deduction['cart_item'].unit_price,
                                cost_price=None,  # Unknown cost
                                total_price=deduction['cart_item'].unit_price * deduction['quantity'],
                                receipt_number=sale.receipt_number,
                                sale_date=sale.sale_date
                            )
                            continue  # Skip the rest of batch processing
                        else:
                            return Response(
                                {'error': f'Insufficient stock for product "{product.name}". Available: {total_available}, Required: {deduction["quantity"]}'},
                                status=status.HTTP_400_BAD_REQUEST
                            )

                    # Apply the deductions
                    for batch_deduction in batch_deductions:
                        batch = batch_deduction['batch']
                        quantity = batch_deduction['quantity']

                        # Reduce batch quantity
                        from decimal import Decimal
                        batch.quantity = Decimal(str(batch.quantity)) - Decimal(str(quantity))
                        batch.save(update_fields=['quantity'])

                        # Deduct from product total stock
                        product.stock_quantity = Decimal(str(product.stock_quantity)) - Decimal(str(quantity))
                        product.save(update_fields=['stock_quantity'])

                        # Create stock movement record
                        StockMovement.objects.create(
                            product=product,
                            movement_type='out',
                            quantity=-quantity,  # Negative for stock out
                            reason=f'Sale {sale.receipt_number} - Batch {batch.batch_number}',
                            user=cashier
                        )

                        # Create sales history record with batch information
                        SalesHistory.objects.create(
                            product=product,
                            batch=batch,
                            quantity=quantity,
                            unit_price=deduction['cart_item'].unit_price,
                            cost_price=batch.cost_price,
                            total_price=deduction['cart_item'].unit_price * quantity,
                            receipt_number=sale.receipt_number,
                            sale_date=sale.sale_date
                        )

                # STRICT VALIDATION: Require payment method and ensure payment creation succeeds
                payment_method = request.data.get('payment_method', '').strip().lower()
                if not payment_method:
                    return Response(
                        {'error': 'Payment method is required for all transactions'},
                        status=status.HTTP_400_BAD_REQUEST
                    )

                # Validate payment method
                valid_payment_methods = ['cash', 'mpesa', 'mobile', 'split']
                if payment_method not in valid_payment_methods:
                    return Response(
                        {'error': f'Invalid payment method. Must be one of: {", ".join(valid_payment_methods)}'},
                        status=status.HTTP_400_BAD_REQUEST
                    )

                # Validate split payment data if split method selected
                if payment_method == 'split':
                    split_data = request.data.get('split_data', {})
                    if not split_data or (split_data.get('cash', 0) == 0 and split_data.get('mpesa', 0) == 0):
                        raise ValueError('Split payment requires cash and/or mpesa amounts in split_data')

                # Create payment record for the sale
                created_payments = []
                if payment_method == 'split':
                    # For split payments, create appropriate payment records
                    split_data = request.data.get('split_data', {})
                    cash_amount = split_data.get('cash', 0)
                    mpesa_amount = split_data.get('mpesa', 0)

                    # Check if it's truly split (both amounts > 0) or pure payment
                    if cash_amount > 0 and mpesa_amount > 0:
                        # True split payment - create two records
                        payment = Payment.objects.create(
                            sale=sale,
                            payment_type='cash',
                            amount=cash_amount,
                            status='completed'
                        )
                        created_payments.append(payment)

                        payment = Payment.objects.create(
                            sale=sale,
                            payment_type='mpesa',
                            amount=mpesa_amount,
                            mpesa_number=request.data.get('mpesa_number', ''),
                            status='completed'
                        )
                        created_payments.append(payment)
                    elif mpesa_amount > 0:
                        # Pure M-Pesa payment
                        payment = Payment.objects.create(
                            sale=sale,
                            payment_type='mpesa',
                            amount=mpesa_amount,
                            mpesa_number=request.data.get('mpesa_number', ''),
                            status='completed'
                        )
                        created_payments.append(payment)
                    elif cash_amount > 0:
                        # Pure cash payment
                        payment = Payment.objects.create(
                            sale=sale,
                            payment_type='cash',
                            amount=cash_amount,
                            status='completed'
                        )
                        created_payments.append(payment)
                else:
                    # Single payment method
                    payment_type = 'cash'
                    if payment_method in ['mpesa', 'mobile']:
                        payment_type = 'mpesa'

                    payment = Payment.objects.create(
                        sale=sale,
                        payment_type=payment_type,
                        amount=total_amount,
                        mpesa_number=request.data.get('mpesa_number', '') if payment_type == 'mpesa' else '',
                        status='completed'
                    )
                    created_payments.append(payment)

                # CRITICAL VALIDATION: Ensure at least one payment was created
                if not created_payments:
                    raise ValueError("No payment records were created for this transaction")

                # Verify payment amounts total matches sale amount
                total_payment_amount = sum(float(p.amount) for p in created_payments)
                if abs(total_payment_amount - float(total_amount)) > 0.01:  # Allow small floating point differences
                    raise ValueError(f"Payment amount mismatch: payments total {total_payment_amount}, sale total {total_amount}")

                # Serialize and return the sale
                serializer = self.get_serializer(sale)
                return Response(serializer.data, status=status.HTTP_201_CREATED)

        except Exception as e:
            print(f"Error creating sale: {str(e)}")
            import traceback
            traceback.print_exc()

            # Provide user-friendly error messages
            error_message = str(e)
            if 'stock' in error_message.lower():
                user_error = '❌ Insufficient Stock'
                user_message = 'One or more items in your cart are out of stock.'
                user_details = 'Please remove out-of-stock items or check inventory levels.'
            elif 'credit' in error_message.lower():
                user_error = '❌ Credit Limit Exceeded'
                user_message = 'The customer has exceeded their credit limit.'
                user_details = 'Please choose a different payment method or contact the customer.'
            elif 'split payment requires' in error_message.lower():
                user_error = '❌ Payment Error'
                user_message = 'Split payment requires cash and/or M-Pesa amounts.'
                user_details = 'Please enter valid amounts for both payment methods or choose a single payment method.'
            else:
                user_error = '❌ Sale Error'
                user_message = 'An unexpected error occurred while processing the sale.'
                user_details = 'Please try again or contact your administrator.'

            return Response({
                'error': user_error,
                'message': user_message,
                'details': user_details,
                'technical_error': error_message if settings.DEBUG else None
            }, status=status.HTTP_400_BAD_REQUEST)

class SaleItemViewSet(viewsets.ModelViewSet):
    queryset = SaleItem.objects.all()
    serializer_class = SaleItemSerializer

class ReturnViewSet(viewsets.ModelViewSet):
    queryset = Return.objects.all()
    serializer_class = ReturnSerializer

class InvoiceViewSet(viewsets.ModelViewSet):
    queryset = Invoice.objects.all()
    serializer_class = InvoiceSerializer
    filter_backends = [DjangoFilterBackend, SearchFilter, OrderingFilter]
    filterset_fields = ['customer', 'status', 'sale']
    search_fields = ['invoice_number', 'customer__name', 'notes']
    ordering_fields = ['invoice_date', 'due_date', 'total_amount', 'created_at']
    ordering = ['-invoice_date']

    @action(detail=True, methods=['post'])
    def mark_paid(self, request, pk=None):
        """Mark invoice as paid"""
        invoice = self.get_object()
        invoice.status = 'paid'
        invoice.save()
        serializer = self.get_serializer(invoice)
        return Response(serializer.data)

    @action(detail=True, methods=['post'])
    def send_invoice(self, request, pk=None):
        """Mark invoice as sent"""
        invoice = self.get_object()
        invoice.status = 'sent'
        invoice.save()
        serializer = self.get_serializer(invoice)
        return Response(serializer.data)

    @action(detail=False, methods=['post'])
    def generate_from_sale(self, request):
        """Generate invoice from an existing sale"""
        sale_id = request.data.get('sale_id')
        customer_id = request.data.get('customer_id')
        due_date = request.data.get('due_date')

        try:
            sale = Sale.objects.get(id=sale_id)
        except Sale.DoesNotExist:
            return Response({'error': 'Sale not found'}, status=status.HTTP_404_NOT_FOUND)

        # Check if invoice already exists for this sale
        if hasattr(sale, 'invoice'):
            return Response({'error': 'Invoice already exists for this sale'}, status=status.HTTP_400_BAD_REQUEST)

        # Get customer (from sale or specified)
        customer = sale.customer
        if customer_id:
            from customers.models import Customer
            try:
                customer = Customer.objects.get(id=customer_id)
            except Customer.DoesNotExist:
                return Response({'error': 'Customer not found'}, status=status.HTTP_404_NOT_FOUND)

        if not customer:
            return Response({'error': 'No customer associated with sale'}, status=status.HTTP_400_BAD_REQUEST)

        # Create invoice
        invoice = Invoice.objects.create(
            sale=sale,
            customer=customer,
            due_date=due_date or (timezone.now().date() + timezone.timedelta(days=30)),
            subtotal=sale.total_amount,
            total_amount=sale.final_amount,
            status='sent',
            notes=f'Invoice generated from sale {sale.receipt_number}'
        )

        # Create invoice items from sale items
        for sale_item in sale.saleitem_set.all():
            InvoiceItem.objects.create(
                invoice=invoice,
                product=sale_item.product,
                description=sale_item.product.name,
                quantity=sale_item.quantity,
                unit_price=sale_item.unit_price,
                tax_rate=0,  # Could be enhanced to include tax
                discount_amount=sale_item.discount
            )

        serializer = self.get_serializer(invoice)
        return Response(serializer.data, status=status.HTTP_201_CREATED)

class InvoiceItemViewSet(viewsets.ModelViewSet):
    queryset = InvoiceItem.objects.all()
    serializer_class = InvoiceItemSerializer
    filter_backends = [DjangoFilterBackend, SearchFilter, OrderingFilter]
    filterset_fields = ['invoice', 'product']
    search_fields = ['description', 'product__name']
    ordering_fields = ['quantity', 'unit_price']
    ordering = ['description']
