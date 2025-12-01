from rest_framework import viewsets, status
from rest_framework.response import Response
from .models import Payment, PaymentLog, InstallmentPlan
from .serializers import PaymentSerializer, PaymentLogSerializer, InstallmentPlanSerializer

class PaymentViewSet(viewsets.ModelViewSet):
    queryset = Payment.objects.all()
    serializer_class = PaymentSerializer

    def create(self, request, *args, **kwargs):
        """
        Custom create method to handle payment creation with proper validation
        """
        try:
            print(f"Creating payment with data: {request.data}")

            # Debug: Print all request data
            print(f"Full request data: {dict(request.data)}")
            print(f"Request data types: {[(k, type(v)) for k, v in request.data.items()]}")

            # Validate amount
            amount = request.data.get('amount')
            print(f"Amount received: {amount} (type: {type(amount)})")
            if amount is not None:
                try:
                    float_amount = float(amount)
                    request.data['amount'] = float_amount
                    print(f"Converted amount to: {float_amount}")
                except (ValueError, TypeError) as e:
                    print(f"Error converting amount: {str(e)}")
                    return Response(
                        {'error': f'Invalid amount format: {amount}'},
                        status=status.HTTP_400_BAD_REQUEST
                    )

            # Ensure payment_type is valid
            payment_type = request.data.get('payment_type', '')
            valid_types = [choice[0] for choice in Payment.PAYMENT_TYPES]
            print(f"Payment type received: '{payment_type}' (type: {type(payment_type)}), valid types: {valid_types}")

            # Handle case-insensitive matching
            payment_type_lower = str(payment_type).lower()
            type_mapping = {
                'cash': 'cash',
                'mpesa': 'mpesa',
                'split': 'split'
            }

            normalized_type = type_mapping.get(payment_type_lower, payment_type)

            if normalized_type not in valid_types:
                print(f"Invalid payment type: {payment_type}, normalized: {normalized_type}")
                return Response(
                    {'error': f'Invalid payment_type "{payment_type}". Valid methods: cash, mpesa, split'},
                    status=status.HTTP_400_BAD_REQUEST
                )

            # Update the request data with normalized type
            request.data['payment_type'] = normalized_type
            print(f"Normalized payment type to: {normalized_type}")

            # Validate sale exists (if provided)
            sale_id = request.data.get('sale')
            customer_id = request.data.get('customer_id') or request.data.get('customer')

            print(f"Validating sale ID: {sale_id} (type: {type(sale_id)})")
            print(f"Customer ID: {customer_id} (type: {type(customer_id)})")

            # Either sale or customer must be provided
            if not sale_id and not customer_id:
                return Response(
                    {'error': 'Either sale or customer must be provided'},
                    status=status.HTTP_400_BAD_REQUEST
                )

            if sale_id:
                try:
                    from sales.models import Sale
                    # Ensure sale_id is converted to the right type
                    if isinstance(sale_id, str):
                        sale_id_int = int(sale_id)
                    else:
                        sale_id_int = sale_id
                    sale = Sale.objects.get(id=sale_id_int)
                    print(f"Sale found: {sale.id} - {sale.receipt_number}")
                except (Sale.DoesNotExist, ValueError, TypeError) as e:
                    print(f"Error validating sale: {str(e)}")
                    return Response(
                        {'error': f'Sale with id {sale_id} not found or invalid'},
                        status=status.HTTP_400_BAD_REQUEST
                    )
                except Exception as e:
                    print(f"Unexpected error validating sale: {str(e)}")
                    return Response(
                        {'error': f'Error validating sale: {str(e)}'},
                        status=status.HTTP_400_BAD_REQUEST
                    )

            # Validate customer exists (if provided)
            if customer_id:
                try:
                    from customers.models import Customer
                    if isinstance(customer_id, str):
                        customer_id_int = int(customer_id)
                    else:
                        customer_id_int = customer_id
                    customer = Customer.objects.get(id=customer_id_int)
                    print(f"Customer found: {customer.id} - {customer.name}")
                    request.data['customer'] = customer_id_int
                except Exception as e:
                    print(f"Error validating customer: {str(e)}")
                    return Response(
                        {'error': f'Customer with id {customer_id} not found or invalid'},
                        status=status.HTTP_400_BAD_REQUEST
                    )

            # Debug: Print final request data before creating
            print(f"Final request data before creation: {dict(request.data)}")

            # Final validation - ensure all data is in correct format
            try:
                # Test that we can create the payment object
                test_data = {
                    'payment_type': request.data['payment_type'],
                    'amount': request.data['amount'],
                    'reference_number': request.data.get('reference_number', ''),
                    'status': 'completed'
                }

                # Only add sale_id if it exists (for credit payments, sale will be None)
                if request.data.get('sale') is not None:
                    test_data['sale_id'] = request.data['sale']

                # Add split_data if payment type is split
                if request.data.get('payment_type') == 'split' and request.data.get('split_data'):
                    test_data['split_data'] = request.data['split_data']

                print(f"Test payment data: {test_data}")

                # Try to validate the data format
                if request.data.get('sale') is not None and not isinstance(request.data['sale'], int):
                    raise ValueError(f"Sale ID must be integer, got {type(request.data['sale'])}")
                if not isinstance(test_data['amount'], (int, float)):
                    raise ValueError(f"Amount must be numeric, got {type(test_data['amount'])}")

            except Exception as e:
                print(f"Data validation error: {str(e)}")
                return Response(
                    {'error': f'Invalid payment data format: {str(e)}'},
                    status=status.HTTP_400_BAD_REQUEST
                )

            # Additional validation for required fields
            required_fields = ['payment_type', 'amount']
            for field in required_fields:
                value = request.data.get(field)
                print(f"Field {field}: {value} (type: {type(value)})")
                if value is None or value == '':
                    return Response(
                        {'error': f'Missing or empty required field: {field}'},
                        status=status.HTTP_400_BAD_REQUEST
                    )

            # For credit payments (not associated with sales), ensure customer is provided
            if not sale_id and not customer_id:
                return Response(
                    {'error': 'Either sale or customer must be provided for payment'},
                    status=status.HTTP_400_BAD_REQUEST
                )

            # Try to create the payment object manually to catch any validation errors
            try:
                payment_data = {
                    'payment_type': request.data['payment_type'],
                    'amount': request.data['amount'],
                    'reference_number': request.data.get('reference_number', ''),
                    'mpesa_number': request.data.get('mpesa_number', ''),
                    'description': request.data.get('description', ''),
                    'status': 'completed'
                }

                # Add sale or customer based on what's provided
                if sale_id:
                    payment_data['sale_id'] = request.data['sale']
                if customer_id:
                    payment_data['customer_id'] = request.data['customer']

                # Add split_data if payment type is split
                if request.data.get('payment_type') == 'split' and request.data.get('split_data'):
                    payment_data['split_data'] = request.data['split_data']

                payment = Payment(**payment_data)
                # Validate the model
                payment.full_clean()
                print("Payment object validation passed")
            except Exception as validation_error:
                print(f"Payment validation error: {str(validation_error)}")
                return Response(
                    {'error': f'Payment validation failed: {str(validation_error)}'},
                    status=status.HTTP_400_BAD_REQUEST
                )

            # Final check - ensure the sale exists and is accessible (only if sale is provided)
            if request.data.get('sale') is not None:
                try:
                    from sales.models import Sale
                    sale_id = request.data['sale']
                    print(f"Final sale check - ID: {sale_id}, type: {type(sale_id)}")
                    sale = Sale.objects.get(id=sale_id)
                    print(f"Final sale check passed: {sale.receipt_number}")
                except Exception as e:
                    print(f"Final sale validation failed: {str(e)}")
                    return Response(
                        {'error': f'Cannot access sale: {str(e)}'},
                        status=status.HTTP_400_BAD_REQUEST
                    )

            # Call parent create method
            response = super().create(request, *args, **kwargs)

            # Payment processing completed

            return response

        except Exception as e:
            print(f"Error creating payment: {str(e)}")
            import traceback
            traceback.print_exc()
            return Response(
                {'error': f'Error creating payment: {str(e)}', 'details': str(e), 'traceback': traceback.format_exc()},
                status=status.HTTP_400_BAD_REQUEST
            )

class PaymentLogViewSet(viewsets.ModelViewSet):
    queryset = PaymentLog.objects.all()
    serializer_class = PaymentLogSerializer

class InstallmentPlanViewSet(viewsets.ModelViewSet):
    queryset = InstallmentPlan.objects.all()
    serializer_class = InstallmentPlanSerializer
