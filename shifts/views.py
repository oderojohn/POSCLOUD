from rest_framework import viewsets, generics, status
from rest_framework.response import Response
from django.utils import timezone
from django_filters.rest_framework import DjangoFilterBackend
from rest_framework.filters import SearchFilter, OrderingFilter
from .models import Shift
from .serializers import ShiftSerializer

class ShiftViewSet(viewsets.ModelViewSet):
    queryset = Shift.objects.all()
    serializer_class = ShiftSerializer
    filter_backends = [DjangoFilterBackend, SearchFilter, OrderingFilter]
    filterset_fields = ['cashier', 'status', 'start_time', 'end_time']
    search_fields = ['cashier__user__username', 'cashier__user__first_name', 'cashier__user__last_name']
    ordering_fields = ['start_time', 'end_time', 'total_sales', 'transaction_count']
    ordering = ['-start_time']

class StartShiftView(generics.CreateAPIView):
    serializer_class = ShiftSerializer

    def create(self, request, *args, **kwargs):
        # Get or create user profile for authenticated user
        from users.models import UserProfile
        user_profile, created = UserProfile.objects.get_or_create(
            user=request.user,
            defaults={'role': 'cashier'}
        )

        cashier = user_profile
        # Frontend sends 'starting_cash', backend expects 'opening_balance'
        opening_balance = request.data.get('starting_cash', 0)

        # Check if there's an open shift
        existing_shift = Shift.objects.filter(cashier=cashier, status='open').first()
        if existing_shift:
            return Response({'error': 'Shift already open'}, status=status.HTTP_400_BAD_REQUEST)
        shift = Shift.objects.create(cashier=cashier, opening_balance=opening_balance)
        serializer = self.get_serializer(shift)
        return Response(serializer.data, status=status.HTTP_201_CREATED)

class EndShiftView(generics.CreateAPIView):
    serializer_class = ShiftSerializer

    def create(self, request, *args, **kwargs):
        from users.models import UserProfile
        try:
            user_profile = request.user.userprofile
        except UserProfile.DoesNotExist:
            user_profile = UserProfile.objects.create(
                user=request.user,
                role='cashier'
            )

        cashier = user_profile
        # Frontend sends 'ending_cash', backend expects 'closing_balance'
        closing_balance = float(request.data.get('ending_cash', 0))

        try:
            shift = Shift.objects.get(cashier=cashier, status='open')
            print(f"EndShiftView: Found open shift {shift.id} for user {cashier.user.username}")
        except Shift.DoesNotExist:
            print(f"EndShiftView: No open shift found for user {cashier.user.username}")
            return Response({
                'error': '❌ No Active Shift',
                'message': 'You do not have an active shift to end.',
                'details': 'Please start a shift first before attempting to end it.',
                'action_required': 'Start a shift first'
            }, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            print(f"EndShiftView: Error finding shift: {str(e)}")
            return Response({
                'error': '❌ Shift Error',
                'message': 'An error occurred while accessing your shift information.',
                'details': 'Please contact your administrator if this problem persists.',
                'action_required': 'Contact administrator'
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        # Check for held orders before allowing shift end
        from sales.models import Cart
        held_orders_count = Cart.objects.filter(cashier=cashier, status='held').count()
        if held_orders_count > 0:
            return Response({
                'error': f'⚠️ Cannot End Shift',
                'message': f'You have {held_orders_count} unfinished held order(s) that need to be completed or cancelled before ending your shift.',
                'details': 'Please review your held orders and either complete the payments or cancel them before closing the shift.',
                'action_required': 'Complete or cancel all held orders first'
            }, status=status.HTTP_400_BAD_REQUEST)

        # Perform cash register reconciliation
        from decimal import Decimal

        # Calculate expected closing balance
        # Expected = Opening Balance + Cash Sales (only cash payments count towards physical cash)
        expected_balance = Decimal(str(shift.opening_balance)) + Decimal(str(shift.cash_sales))

        # Convert closing balance to Decimal for consistency
        actual_closing_balance = Decimal(str(closing_balance))

        # Calculate discrepancy (Actual - Expected)
        # Positive discrepancy = Overage (cashier has more than expected)
        # Negative discrepancy = Shortage (cashier has less than expected)
        discrepancy = actual_closing_balance - expected_balance

        # Update shift with closing information
        shift.end_time = timezone.now()
        shift.closing_balance = actual_closing_balance
        shift.discrepancy = discrepancy
        shift.status = 'closed'

        # Final totals are already calculated during sales
        shift.save()
        print(f"EndShiftView: Shift {shift.id} closed successfully for user {cashier.user.username}")
        print(f"EndShiftView: Expected: {expected_balance}, Actual: {actual_closing_balance}, Discrepancy: {discrepancy}")

        serializer = self.get_serializer(shift)

        # Return comprehensive shift data with detailed reconciliation
        response_data = serializer.data
        response_data.update({
            'reconciliation': {
                'opening_balance': float(shift.opening_balance),
                'cash_sales': float(shift.cash_sales),
                'card_sales': float(shift.card_sales),
                'mobile_sales': float(shift.mobile_sales),
                'total_sales': float(shift.total_sales),
                'expected_closing_balance': float(expected_balance),
                'actual_closing_balance': float(actual_closing_balance),
                'discrepancy': float(discrepancy),
                'discrepancy_type': 'overage' if discrepancy > 0 else 'shortage' if discrepancy < 0 else 'balanced',
                'discrepancy_description': (
                    f"{'Overage' if discrepancy > 0 else 'Shortage' if discrepancy < 0 else 'Balanced'}: "
                    f"Ksh {abs(float(discrepancy)):.2f}"
                )
            },
            'status_message': (
                f"Shift closed successfully!\n"
                f"Expected Cash: Ksh {float(expected_balance):.2f}\n"
                f"Actual Cash: Ksh {float(actual_closing_balance):.2f}\n"
                f"{'Overage' if discrepancy > 0 else 'Shortage' if discrepancy < 0 else 'Balanced'}: "
                f"Ksh {abs(float(discrepancy)):.2f}"
            )
        })

        return Response(response_data, status=status.HTTP_200_OK)

class CurrentShiftView(generics.GenericAPIView):
    serializer_class = ShiftSerializer

    def get(self, request, *args, **kwargs):
        from users.models import UserProfile
        # Get or create user profile for authenticated user
        user_profile, created = UserProfile.objects.get_or_create(
            user=request.user,
            defaults={'role': 'cashier'}
        )
        if created:
            print(f"CurrentShiftView: Created UserProfile for user {request.user}")

        try:
            shift = Shift.objects.get(cashier=user_profile, status='open')
            print(f"CurrentShiftView: Found open shift {shift.id} for user {user_profile.user.username}")
            print(f"CurrentShiftView: Shift data - status: {shift.status}, start_time: {shift.start_time}")
            serializer = self.get_serializer(shift)
            data = serializer.data
            print(f"CurrentShiftView: Serialized data: {data}")
            return Response(data, status=status.HTTP_200_OK)
        except Shift.DoesNotExist:
            print(f"CurrentShiftView: No open shift found for user {user_profile.user.username}")
            return Response({'detail': 'No active shift found'}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            print(f"CurrentShiftView: Error serializing shift: {e}")
            import traceback
            traceback.print_exc()
            return Response({'error': f'Error retrieving shift data: {str(e)}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class AllShiftsView(generics.ListAPIView):
    serializer_class = ShiftSerializer
    filter_backends = [DjangoFilterBackend, SearchFilter, OrderingFilter]
    filterset_fields = ['cashier', 'status', 'start_time', 'end_time']
    search_fields = ['cashier__user__username', 'cashier__user__first_name', 'cashier__user__last_name']
    ordering_fields = ['start_time', 'end_time', 'total_sales', 'transaction_count']
    ordering = ['-start_time']

    def get_queryset(self):
        queryset = Shift.objects.all()

        # Filter by date range if provided
        start_date = self.request.query_params.get('start_date')
        end_date = self.request.query_params.get('end_date')

        if start_date:
            queryset = queryset.filter(start_time__date__gte=start_date)
        if end_date:
            queryset = queryset.filter(start_time__date__lte=end_date)

        return queryset
