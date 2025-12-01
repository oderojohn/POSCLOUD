from rest_framework import viewsets, generics, status
from rest_framework.response import Response
from django_filters.rest_framework import DjangoFilterBackend
from rest_framework.filters import SearchFilter, OrderingFilter
from .models import Customer, LoyaltyTransaction
from .serializers import CustomerSerializer, LoyaltyTransactionSerializer

class CustomerViewSet(viewsets.ModelViewSet):
    queryset = Customer.objects.all()
    serializer_class = CustomerSerializer
    filter_backends = [DjangoFilterBackend, SearchFilter, OrderingFilter]
    filterset_fields = ['customer_type', 'is_active']
    search_fields = ['name', 'phone', 'email', 'business_name']
    ordering_fields = ['name', 'created_at']
    ordering = ['name']

    def get_queryset(self):
        queryset = Customer.objects.all()
        mode = self.request.query_params.get('mode', 'retail')

        if mode == 'wholesale':
            # In wholesale mode, show only wholesale customers
            queryset = queryset.filter(customer_type='wholesale')
        elif mode == 'retail':
            # In retail mode, show only retail customers
            queryset = queryset.filter(customer_type='retail')

        return queryset

class LoyaltyTransactionViewSet(viewsets.ModelViewSet):
    queryset = LoyaltyTransaction.objects.all()
    serializer_class = LoyaltyTransactionSerializer

class LoyaltyView(generics.RetrieveUpdateAPIView):
    serializer_class = CustomerSerializer

    def get_object(self):
        customer = generics.get_object_or_404(Customer, pk=self.kwargs['customer_pk'])
        return customer

    def retrieve(self, request, *args, **kwargs):
        customer = self.get_object()
        return Response({'loyalty_points': customer.loyalty_points})

class CustomerLookupView(generics.RetrieveAPIView):
    serializer_class = CustomerSerializer

    def get_object(self):
        phone = self.request.query_params.get('phone')
        if not phone:
            from django.http import Http404
            raise Http404("Phone number required")

        try:
            customer = Customer.objects.get(phone=phone, is_active=True)
            return customer
        except Customer.DoesNotExist:
            from django.http import Http404
            raise Http404("Customer not found")

    def update(self, request, *args, **kwargs):
        customer = self.get_object()
        points = request.data.get('points', 0)
        transaction_type = request.data.get('type', 'earn')  # earn or redeem
        reason = request.data.get('reason', '')

        if transaction_type == 'earn':
            customer.loyalty_points += points
        elif transaction_type == 'redeem':
            if customer.loyalty_points >= points:
                customer.loyalty_points -= points
            else:
                return Response({'error': 'Insufficient points'}, status=status.HTTP_400_BAD_REQUEST)
        customer.save()

        # Create transaction
        LoyaltyTransaction.objects.create(
            customer=customer,
            transaction_type=transaction_type,
            points=points,
            reason=reason
        )

        return Response({'loyalty_points': customer.loyalty_points})
