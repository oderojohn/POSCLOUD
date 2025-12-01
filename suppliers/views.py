from rest_framework import viewsets, status
from rest_framework.decorators import action
from rest_framework.response import Response
from django_filters.rest_framework import DjangoFilterBackend
from rest_framework.filters import SearchFilter, OrderingFilter
from .models import Supplier, SupplierPriceHistory, PurchaseOrder, PurchaseOrderItem
from .serializers import SupplierSerializer, SupplierPriceHistorySerializer, PurchaseOrderSerializer, PurchaseOrderItemSerializer

class SupplierViewSet(viewsets.ModelViewSet):
    queryset = Supplier.objects.all()
    serializer_class = SupplierSerializer
    filter_backends = [SearchFilter, OrderingFilter]
    search_fields = ['name', 'contact_person', 'phone', 'email']
    ordering_fields = ['name']
    ordering = ['name']

class SupplierPriceHistoryViewSet(viewsets.ModelViewSet):
    queryset = SupplierPriceHistory.objects.all()
    serializer_class = SupplierPriceHistorySerializer
    filter_backends = [DjangoFilterBackend, SearchFilter, OrderingFilter]
    filterset_fields = ['supplier', 'product']
    search_fields = ['supplier__name', 'product__name']
    ordering_fields = ['date']
    ordering = ['-date']

class PurchaseOrderViewSet(viewsets.ModelViewSet):
    queryset = PurchaseOrder.objects.all()
    serializer_class = PurchaseOrderSerializer
    filter_backends = [DjangoFilterBackend, SearchFilter, OrderingFilter]
    filterset_fields = ['supplier', 'status']
    search_fields = ['order_number', 'supplier__name']
    ordering_fields = ['order_date', 'expected_delivery_date']
    ordering = ['-order_date']
    http_method_names = ['get', 'post', 'put', 'patch', 'delete']  # Explicitly allow PATCH

    def partial_update(self, request, *args, **kwargs):
        """Handle PATCH requests for partial updates"""
        kwargs['partial'] = True
        return super().partial_update(request, *args, **kwargs)

    @action(detail=True, methods=['post'])
    def receive_batch(self, request, pk=None):
        """Receive a batch for a purchase order item"""
        purchase_order = self.get_object()
        item_id = request.data.get('item_id')
        batch_number = request.data.get('batch_number')
        quantity = request.data.get('quantity')
        expiry_date = request.data.get('expiry_date')

        try:
            item = purchase_order.items.get(id=item_id)
        except PurchaseOrderItem.DoesNotExist:
            return Response({'error': 'Purchase order item not found'}, status=status.HTTP_404_NOT_FOUND)

        if item.received_quantity + quantity > item.quantity:
            return Response({'error': 'Received quantity exceeds ordered quantity'}, status=status.HTTP_400_BAD_REQUEST)

        # Create batch
        from inventory.models import Batch
        batch = Batch.objects.create(
            product=item.product,
            batch_number=batch_number,
            quantity=quantity,
            expiry_date=expiry_date,
            purchase_date=purchase_order.order_date,
            supplier=purchase_order.supplier,
            purchase_order_item=item,
            status='ordered'
        )

        # Receive the batch (adds to stock)
        batch.receive_batch()

        # Update the purchase order item's received quantity
        item.received_quantity += quantity
        item.save()

        return Response({'message': 'Batch received successfully', 'batch_id': batch.id})

class PurchaseOrderItemViewSet(viewsets.ModelViewSet):
    queryset = PurchaseOrderItem.objects.all()
    serializer_class = PurchaseOrderItemSerializer
    filter_backends = [DjangoFilterBackend, SearchFilter, OrderingFilter]
    filterset_fields = ['purchase_order', 'product']
    search_fields = ['product__name', 'batch_number']
    ordering_fields = ['product__name']
    ordering = ['product__name']
