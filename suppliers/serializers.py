from rest_framework import serializers
from .models import Supplier, SupplierPriceHistory, PurchaseOrder, PurchaseOrderItem

class SupplierSerializer(serializers.ModelSerializer):
    class Meta:
        model = Supplier
        fields = '__all__'

class SupplierPriceHistorySerializer(serializers.ModelSerializer):
    supplier_name = serializers.CharField(source='supplier.name', read_only=True)
    product_name = serializers.CharField(source='product.name', read_only=True)
    
    class Meta:
        model = SupplierPriceHistory
        fields = '__all__'

class PurchaseOrderItemSerializer(serializers.ModelSerializer):
    product_name = serializers.CharField(source='product.name', read_only=True)
    total_price = serializers.DecimalField(max_digits=10, decimal_places=2, read_only=True)
    is_fully_received = serializers.BooleanField(read_only=True)

    class Meta:
        model = PurchaseOrderItem
        fields = '__all__'

class PurchaseOrderSerializer(serializers.ModelSerializer):
    supplier_name = serializers.CharField(source='supplier.name', read_only=True)
    items = PurchaseOrderItemSerializer(many=True, read_only=True)

    class Meta:
        model = PurchaseOrder
        fields = '__all__'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Make all fields not required for partial updates
        if getattr(self, 'partial', False):
            for field_name, field in self.fields.items():
                field.required = False

    def create(self, validated_data):
        items_data = self.initial_data.get('items', [])
        purchase_order = super().create(validated_data)

        # Create order items
        for item_data in items_data:
            PurchaseOrderItem.objects.create(
                purchase_order=purchase_order,
                product_id=item_data['product'],
                quantity=item_data['quantity'],
                unit_price=item_data['unit_price'],
                batch_number=item_data.get('batch_number', ''),
                expiry_date=item_data.get('expiry_date')
            )

        # Update total amount
        purchase_order.total_amount = sum(item.total_price for item in purchase_order.items.all())
        purchase_order.save()

        return purchase_order

    def update(self, instance, validated_data):
        # Handle status-only updates (like approving orders)
        if 'status' in validated_data and len(validated_data) == 1:
            new_status = validated_data['status']
            instance.status = new_status
            instance.save()
            # Only recalculate status if not manually setting to cancelled or pending
            if new_status not in ['cancelled', 'pending']:
                instance.update_status()
            return instance

        # Handle other updates (full or partial)
        # Update basic fields
        for attr, value in validated_data.items():
            setattr(instance, attr, value)
        instance.save()

        # Check if items are provided in the request (for editing orders)
        items_data = self.initial_data.get('items')
        if items_data is not None:  # Only update items if explicitly provided
            # Clear existing items
            instance.items.all().delete()

            # Create new items
            for item_data in items_data:
                PurchaseOrderItem.objects.create(
                    purchase_order=instance,
                    product_id=item_data['product'],
                    quantity=item_data['quantity'],
                    unit_price=item_data['unit_price'],
                    batch_number=item_data.get('batch_number', ''),
                    expiry_date=item_data.get('expiry_date')
                )

            # Update total amount
            instance.total_amount = sum(item.total_price for item in instance.items.all())
            instance.save()

        return instance