from rest_framework import serializers
from .models import Category, Product, Batch, StockMovement, Supplier, Purchase, PriceHistory, SalesHistory, ProductHistory

class CategorySerializer(serializers.ModelSerializer):
    class Meta:
        model = Category
        fields = '__all__'

class ProductSerializer(serializers.ModelSerializer):
    category_name = serializers.SerializerMethodField()
    is_low_stock = serializers.BooleanField(read_only=True)
    display_price = serializers.SerializerMethodField()

    def get_category_name(self, obj):
        return obj.category.name if obj.category else 'N/A'

    def get_display_price(self, obj):
        """
        Return the appropriate price based on the mode (retail/wholesale)
        """
        request = self.context.get('request')
        if request:
            mode = request.query_params.get('mode', 'retail')
            if mode == 'wholesale' and obj.wholesale_price and obj.wholesale_price > 0:
                return obj.wholesale_price
        return obj.selling_price

    class Meta:
        model = Product
        fields = '__all__'

class BatchSerializer(serializers.ModelSerializer):
    product_name = serializers.SerializerMethodField()
    supplier_name = serializers.SerializerMethodField()

    def get_product_name(self, obj):
        return obj.product.name if obj.product else 'N/A'

    def get_supplier_name(self, obj):
        return obj.supplier.name if obj.supplier else 'N/A'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Make certain fields read-only for updates
        if self.instance and not isinstance(self.instance, list):
            # For existing batches, make status read-only if received
            if hasattr(self.instance, 'status') and self.instance.status == 'received':
                self.fields['status'].read_only = True
                self.fields['received_date'].read_only = True

    class Meta:
        model = Batch
        fields = '__all__'
        read_only_fields = ('received_date', 'purchase_order_item')  # Always read-only

class StockMovementSerializer(serializers.ModelSerializer):
    product_name = serializers.SerializerMethodField()
    user_name = serializers.SerializerMethodField()

    def get_product_name(self, obj):
        return obj.product.name if obj.product else 'N/A'

    def get_user_name(self, obj):
        return obj.user.user.username if obj.user and obj.user.user else 'N/A'

    class Meta:
        model = StockMovement
        fields = '__all__'

class SupplierSerializer(serializers.ModelSerializer):
    class Meta:
        model = Supplier
        fields = '__all__'

class PurchaseSerializer(serializers.ModelSerializer):
    product_name = serializers.SerializerMethodField()
    supplier_name = serializers.SerializerMethodField()

    def get_product_name(self, obj):
        return obj.product.name if obj.product else 'N/A'

    def get_supplier_name(self, obj):
        return obj.supplier.name if obj.supplier else 'N/A'

    class Meta:
        model = Purchase
        fields = '__all__'

class PriceHistorySerializer(serializers.ModelSerializer):
    product_name = serializers.SerializerMethodField()
    supplier_name = serializers.SerializerMethodField()

    def get_product_name(self, obj):
        return obj.product.name if obj.product else 'N/A'

    def get_supplier_name(self, obj):
        return obj.supplier.name if obj.supplier else 'N/A'

    class Meta:
        model = PriceHistory
        fields = '__all__'

class SalesHistorySerializer(serializers.ModelSerializer):
    product_name = serializers.SerializerMethodField()
    customer_name = serializers.SerializerMethodField()

    def get_product_name(self, obj):
        return obj.product.name if obj.product else 'N/A'

    def get_customer_name(self, obj):
        return obj.customer.name if obj.customer else 'N/A'

    class Meta:
        model = SalesHistory
        fields = '__all__'

class ProductHistorySerializer(serializers.ModelSerializer):
    product_name = serializers.SerializerMethodField()
    user_name = serializers.SerializerMethodField()

    def get_product_name(self, obj):
        return obj.product.name if obj.product else 'N/A'

    def get_user_name(self, obj):
        return obj.user.user.username if obj.user and obj.user.user else 'N/A'

    class Meta:
        model = ProductHistory
        fields = '__all__'