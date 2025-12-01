from rest_framework import serializers
from .models import Preorder, PreorderPayment

class PreorderSerializer(serializers.ModelSerializer):
    customer_name = serializers.CharField(source='customer.name', read_only=True)
    product_name = serializers.CharField(source='product.name', read_only=True)
    
    class Meta:
        model = Preorder
        fields = '__all__'

class PreorderPaymentSerializer(serializers.ModelSerializer):
    preorder_product = serializers.CharField(source='preorder.product.name', read_only=True)
    
    class Meta:
        model = PreorderPayment
        fields = '__all__'