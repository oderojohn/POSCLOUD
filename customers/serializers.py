from rest_framework import serializers
from .models import Customer, LoyaltyTransaction

class CustomerSerializer(serializers.ModelSerializer):
    class Meta:
        model = Customer
        fields = '__all__'

class LoyaltyTransactionSerializer(serializers.ModelSerializer):
    customer_name = serializers.CharField(source='customer.name', read_only=True)
    
    class Meta:
        model = LoyaltyTransaction
        fields = '__all__'