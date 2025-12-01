from rest_framework import serializers
from .models import Repair, RepairPart

class RepairPartSerializer(serializers.ModelSerializer):
    product_name = serializers.CharField(source='product.name', read_only=True)
    
    class Meta:
        model = RepairPart
        fields = '__all__'

class RepairSerializer(serializers.ModelSerializer):
    customer_name = serializers.CharField(source='customer.name', read_only=True)
    technician_name = serializers.CharField(source='technician.user.username', read_only=True)
    parts = RepairPartSerializer(many=True, read_only=True)
    
    class Meta:
        model = Repair
        fields = '__all__'