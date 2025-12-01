from rest_framework import serializers
from .models import Chit

class ChitSerializer(serializers.ModelSerializer):
    customer_name = serializers.CharField(source='customer.name', read_only=True)
    items = serializers.ListField(
        child=serializers.DictField(),
        write_only=True,
        required=False
    )

    class Meta:
        model = Chit
        fields = '__all__'

    def create(self, validated_data):
        items = validated_data.pop('items', [])
        chit = Chit.objects.create(**validated_data)
        # Note: Items are not stored in the database for chits
        # They're just used for display/calculation purposes
        return chit