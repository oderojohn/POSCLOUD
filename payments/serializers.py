from rest_framework import serializers
from .models import Payment, PaymentLog, InstallmentPlan

class PaymentSerializer(serializers.ModelSerializer):
    sale_receipt = serializers.CharField(source='sale.receipt_number', read_only=True)
    split_data = serializers.JSONField(required=False, allow_null=True)

    class Meta:
        model = Payment
        fields = '__all__'
        read_only_fields = ('id', 'created_at')

    def validate_sale(self, value):
        """
        Validate that the sale exists
        """
        if value:
            from sales.models import Sale

            # If value is already a Sale object, return it
            if isinstance(value, Sale):
                return value

            # If value is a dictionary (nested object), extract the id
            if isinstance(value, dict):
                sale_id = value.get('id')
                if not sale_id:
                    raise serializers.ValidationError('Sale object must contain an id field')
                value = sale_id

            # Try to get the sale by ID
            try:
                sale = Sale.objects.get(id=value)
                return sale
            except Sale.DoesNotExist:
                raise serializers.ValidationError(f'Sale with id {value} not found')
            except (ValueError, TypeError):
                raise serializers.ValidationError(f'Invalid sale identifier: {value}')
        return value

    def validate_payment_type(self, value):
        """
        Validate payment_type against choices
        """
        valid_types = [choice[0] for choice in Payment.PAYMENT_TYPES]
        if value not in valid_types:
            raise serializers.ValidationError(f'Invalid payment_type. Must be one of: {valid_types}')
        return value

    def validate_amount(self, value):
        """
        Validate and convert amount to proper numeric type
        """
        try:
            return float(value)
        except (ValueError, TypeError):
            raise serializers.ValidationError(f'Amount must be a valid number, got: {value}')

class PaymentLogSerializer(serializers.ModelSerializer):
    payment_reference = serializers.CharField(source='payment.reference_number', read_only=True)

    class Meta:
        model = PaymentLog
        fields = '__all__'

class InstallmentPlanSerializer(serializers.ModelSerializer):
    sale_receipt = serializers.CharField(source='sale.receipt_number', read_only=True)

    class Meta:
        model = InstallmentPlan
        fields = '__all__'