from rest_framework import serializers
from .models import Shift
from sales.serializers import SaleSerializer

class ShiftSerializer(serializers.ModelSerializer):
    cashier_name = serializers.SerializerMethodField()
    approved_by_name = serializers.SerializerMethodField()
    transaction_count = serializers.SerializerMethodField()
    sales = SaleSerializer(source='sale_set', many=True, read_only=True)

    class Meta:
        model = Shift
        fields = [
            'id', 'cashier', 'start_time', 'end_time', 'opening_balance',
            'closing_balance', 'cash_sales', 'card_sales', 'mobile_sales',
            'total_sales', 'status', 'discrepancy', 'approved_by',
            'cashier_name', 'approved_by_name', 'transaction_count', 'sales'
        ]

    def get_transaction_count(self, obj):
        try:
            return obj.sale_set.count()
        except:
            return 0

    def get_cashier_name(self, obj):
        try:
            return obj.cashier.user.username
        except:
            return None

    def get_approved_by_name(self, obj):
        try:
            return obj.approved_by.user.username if obj.approved_by else None
        except:
            return None