from rest_framework import serializers
from .models import Branch

class BranchSerializer(serializers.ModelSerializer):
    manager_name = serializers.CharField(source='manager.user.username', read_only=True)
    
    class Meta:
        model = Branch
        fields = '__all__'