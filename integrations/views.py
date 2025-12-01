from rest_framework.views import APIView
from rest_framework.response import Response

class AccountingSyncView(APIView):
    def get(self, request):
        # Placeholder for accounting sync
        return Response({'message': 'Accounting sync triggered'})

class EcommerceSyncView(APIView):
    def get(self, request):
        # Placeholder for ecommerce sync
        return Response({'message': 'Ecommerce sync triggered'})