from rest_framework import viewsets, status
from rest_framework.response import Response
from .models import Chit
from .serializers import ChitSerializer

class ChitViewSet(viewsets.ModelViewSet):
    queryset = Chit.objects.all()
    serializer_class = ChitSerializer

    def create(self, request, *args, **kwargs):
        """
        Custom create method to handle chit creation with proper field mapping
        """
        try:
            # Map frontend fields to backend fields
            data = request.data.copy()
            data['customer_name'] = data.get('customer_name', 'Walk-in Customer')
            data['amount'] = data.get('total_amount', 0)
            data['description'] = data.get('description', f"Table {data.get('table_number', 'N/A')}")

            # Remove items from data as they're not stored in the database
            data.pop('items', None)

            # Create chit
            serializer = self.get_serializer(data=data)
            serializer.is_valid(raise_exception=True)
            chit = serializer.save()

            # Return response with items for frontend
            response_data = serializer.data
            response_data['items'] = request.data.get('items', [])

            return Response(response_data, status=status.HTTP_201_CREATED)

        except Exception as e:
            return Response(
                {'error': f'Error creating chit: {str(e)}'},
                status=status.HTTP_400_BAD_REQUEST
            )
