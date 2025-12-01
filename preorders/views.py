from rest_framework import viewsets
from .models import Preorder, PreorderPayment
from .serializers import PreorderSerializer, PreorderPaymentSerializer

class PreorderViewSet(viewsets.ModelViewSet):
    queryset = Preorder.objects.all()
    serializer_class = PreorderSerializer

class PreorderPaymentViewSet(viewsets.ModelViewSet):
    queryset = PreorderPayment.objects.all()
    serializer_class = PreorderPaymentSerializer
