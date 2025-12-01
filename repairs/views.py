from rest_framework import viewsets, generics
from .models import Repair, RepairPart
from .serializers import RepairSerializer, RepairPartSerializer

class RepairViewSet(viewsets.ModelViewSet):
    queryset = Repair.objects.all()
    serializer_class = RepairSerializer

class RepairPartViewSet(viewsets.ModelViewSet):
    queryset = RepairPart.objects.all()
    serializer_class = RepairPartSerializer

class RepairPartsView(generics.ListCreateAPIView):
    serializer_class = RepairPartSerializer

    def get_queryset(self):
        repair_pk = self.kwargs['repair_pk']
        return RepairPart.objects.filter(repair_id=repair_pk)

    def perform_create(self, serializer):
        repair_pk = self.kwargs['repair_pk']
        repair = generics.get_object_or_404(Repair, pk=repair_pk)
        serializer.save(repair=repair)
