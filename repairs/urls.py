from django.urls import path
from . import views

urlpatterns = [
    path('<int:repair_pk>/parts/', views.RepairPartsView.as_view()),
]