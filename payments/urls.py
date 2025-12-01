from django.urls import path
from . import views

urlpatterns = [
    path('', views.PaymentViewSet.as_view({'get': 'list', 'post': 'create'})),
    path('<int:pk>/', views.PaymentViewSet.as_view({'get': 'retrieve', 'put': 'update', 'delete': 'destroy'})),
    path('logs/', views.PaymentLogViewSet.as_view({'get': 'list', 'post': 'create'})),
    path('installments/', views.InstallmentPlanViewSet.as_view({'get': 'list', 'post': 'create'})),
]