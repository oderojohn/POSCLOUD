from django.urls import path
from . import views

urlpatterns = [
    path('accounting/', views.AccountingSyncView.as_view()),
    path('ecommerce/', views.EcommerceSyncView.as_view()),
]