from django.urls import path
from . import views

urlpatterns = [
    path('<int:customer_pk>/loyalty/', views.LoyaltyView.as_view()),
    path('lookup/', views.CustomerLookupView.as_view(), name='customer-lookup'),
]