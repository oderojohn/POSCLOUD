from django.urls import path, include
from rest_framework.routers import DefaultRouter
from . import views

router = DefaultRouter()
router.register(r'preorders', views.PreorderViewSet)
router.register(r'preorder-payments', views.PreorderPaymentViewSet)

urlpatterns = [
    path('', include(router.urls)),
]