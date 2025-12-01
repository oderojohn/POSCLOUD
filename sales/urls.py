from django.urls import path, include
from rest_framework.routers import DefaultRouter
from . import views

router = DefaultRouter()
router.register(r'carts', views.CartViewSet)
router.register(r'cart-items', views.CartItemViewSet)
router.register(r'sales', views.SaleViewSet)
router.register(r'sale-items', views.SaleItemViewSet)
router.register(r'returns', views.ReturnViewSet)
router.register(r'invoices', views.InvoiceViewSet)
router.register(r'invoice-items', views.InvoiceItemViewSet)

urlpatterns = [
    path('', include(router.urls)),
]