"""
URL configuration for myshop project.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/5.2/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path, include
from suppliers.views import SupplierPriceHistoryViewSet
from inventory.views import ProductViewSet
from suppliers.views import SupplierViewSet
from customers.views import CustomerViewSet
from repairs.views import RepairViewSet
from preorders.views import PreorderViewSet
from shifts.views import ShiftViewSet
from branches.views import BranchViewSet

urlpatterns = [
    path('admin/', admin.site.urls),
    path('api/users/', include('users.urls')),
    path('api/auth/', include('users.urls')),
    path('api/roles/', include('users.urls')),
    path('api/products/', ProductViewSet.as_view({'get': 'list', 'post': 'create'})),
    path('api/products/<int:pk>/', ProductViewSet.as_view({'get': 'retrieve', 'put': 'update', 'delete': 'destroy'})),
    path('api/suppliers/', SupplierViewSet.as_view({'get': 'list', 'post': 'create'})),
    path('api/suppliers/<int:pk>/', SupplierViewSet.as_view({'get': 'retrieve', 'put': 'update', 'delete': 'destroy'})),
    path('api/', include('sales.urls')),
    path('api/payments/', include('payments.urls')),
    path('api/customers/', include('customers.urls')),
    path('api/customers/', CustomerViewSet.as_view({'get': 'list', 'post': 'create'})),
    path('api/customers/<int:pk>/', CustomerViewSet.as_view({'get': 'retrieve', 'put': 'update', 'delete': 'destroy'})),
    path('api/repairs/', RepairViewSet.as_view({'get': 'list', 'post': 'create'})),
    path('api/repairs/<int:pk>/', RepairViewSet.as_view({'get': 'retrieve', 'put': 'update', 'delete': 'destroy'})),
    path('api/repairs/<int:pk>/parts/', include('repairs.urls')),
    path('api/preorders/', PreorderViewSet.as_view({'get': 'list', 'post': 'create'})),
    path('api/preorders/<int:pk>/', PreorderViewSet.as_view({'get': 'retrieve', 'put': 'update', 'delete': 'destroy'})),
    path('api/supplier-prices/', SupplierPriceHistoryViewSet.as_view({'get': 'list', 'post': 'create'})),
    path('api/shifts/', include('shifts.urls')),
    path('api/chits/', include('chits.urls')),
    path('api/reports/', include('reports.urls')),
    path('api/branches/', BranchViewSet.as_view({'get': 'list', 'post': 'create'})),
    path('api/branches/<int:pk>/', BranchViewSet.as_view({'get': 'retrieve', 'put': 'update', 'delete': 'destroy'})),
    path('api/inventory/', include('inventory.urls')),
    path('api/', include('suppliers.urls')),
    path('api/integrations/', include('integrations.urls')),
    path('api/sync/', include('sync.urls')),
]
