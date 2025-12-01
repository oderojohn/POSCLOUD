from django.urls import path
from rest_framework.routers import DefaultRouter
from . import views

router = DefaultRouter()
router.register(r'categories', views.CategoryViewSet)
router.register(r'products', views.ProductViewSet)
router.register(r'batches', views.BatchViewSet)
router.register(r'stock-movements', views.StockMovementViewSet)
router.register(r'suppliers', views.SupplierViewSet)
router.register(r'purchases', views.PurchaseViewSet)
router.register(r'price-history', views.PriceHistoryViewSet)
router.register(r'sales-history', views.SalesHistoryViewSet)
router.register(r'product-history', views.ProductHistoryViewSet)
router.register(r'recalls', views.ProductRecallViewSet, basename='recalls')

urlpatterns = [
    path('products/low-stock/', views.LowStockView.as_view()),
    path('reports/stock/', views.StockReportView.as_view()),
    path('reports/purchases/', views.PurchaseReportView.as_view()),
    path('reports/supplier/', views.SupplierPerformanceView.as_view()),
    path('reports/valuation/', views.InventoryValuationView.as_view()),
    path('alerts/expiring/', views.ExpiringBatchesView.as_view()),
    path('alerts/expired/', views.ExpiredBatchesView.as_view()),
    path('analytics/profit/', views.ProfitAnalysisView.as_view()),
    path('products/<int:product_id>/timeline/', views.ProductTimelineView.as_view(), name='product-timeline'),
    path('reports/end-of-day-stock/', views.EndOfDayStockView.as_view(), name='end-of-day-stock'),
] + router.urls