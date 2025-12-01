from django.urls import path, include
from rest_framework.routers import DefaultRouter
from . import views

router = DefaultRouter()
router.register(r'users', views.UserProfileViewSet, basename='user')

urlpatterns = [
    path('', include(router.urls)),

    # Auth endpoints
    path('login/', views.LoginView.as_view(), name='login'),
    path('refresh/', views.RefreshView.as_view(), name='refresh'),
    path('logout/', views.LogoutView.as_view(), name='logout'),

    # Roles endpoint (give it a path, not '')
    path('roles/', views.RoleListView.as_view(), name='roles'),
]
