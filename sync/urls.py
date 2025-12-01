from django.urls import path
from django.conf import settings
from . import views

app_name = 'sync'

# All systems can perform sync operations (bidirectional)
urlpatterns = [
    path('status/', views.sync_status, name='sync_status'),
    path('logs/', views.sync_logs, name='sync_logs'),
    path('sync-now/', views.sync_now, name='sync_now'),
    path('queue/', views.sync_queue, name='sync_queue'),
    path('resolve-conflict/<int:conflict_id>/', views.resolve_conflict, name='resolve_conflict'),
    path('clear-queue/', views.clear_sync_queue, name='clear_sync_queue'),
    # Cloud systems can receive uploads and serve downloads
    path('upload/', views.sync_upload, name='sync_upload'),
    path('download/', views.sync_download, name='sync_download'),
]