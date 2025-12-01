import json
import logging
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
from django.utils import timezone
from django.conf import settings
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework import status

from .models import SyncQueue, SyncLog, SyncState
from django.conf import settings

logger = logging.getLogger(__name__)


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def sync_status(request):
    """
    Get current sync status and statistics.
    """
    try:
        pending_count = SyncQueue.objects.filter(processed_at__isnull=True).count()
        last_sync = SyncState.objects.filter(key='last_sync').first()

        return Response({
                'sync_enabled': settings.SYNC_SETTINGS['ENABLED'],
                'pending_changes': pending_count,
                'last_sync': last_sync.last_sync_timestamp if last_sync else None,
                'environment': getattr(settings, 'ENVIRONMENT', 'development'),
                'system_state': getattr(settings, 'SYSTEM_STATE', 'local'),
                'is_cloud': settings.SYNC_SETTINGS.get('IS_CLOUD', False),
            })
    except Exception as e:
        logger.error(f"Error getting sync status: {e}")
        return Response({'error': 'Failed to get sync status'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(['POST'])
@permission_classes([IsAuthenticated])
def sync_now(request):
    """
    Trigger immediate sync operation (All systems can sync).
    """
    if not settings.SYNC_SETTINGS['ENABLED']:
        return Response({'error': 'Sync is not enabled'}, status=status.HTTP_400_BAD_REQUEST)

    try:
        # Import here to avoid circular imports
        from .services import SyncService

        sync_service = SyncService()
        result = sync_service.perform_sync()

        return Response({
            'success': True,
            'uploaded': result.get('uploaded', 0),
            'downloaded': result.get('downloaded', 0),
            'conflicts': result.get('conflicts', 0),
            'errors': result.get('errors', []),
        })

    except Exception as e:
        logger.error(f"Error during sync: {e}")
        SyncLog.objects.create(
            operation='error',
            status='error',
            message=f'Sync failed: {str(e)}'
        )
        return Response({'error': 'Sync failed'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def sync_queue(request):
    """
    Get current sync queue items.
    """
    try:
        queue_items = SyncQueue.objects.filter(
            processed_at__isnull=True
        ).order_by('created_at')[:50]  # Limit to 50 items

        data = []
        for item in queue_items:
            data.append({
                'id': item.id,
                'model_name': item.model_name,
                'object_id': item.object_id,
                'operation': item.operation,
                'created_at': item.created_at,
                'retry_count': item.retry_count,
            })

        return Response({'queue': data})

    except Exception as e:
        logger.error(f"Error getting sync queue: {e}")
        return Response({'error': 'Failed to get sync queue'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(['POST'])
@permission_classes([IsAuthenticated])
def resolve_conflict(request, conflict_id):
    """
    Resolve a sync conflict manually.
    """
    try:
        resolution = request.data.get('resolution')  # 'local', 'remote', 'merge'
        if resolution not in ['local', 'remote', 'merge']:
            return Response({'error': 'Invalid resolution'}, status=status.HTTP_400_BAD_REQUEST)

        # This would need to be implemented based on conflict storage
        # For now, just log it
        SyncLog.objects.create(
            operation='conflict',
            status='success',
            message=f'Conflict {conflict_id} resolved with {resolution}',
            details={'conflict_id': conflict_id, 'resolution': resolution}
        )

        return Response({'success': True})

    except Exception as e:
        logger.error(f"Error resolving conflict: {e}")
        return Response({'error': 'Failed to resolve conflict'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(['DELETE'])
@permission_classes([IsAuthenticated])
def clear_sync_queue(request):
    """
    Clear all pending sync queue items (use with caution).
    """
    try:
        deleted_count = SyncQueue.objects.filter(processed_at__isnull=True).delete()[0]

        SyncLog.objects.create(
            operation='upload',
            status='success',
            message=f'Cleared {deleted_count} items from sync queue'
        )

        return Response({'success': True, 'cleared_count': deleted_count})

    except Exception as e:
        logger.error(f"Error clearing sync queue: {e}")
        return Response({'error': 'Failed to clear sync queue'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def sync_logs(request):
    """
    Get recent sync logs for debugging.
    """
    try:
        limit = int(request.query_params.get('limit', 50))
        logs = SyncLog.objects.order_by('-timestamp')[:limit]

        data = []
        for log in logs:
            data.append({
                'id': log.id,
                'timestamp': log.timestamp,
                'operation': log.operation,
                'model_name': log.model_name,
                'object_id': log.object_id,
                'status': log.status,
                'message': log.message,
                'details': log.details,
            })

        return Response({'logs': data})

    except Exception as e:
        logger.error(f"Error getting sync logs: {e}")
        return Response({'error': 'Failed to get sync logs'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


# Cloud sync endpoints (only available on cloud systems)
@api_view(['POST'])
@permission_classes([IsAuthenticated])
def sync_upload(request):
    """
    Receive sync data from local systems (Cloud only).
    """
    if not settings.SYNC_SETTINGS.get('IS_CLOUD', False):
        return Response({'error': 'This endpoint is only available on cloud systems'}, status=status.HTTP_403_FORBIDDEN)

    try:
        changes = request.data.get('changes', {})
        processed = 0
        conflicts = 0

        # Process each operation type
        for operation, change_list in changes.items():
            for change in change_list:
                try:
                    _apply_cloud_change(change, operation)
                    processed += 1
                except Exception as e:
                    logger.error(f"Failed to apply {operation} change: {e}")
                    conflicts += 1

        SyncLog.objects.create(
            operation='upload',
            status='success',
            message=f'Processed {processed} changes, {conflicts} conflicts'
        )

        return Response({
            'processed': processed,
            'conflicts': conflicts
        })

    except Exception as e:
        logger.error(f"Error in sync upload: {e}")
        return Response({'error': 'Failed to process sync upload'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def sync_download(request):
    """
    Send sync data to local systems (Cloud only).
    """
    if not settings.SYNC_SETTINGS.get('IS_CLOUD', False):
        return Response({'error': 'This endpoint is only available on cloud systems'}, status=status.HTTP_403_FORBIDDEN)

    try:
        since = request.query_params.get('since')
        changes = _get_changes_since(since)

        return Response({'changes': changes})

    except Exception as e:
        logger.error(f"Error in sync download: {e}")
        return Response({'error': 'Failed to get sync changes'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


def _apply_cloud_change(change, operation):
    """
    Apply a change from local system to cloud database.
    """
    from django.apps import apps

    model_name = change['model']
    object_id = change['id']
    data = change['data']
    version = change.get('version', 1)

    app_label, model_short_name = model_name.split('.')
    model_class = apps.get_model(app_label, model_short_name)

    if operation == 'create':
        if not model_class.objects.filter(pk=object_id).exists():
            obj = model_class(**data)
            obj.pk = object_id
            obj.sync_status = 'synced'
            obj.sync_version = version
            obj.save()

    elif operation == 'update':
        obj, created = model_class.objects.get_or_create(pk=object_id, defaults=data)
        if not created:
            for key, value in data.items():
                if hasattr(obj, key):
                    setattr(obj, key, value)
        obj.sync_status = 'synced'
        obj.sync_version = version
        obj.save()

    elif operation == 'delete':
        try:
            obj = model_class.objects.get(pk=object_id)
            obj.delete()
        except model_class.DoesNotExist:
            pass


def _get_changes_since(since_timestamp):
    """
    Get all changes since the given timestamp.
    """
    from django.apps import apps
    from django.db.models import Q

    changes = []
    if since_timestamp:
        since = timezone.datetime.fromisoformat(since_timestamp.replace('Z', '+00:00'))
    else:
        since = None

    # Get all models that have sync capabilities
    sync_models = []
    for app_config in apps.get_app_configs():
        for model in app_config.get_models():
            if hasattr(model, '_sync_enabled') and model._sync_enabled:
                sync_models.append(model)

    for model in sync_models:
        queryset = model.objects.all()
        if since:
            queryset = queryset.filter(last_modified__gt=since)

        for obj in queryset:
            changes.append({
                'model': f"{model._meta.app_label}.{model._meta.model_name}",
                'id': obj.pk,
                'operation': 'update',  # For now, send all as updates
                'data': obj.get_sync_data(),
                'version': obj.sync_version,
            })

    return changes