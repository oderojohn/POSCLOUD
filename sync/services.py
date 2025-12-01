import json
import logging
import requests
from django.conf import settings
from django.utils import timezone
from django.apps import apps
from .models import SyncQueue, SyncLog, SyncState

logger = logging.getLogger(__name__)


class SyncService:
    """
    Service for handling bidirectional synchronization between local and cloud databases.
    """

    def __init__(self):
        self.is_cloud = settings.SYNC_SETTINGS.get('IS_CLOUD', False)
        self.cloud_url = settings.SYNC_SETTINGS.get('CLOUD_URL', '')
        self.api_key = settings.SYNC_SETTINGS.get('API_KEY', '')
        self.batch_size = settings.SYNC_SETTINGS.get('BATCH_SIZE', 100)
        self.conflict_resolution = settings.SYNC_SETTINGS.get('CONFLICT_RESOLUTION', 'server_wins')
        self.peer_urls = settings.SYNC_SETTINGS.get('PEER_URLS', [])  # For cloud-to-cloud sync

    def perform_sync(self):
        """
        Perform complete bidirectional sync operation.
        """
        results = {
            'uploaded': 0,
            'downloaded': 0,
            'conflicts': 0,
            'errors': []
        }

        try:
            # For cloud systems: sync with other clouds or central systems
            if self.is_cloud:
                # Cloud-to-cloud sync (if configured)
                if hasattr(self, 'peer_urls') and self.peer_urls:
                    for peer_url in self.peer_urls:
                        peer_result = self._sync_with_peer(peer_url)
                        results['uploaded'] += peer_result.get('uploaded', 0)
                        results['downloaded'] += peer_result.get('downloaded', 0)
                        results['conflicts'] += peer_result.get('conflicts', 0)
                        results['errors'].extend(peer_result.get('errors', []))
            else:
                # Local-to-cloud sync
                # Upload local changes first
                upload_result = self._upload_changes()
                results.update(upload_result)

                # Then download cloud changes
                download_result = self._download_changes()
                results['downloaded'] = download_result.get('downloaded', 0)
                results['conflicts'] = download_result.get('conflicts', 0)
                results['errors'].extend(download_result.get('errors', []))

            # Update sync state
            SyncState.objects.update_or_create(
                key='last_sync',
                defaults={'last_sync_timestamp': timezone.now()}
            )

            SyncLog.objects.create(
                operation='sync',
                status='success',
                message=f'Sync completed: {results["uploaded"]} uploaded, {results["downloaded"]} downloaded, {results["conflicts"]} conflicts'
            )

        except Exception as e:
            logger.error(f"Sync failed: {e}")
            results['errors'].append(str(e))
            SyncLog.objects.create(
                operation='error',
                status='error',
                message=f'Sync failed: {str(e)}'
            )

        return results

    def _sync_with_peer(self, peer_url):
        """
        Sync with another cloud system (peer-to-peer).
        """
        results = {'uploaded': 0, 'downloaded': 0, 'conflicts': 0, 'errors': []}

        try:
            # Upload to peer
            upload_result = self._upload_to_peer(peer_url)
            results.update(upload_result)

            # Download from peer
            download_result = self._download_from_peer(peer_url)
            results['downloaded'] = download_result.get('downloaded', 0)
            results['conflicts'] = download_result.get('conflicts', 0)
            results['errors'].extend(download_result.get('errors', []))

        except Exception as e:
            logger.error(f"Peer sync failed with {peer_url}: {e}")
            results['errors'].append(str(e))

        return results

    def _upload_to_peer(self, peer_url):
        """Upload changes to a peer cloud system."""
        # Similar to _upload_changes but to peer URL
        results = {'uploaded': 0}

        pending_changes = SyncQueue.objects.filter(
            processed_at__isnull=True
        ).order_by('created_at')[:self.batch_size]

        if not pending_changes:
            return results

        changes_by_operation = {
            'create': [], 'update': [], 'delete': []
        }

        for change in pending_changes:
            changes_by_operation[change.operation].append({
                'model': change.model_name,
                'id': change.object_id,
                'data': change.data,
                'version': change.sync_version
            })

        try:
            response = requests.post(
                f"{peer_url}/api/sync/upload/",
                json={'changes': changes_by_operation},
                headers={
                    'Authorization': f'Bearer {self.api_key}',
                    'Content-Type': 'application/json'
                },
                timeout=30
            )

            if response.status_code == 200:
                result = response.json()
                results['uploaded'] = result.get('processed', 0)

                # Mark processed
                for change in pending_changes:
                    change.processed_at = timezone.now()
                    change.save()

        except Exception as e:
            logger.error(f"Upload to peer {peer_url} error: {e}")
            for change in pending_changes:
                change.retry_count += 1
                change.error_message = str(e)
                change.save()

        return results

    def _download_from_peer(self, peer_url):
        """Download changes from a peer cloud system."""
        results = {'downloaded': 0, 'conflicts': 0, 'errors': []}

        last_sync = SyncState.objects.filter(key='last_sync').first()
        since = last_sync.last_sync_timestamp.isoformat() if last_sync else None

        try:
            params = {}
            if since:
                params['since'] = since

            response = requests.get(
                f"{peer_url}/api/sync/download/",
                params=params,
                headers={'Authorization': f'Bearer {self.api_key}'},
                timeout=30
            )

            if response.status_code == 200:
                data = response.json()
                changes = data.get('changes', [])

                for change in changes:
                    try:
                        self._apply_cloud_change(change, change.get('operation', 'update'))
                        results['downloaded'] += 1
                    except Exception as e:
                        logger.error(f"Failed to apply change from peer: {e}")
                        results['errors'].append(f"Failed to apply change: {e}")

        except Exception as e:
            logger.error(f"Download from peer {peer_url} error: {e}")
            results['errors'].append(str(e))

        return results

    def _upload_changes(self):
        """
        Upload local changes to cloud.
        """
        results = {'uploaded': 0, 'conflicts': 0}

        # Get pending changes
        pending_changes = SyncQueue.objects.filter(
            processed_at__isnull=True
        ).order_by('created_at')[:self.batch_size]

        if not pending_changes:
            return results

        # Group changes by operation
        changes_by_operation = {
            'create': [],
            'update': [],
            'delete': []
        }

        for change in pending_changes:
            changes_by_operation[change.operation].append({
                'model': change.model_name,
                'id': change.object_id,
                'data': change.data,
                'version': change.sync_version
            })

        # Send to cloud
        try:
            response = requests.post(
                f"{self.cloud_url}/api/sync/upload/",
                json={'changes': changes_by_operation},
                headers={
                    'Authorization': f'Bearer {self.api_key}',
                    'Content-Type': 'application/json'
                },
                timeout=30
            )

            if response.status_code == 200:
                result = response.json()
                results['uploaded'] = result.get('processed', 0)
                results['conflicts'] = result.get('conflicts', 0)

                # Mark processed items
                for change in pending_changes:
                    change.processed_at = timezone.now()
                    change.save()

            else:
                raise Exception(f"Upload failed: {response.status_code} {response.text}")

        except Exception as e:
            logger.error(f"Upload error: {e}")
            # Mark items for retry
            for change in pending_changes:
                change.retry_count += 1
                change.error_message = str(e)
                change.save()

        return results

    def _download_changes(self):
        """
        Download changes from cloud.
        """
        results = {'downloaded': 0}

        # Get last sync timestamp
        last_sync = SyncState.objects.filter(key='last_sync').first()
        since_timestamp = last_sync.last_sync_timestamp.isoformat() if last_sync else None

        try:
            params = {}
            if since_timestamp:
                params['since'] = since_timestamp

            response = requests.get(
                f"{self.cloud_url}/api/sync/download/",
                params=params,
                headers={'Authorization': f'Bearer {self.api_key}'},
                timeout=30
            )

            if response.status_code == 200:
                data = response.json()
                changes = data.get('changes', [])

                for change in changes:
                    try:
                        self._apply_cloud_change(change)
                        results['downloaded'] += 1
                    except Exception as e:
                        logger.error(f"Failed to apply change {change}: {e}")
                        results['errors'].append(f"Failed to apply change: {e}")

            else:
                raise Exception(f"Download failed: {response.status_code} {response.text}")

        except Exception as e:
            logger.error(f"Download error: {e}")
            results['errors'].append(str(e))

        return results

    def _apply_cloud_change(self, change):
        """
        Apply a single change from cloud to local database.
        """
        model_name = change['model']
        object_id = change['id']
        operation = change['operation']
        data = change['data']
        cloud_version = change.get('version', 1)

        # Get the model class
        app_label, model_name_short = model_name.split('.')
        model_class = apps.get_model(app_label, model_name_short)

        if operation == 'create':
            # Check if object already exists
            if not model_class.objects.filter(pk=object_id).exists():
                # Create new object
                obj = model_class(**data)
                obj.sync_status = 'synced'
                obj.sync_version = cloud_version
                obj.save()

        elif operation == 'update':
            try:
                obj = model_class.objects.get(pk=object_id)
                local_version = getattr(obj, 'sync_version', 0)

                if cloud_version > local_version:
                    # Apply update
                    for key, value in data.items():
                        if hasattr(obj, key):
                            setattr(obj, key, value)
                    obj.sync_status = 'synced'
                    obj.sync_version = cloud_version
                    obj.save()
                elif cloud_version < local_version:
                    # Local is newer, this is a conflict
                    self._handle_conflict(obj, data, cloud_version)

            except model_class.DoesNotExist:
                # Object doesn't exist locally, create it
                obj = model_class(**data)
                obj.pk = object_id
                obj.sync_status = 'synced'
                obj.sync_version = cloud_version
                obj.save()

        elif operation == 'delete':
            try:
                obj = model_class.objects.get(pk=object_id)
                obj.delete()  # This will do soft delete if sync-enabled
            except model_class.DoesNotExist:
                pass  # Already deleted

    def _handle_conflict(self, local_obj, cloud_data, cloud_version):
        """
        Handle sync conflicts based on configured resolution strategy.
        """
        if self.conflict_resolution == 'server_wins':
            # Apply cloud changes
            for key, value in cloud_data.items():
                if hasattr(local_obj, key):
                    setattr(local_obj, key, value)
            local_obj.sync_version = cloud_version
            local_obj.sync_status = 'synced'
            local_obj.save()

        elif self.conflict_resolution == 'client_wins':
            # Keep local changes, mark as pending re-sync
            local_obj.sync_status = 'pending'
            local_obj.save()

        else:  # manual resolution
            # Mark as conflict for manual resolution
            local_obj.sync_status = 'conflict'
            local_obj.save()

            # Log conflict details
            SyncLog.objects.create(
                operation='conflict',
                status='error',
                model_name=local_obj._meta.label_lower,
                object_id=local_obj.pk,
                message='Sync conflict detected',
                details={
                    'local_version': local_obj.sync_version,
                    'cloud_version': cloud_version,
                    'cloud_data': cloud_data
                }
            )

    def get_sync_status(self):
        """
        Get current sync status information.
        """
        pending_count = SyncQueue.objects.filter(processed_at__isnull=True).count()
        last_sync = SyncState.objects.filter(key='last_sync').first()

        return {
            'enabled': settings.SYNC_SETTINGS['ENABLED'],
            'pending_changes': pending_count,
            'last_sync': last_sync.last_sync_timestamp if last_sync else None,
            'cloud_url': self.cloud_url,
        }