import uuid
from django.db import models
from django.utils import timezone


class SyncModel(models.Model):
    """
    Abstract base model that adds sync capabilities to all models.
    """
    sync_id = models.UUIDField(default=uuid.uuid4, unique=True, editable=False)
    sync_version = models.PositiveIntegerField(default=1, editable=False)
    last_modified = models.DateTimeField(auto_now=True)
    sync_status = models.CharField(
        max_length=20,
        choices=[
            ('local', 'Local Only'),
            ('pending', 'Pending Sync'),
            ('synced', 'Synced'),
            ('conflict', 'Conflict'),
            ('deleted', 'Marked for Deletion'),
        ],
        default='local',
        editable=False
    )
    deleted_at = models.DateTimeField(null=True, blank=True, editable=False)

    class Meta:
        abstract = True

    def save(self, *args, **kwargs):
        # Increment version on save
        if self.pk:  # Only increment if this is an update
            self.sync_version += 1

        # Mark as pending sync if not local-only
        if hasattr(self, '_sync_enabled') and self._sync_enabled:
            if self.sync_status == 'synced':
                self.sync_status = 'pending'

        super().save(*args, **kwargs)

        # Queue for sync if sync is enabled
        if hasattr(self, '_sync_enabled') and self._sync_enabled and self.sync_status == 'pending':
            SyncQueue.objects.get_or_create(
                model_name=self._meta.label_lower,
                object_id=self.pk,
                defaults={
                    'operation': 'update',
                    'data': self.get_sync_data(),
                    'sync_version': self.sync_version
                }
            )

    def delete(self, *args, **kwargs):
        if hasattr(self, '_sync_enabled') and self._sync_enabled:
            # Soft delete for sync
            self.sync_status = 'deleted'
            self.deleted_at = timezone.now()
            self.save()
        else:
            super().delete(*args, **kwargs)

    def get_sync_data(self):
        """
        Get model data for sync. Override in subclasses if needed.
        """
        data = {}
        for field in self._meta.fields:
            if not field.primary_key and field.name not in ['sync_id', 'sync_version', 'last_modified', 'sync_status', 'deleted_at']:
                value = getattr(self, field.name)
                if isinstance(value, models.Model):
                    data[field.name] = str(value.pk)
                else:
                    data[field.name] = value
        return data


class SyncQueue(models.Model):
    """
    Queue for tracking changes that need to be synced to cloud.
    """
    model_name = models.CharField(max_length=100)  # e.g., 'inventory.product'
    object_id = models.PositiveIntegerField()
    operation = models.CharField(
        max_length=10,
        choices=[
            ('create', 'Create'),
            ('update', 'Update'),
            ('delete', 'Delete'),
        ]
    )
    data = models.JSONField()  # Serialized model data
    sync_version = models.PositiveIntegerField()
    created_at = models.DateTimeField(auto_now_add=True)
    processed_at = models.DateTimeField(null=True, blank=True)
    retry_count = models.PositiveIntegerField(default=0)
    error_message = models.TextField(blank=True)

    class Meta:
        unique_together = ['model_name', 'object_id', 'sync_version']
        ordering = ['created_at']

    def __str__(self):
        return f"{self.operation} {self.model_name} {self.object_id}"


class SyncLog(models.Model):
    """
    Log of sync operations for debugging and monitoring.
    """
    timestamp = models.DateTimeField(auto_now_add=True)
    operation = models.CharField(
        max_length=20,
        choices=[
            ('upload', 'Upload to Cloud'),
            ('download', 'Download from Cloud'),
            ('conflict', 'Conflict Resolution'),
            ('error', 'Error'),
        ]
    )
    model_name = models.CharField(max_length=100, blank=True)
    object_id = models.PositiveIntegerField(null=True, blank=True)
    status = models.CharField(
        max_length=20,
        choices=[
            ('success', 'Success'),
            ('error', 'Error'),
            ('partial', 'Partial Success'),
        ]
    )
    message = models.TextField()
    details = models.JSONField(null=True, blank=True)

    class Meta:
        ordering = ['-timestamp']

    def __str__(self):
        return f"{self.timestamp} {self.operation} {self.status}"


class SyncState(models.Model):
    """
    Track sync state between local and cloud.
    """
    key = models.CharField(max_length=100, unique=True)
    last_sync_timestamp = models.DateTimeField(null=True, blank=True)
    last_sync_version = models.PositiveIntegerField(default=0)
    cloud_last_sync = models.DateTimeField(null=True, blank=True)

    class Meta:
        verbose_name = 'Sync State'
        verbose_name_plural = 'Sync States'

    def __str__(self):
        return f"{self.key}: {self.last_sync_timestamp}"