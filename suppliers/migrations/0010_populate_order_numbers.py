# Generated manually to populate order_numbers for existing PurchaseOrder records

from django.db import migrations
from django.utils import timezone
import uuid


def populate_order_numbers(apps, schema_editor):
    PurchaseOrder = apps.get_model('suppliers', 'PurchaseOrder')
    for order in PurchaseOrder.objects.filter(order_number=''):
        # Generate unique order number using timestamp and random component
        timestamp = timezone.now().strftime('%Y%m%d%H%M%S')
        unique_id = str(uuid.uuid4())[:4].upper()
        order.order_number = f"PO{timestamp}{unique_id}"
        order.save()


class Migration(migrations.Migration):

    dependencies = [
        ('suppliers', '0009_add_batch_number_and_expiry_date_fields'),
    ]

    operations = [
        migrations.RunPython(populate_order_numbers),
    ]