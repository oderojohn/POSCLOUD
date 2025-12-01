# Generated manually to add missing order_number field to PurchaseOrder

from django.db import migrations, models
from django.utils import timezone
import uuid


def populate_order_numbers(apps, schema_editor):
    PurchaseOrder = apps.get_model('suppliers', 'PurchaseOrder')
    # Use raw SQL to avoid field issues during migration
    from django.db import connection
    with connection.cursor() as cursor:
        cursor.execute("""
            UPDATE suppliers_purchaseorder
            SET order_number = CONCAT('PO', TO_CHAR(NOW(), 'YYYYMMDDHH24MISS'), UPPER(SUBSTRING(MD5(RANDOM()::text) FROM 1 FOR 4)))
            WHERE order_number IS NULL OR order_number = ''
        """)


class Migration(migrations.Migration):

    dependencies = [
        ('suppliers', '0003_remove_purchaseorder_expected_delivery_and_more'),
    ]

    operations = [
        migrations.AddField(
            model_name='purchaseorder',
            name='order_number',
            field=models.CharField(blank=True, max_length=50),
        ),
        migrations.RunPython(populate_order_numbers),
    ]