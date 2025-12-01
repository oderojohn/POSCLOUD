# Generated manually to add received_quantity field to PurchaseOrderItem

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('suppliers', '0007_add_purchaseorder_created_at_updated_at_fields'),
    ]

    operations = [
        migrations.AddField(
            model_name='purchaseorderitem',
            name='received_quantity',
            field=models.PositiveIntegerField(default=0),
        ),
    ]