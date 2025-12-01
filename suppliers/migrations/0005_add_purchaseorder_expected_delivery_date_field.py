# Generated manually to add missing expected_delivery_date field to PurchaseOrder

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('suppliers', '0004_add_purchaseorder_order_number_field'),
    ]

    operations = [
        migrations.AddField(
            model_name='purchaseorder',
            name='expected_delivery_date',
            field=models.DateField(blank=True, null=True),
        ),
    ]