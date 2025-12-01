# Generated manually to add batch_number and expiry_date fields to PurchaseOrderItem

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('suppliers', '0008_add_received_quantity_field'),
    ]

    operations = [
        migrations.AddField(
            model_name='purchaseorderitem',
            name='batch_number',
            field=models.CharField(blank=True, max_length=50),
        ),
        migrations.AddField(
            model_name='purchaseorderitem',
            name='expiry_date',
            field=models.DateField(blank=True, null=True),
        ),
    ]