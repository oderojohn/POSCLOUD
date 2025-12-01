# Generated manually to add missing notes field to PurchaseOrder

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('suppliers', '0005_add_purchaseorder_expected_delivery_date_field'),
    ]

    operations = [
        migrations.AddField(
            model_name='purchaseorder',
            name='notes',
            field=models.TextField(blank=True),
        ),
    ]