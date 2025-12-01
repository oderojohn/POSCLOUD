# Generated manually to add missing created_at and updated_at fields to PurchaseOrder

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('suppliers', '0006_add_purchaseorder_notes_field'),
    ]

    operations = [
        migrations.AddField(
            model_name='purchaseorder',
            name='created_at',
            field=models.DateTimeField(auto_now_add=True),
        ),
        migrations.AddField(
            model_name='purchaseorder',
            name='updated_at',
            field=models.DateTimeField(auto_now=True),
        ),
    ]