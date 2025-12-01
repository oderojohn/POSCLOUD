# Generated manually to add missing batch fields

import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('inventory', '0010_create_missing_models'),
        ('suppliers', '0003_remove_purchaseorder_expected_delivery_and_more'),
    ]

    operations = [
        migrations.AddField(
            model_name='batch',
            name='cost_price',
            field=models.DecimalField(blank=True, decimal_places=2, help_text='Cost price per unit for this batch', max_digits=10, null=True),
        ),
        migrations.AddField(
            model_name='batch',
            name='purchase_order_item',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name='batches', to='suppliers.purchaseorderitem'),
        ),
        migrations.AddField(
            model_name='batch',
            name='received_date',
            field=models.DateField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='batch',
            name='status',
            field=models.CharField(choices=[('ordered', 'Ordered'), ('received', 'Received'), ('expired', 'Expired')], default='ordered', max_length=20),
        ),
        migrations.AddField(
            model_name='saleshistory',
            name='batch',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, to='inventory.batch'),
        ),
        migrations.AddField(
            model_name='saleshistory',
            name='cost_price',
            field=models.DecimalField(blank=True, decimal_places=2, max_digits=10, null=True),
        ),
        migrations.AddField(
            model_name='saleshistory',
            name='profit',
            field=models.DecimalField(blank=True, decimal_places=2, max_digits=10, null=True),
        ),
    ]