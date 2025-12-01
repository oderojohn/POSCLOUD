# Generated manually to add unique constraint to order_number field

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('suppliers', '0010_populate_order_numbers'),
    ]

    operations = [
        migrations.AlterField(
            model_name='purchaseorder',
            name='order_number',
            field=models.CharField(blank=True, max_length=50, unique=True),
        ),
    ]