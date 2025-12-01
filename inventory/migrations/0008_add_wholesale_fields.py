# Generated manually to add missing wholesale fields

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('inventory', '0007_add_category_field'),
    ]

    operations = [
        migrations.AddField(
            model_name='product',
            name='wholesale_min_qty',
            field=models.PositiveIntegerField(default=10),
        ),
        migrations.AddField(
            model_name='product',
            name='wholesale_price',
            field=models.DecimalField(blank=True, decimal_places=2, max_digits=10, null=True),
        ),
    ]