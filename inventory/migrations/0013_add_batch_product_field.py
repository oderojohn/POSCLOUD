# Generated manually to add missing product field to Batch

import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('inventory', '0012_merge_20251025_1754'),
    ]

    operations = [
        migrations.AddField(
            model_name='batch',
            name='product',
            field=models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='inventory.product'),
        ),
    ]