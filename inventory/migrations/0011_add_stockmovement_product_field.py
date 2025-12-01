# Generated manually to add missing product field to StockMovement

import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('inventory', '0010_create_missing_models'),
        ('users', '0002_alter_userprofile_role'),
    ]

    operations = [
        migrations.AddField(
            model_name='stockmovement',
            name='product',
            field=models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='inventory.product'),
        ),
        migrations.AddField(
            model_name='stockmovement',
            name='user',
            field=models.ForeignKey(null=True, on_delete=django.db.models.deletion.SET_NULL, to='users.userprofile'),
        ),
    ]