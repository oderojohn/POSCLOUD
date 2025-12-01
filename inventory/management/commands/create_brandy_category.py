from django.core.management.base import BaseCommand
from inventory.models import Category

class Command(BaseCommand):
    help = 'Create Brandy category'

    def handle(self, *args, **options):
        category, created = Category.objects.get_or_create(
            name='Brandy',
            defaults={
                'description': 'Brandy spirits and liquors'
            }
        )

        if created:
            self.stdout.write(self.style.SUCCESS('Successfully created Brandy category'))
        else:
            self.stdout.write(self.style.WARNING('Brandy category already exists'))