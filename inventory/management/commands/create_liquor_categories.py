from django.core.management.base import BaseCommand
from inventory.models import Category

class Command(BaseCommand):
    help = 'Create Brandy and Spirits categories'

    def handle(self, *args, **options):
        # Create Brandy category
        brandy_category, brandy_created = Category.objects.get_or_create(
            name='Brandy',
            defaults={
                'description': 'Brandy spirits and liquors'
            }
        )

        # Create Spirits category
        spirits_category, spirits_created = Category.objects.get_or_create(
            name='Spirits',
            defaults={
                'description': 'Various spirits and liquors'
            }
        )

        if brandy_created:
            self.stdout.write(self.style.SUCCESS('Successfully created Brandy category'))
        else:
            self.stdout.write(self.style.WARNING('Brandy category already exists'))

        if spirits_created:
            self.stdout.write(self.style.SUCCESS('Successfully created Spirits category'))
        else:
            self.stdout.write(self.style.WARNING('Spirits category already exists'))