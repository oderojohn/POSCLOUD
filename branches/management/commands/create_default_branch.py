from django.core.management.base import BaseCommand
from branches.models import Branch

class Command(BaseCommand):
    help = 'Create a default branch if none exists'

    def handle(self, *args, **options):
        if Branch.objects.filter(is_active=True).exists():
            self.stdout.write(self.style.SUCCESS('Active branch already exists'))
            return

        branch = Branch.objects.create(
            name='Main Branch',
            location='Main Location',
            address='Default Address',
            phone='0000000000',
            is_active=True
        )
        self.stdout.write(self.style.SUCCESS(f'Created default branch: {branch.name}'))