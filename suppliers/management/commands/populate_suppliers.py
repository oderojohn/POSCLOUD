from django.core.management.base import BaseCommand
from suppliers.models import Supplier

class Command(BaseCommand):
    help = 'Populate sample suppliers for testing'

    def handle(self, *args, **options):
        suppliers_data = [
            {
                'name': 'TechGear Inc',
                'contact_person': 'John Smith',
                'email': 'john@techgear.com',
                'phone': '+254712345678',
                'address': '123 Technology Street, Nairobi'
            },
            {
                'name': 'OfficeWorld',
                'contact_person': 'Sarah Johnson',
                'email': 'sarah@officeworld.com',
                'phone': '+254787654321',
                'address': '456 Business Avenue, Nairobi'
            },
            {
                'name': 'SupplyChain Co',
                'contact_person': 'Mike Brown',
                'email': 'mike@supplychain.com',
                'phone': '+254723456789',
                'address': '789 Logistics Road, Nairobi'
            },
            {
                'name': 'Global Parts',
                'contact_person': 'Lisa Wong',
                'email': 'lisa@globalparts.com',
                'phone': '+254798765432',
                'address': '321 Industrial Area, Nairobi'
            }
        ]

        for supplier_data in suppliers_data:
            supplier, created = Supplier.objects.get_or_create(
                name=supplier_data['name'],
                defaults=supplier_data
            )
            if created:
                self.stdout.write(
                    self.style.SUCCESS(f'Successfully created supplier "{supplier.name}"')
                )
            else:
                self.stdout.write(
                    self.style.WARNING(f'Supplier "{supplier.name}" already exists')
                )

        self.stdout.write(
            self.style.SUCCESS(f'Successfully populated {len(suppliers_data)} suppliers')
        )