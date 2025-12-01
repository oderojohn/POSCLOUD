"""
Management command to initialize Branch A from existing live data.

This command ensures that all existing data becomes associated with Branch A,
making the current live database become Branch A for branch isolation.
"""

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from branches.models import Branch


class Command(BaseCommand):
    help = 'Initialize Branch A from existing live data'

    def handle(self, *args, **options):
        """
        Initialize Branch A by creating it and assigning all existing data to it.
        """
        self.stdout.write('🚀 Starting Branch A initialization...')

        try:
            with transaction.atomic():
                # Create Branch A if it doesn't exist
                branch_a, created = Branch.objects.get_or_create(
                    name='Branch A',
                    defaults={
                        'location': 'Main Branch',
                        'address': 'Current live database location',
                        'phone': '+254700000000',
                        'is_active': True,
                    }
                )

                if created:
                    self.stdout.write(
                        self.style.SUCCESS(f'✅ Created Branch A (ID: {branch_a.id})')
                    )
                else:
                    self.stdout.write(
                        self.style.WARNING(f'⚠️  Branch A already exists (ID: {branch_a.id})')
                    )

                # Count records for each model and associate with Branch A
                self.stdout.write('\n📊 Associating existing data with Branch A...')

                # Import models dynamically to avoid import issues
                from django.apps import apps
                
                # List of models that should be associated with Branch A
                models_to_update = [
                    ('inventory', 'Product'),
                    ('inventory', 'Category'),
                    ('inventory', 'Batch'),
                    ('inventory', 'StockMovement'),
                    ('inventory', 'SalesHistory'),
                    ('inventory', 'PriceHistory'),
                    ('inventory', 'Purchase'),
                    ('sales', 'Cart'),
                    ('sales', 'Sale'),
                    ('sales', 'Return'),
                    ('sales', 'Invoice'),
                    ('customers', 'Customer'),
                    ('customers', 'LoyaltyTransaction'),
                    ('payments', 'Payment'),
                    ('payments', 'InstallmentPlan'),
                    ('suppliers', 'Supplier'),
                    ('suppliers', 'PurchaseOrder'),
                    ('suppliers', 'PurchaseOrderItem'),
                ]

                total_updated = 0

                for app_label, model_name in models_to_update:
                    try:
                        model = apps.get_model(app_label, model_name)
                        
                        # Only update records that don't have a branch assigned yet
                        branch_field = getattr(model, 'branch', None)
                        if branch_field:
                            # Get the field name
                            field_name = branch_field.field.name
                            
                            # Update records without branch assignment
                            updated_count = model.objects.filter(
                                **{f'{field_name}__isnull': True}
                            ).update(**{field_name: branch_a})
                            
                            if updated_count > 0:
                                total_updated += updated_count
                                self.stdout.write(
                                    f'  • {model_name}: {updated_count} records updated'
                                )
                            else:
                                self.stdout.write(
                                    f'  • {model_name}: No records to update'
                                )
                        else:
                            self.stdout.write(
                                f'  • {model_name}: No branch field found, skipping'
                            )

                    except Exception as e:
                        self.stdout.write(
                            self.style.ERROR(f'  • {model_name}: Error - {str(e)}')
                        )

                # Update user profiles that don't have a branch
                from users.models import UserProfile
                user_profiles_updated = UserProfile.objects.filter(
                    branch__isnull=True
                ).update(branch=branch_a)
                
                if user_profiles_updated > 0:
                    total_updated += user_profiles_updated
                    self.stdout.write(
                        f'  • UserProfile: {user_profiles_updated} records updated'
                    )

                self.stdout.write(
                    self.style.SUCCESS(f'\n✅ Branch A initialization completed!')
                )
                self.stdout.write(
                    f'📈 Total records updated: {total_updated}'
                )
                self.stdout.write(
                    f'🏢 Branch A ID: {branch_a.id}'
                )
                self.stdout.write(
                    f'📍 Branch A Name: {branch_a.name}'
                )

                # Provide next steps
                self.stdout.write('\n🎯 Next steps:')
                self.stdout.write('1. Run: python manage.py makemigrations')
                self.stdout.write('2. Run: python manage.py migrate')
                self.stdout.write('3. The branch isolation system is ready to use!')

        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f'❌ Error during initialization: {str(e)}')
            )
            raise CommandError(f'Branch A initialization failed: {str(e)}')