from django.core.management.base import BaseCommand
from django.contrib.auth.models import User
from users.models import UserProfile


class Command(BaseCommand):
    help = 'Create UserProfile instances for users that do not have one'

    def handle(self, *args, **options):
        users_without_profile = User.objects.filter(userprofile__isnull=True)
        
        if not users_without_profile.exists():
            self.stdout.write(self.style.SUCCESS('All users already have UserProfiles.'))
            return
        
        created_count = 0
        for user in users_without_profile:
            try:
                UserProfile.objects.get_or_create(
                    user=user,
                    defaults={'role': 'cashier'}
                )
                created_count += 1
                self.stdout.write(f'Created UserProfile for user: {user.username}')
            except Exception as e:
                self.stdout.write(
                    self.style.ERROR(f'Failed to create UserProfile for user {user.username}: {str(e)}')
                )
        
        if created_count > 0:
            self.stdout.write(
                self.style.SUCCESS(f'Successfully created {created_count} UserProfile(s).')
            )
        else:
            self.stdout.write(self.style.WARNING('No new UserProfiles were created.'))