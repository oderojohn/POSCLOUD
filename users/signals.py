from django.db.models.signals import post_save
from django.dispatch import receiver
from django.contrib.auth.models import User
from .models import UserProfile

@receiver(post_save, sender=User)
def create_user_profile(sender, instance, created, **kwargs):
    """
    Automatically create a UserProfile when a User is created.
    This prevents race conditions when multiple processes try to create UserProfile.
    """
    if created:
        try:
            UserProfile.objects.get_or_create(
                user=instance,
                defaults={'role': 'cashier'}
            )
        except Exception as e:
            # Log the error but don't fail the user creation
            print(f"Error creating UserProfile for user {instance.username}: {str(e)}")

@receiver(post_save, sender=User)
def save_user_profile(sender, instance, **kwargs):
    """
    Save the UserProfile when the User is saved.
    """
    if hasattr(instance, 'userprofile'):
        instance.userprofile.save()