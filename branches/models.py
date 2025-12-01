from django.db import models

# Create your models here.

class Branch(models.Model):
    name = models.CharField(max_length=100)
    location = models.CharField(max_length=200)
    address = models.TextField()
    phone = models.CharField(max_length=15)
    manager = models.ForeignKey('users.UserProfile', on_delete=models.SET_NULL, null=True, blank=True, related_name='managed_branches')
    is_active = models.BooleanField(default=True)
    
    def __str__(self):
        return self.name
