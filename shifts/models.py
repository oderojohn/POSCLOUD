from django.db import models

# Create your models here.

class Shift(models.Model):
    STATUS_CHOICES = [
        ('open', 'Open'),
        ('closed', 'Closed'),
    ]
    
    cashier = models.ForeignKey('users.UserProfile', on_delete=models.CASCADE)
    start_time = models.DateTimeField(auto_now_add=True)
    end_time = models.DateTimeField(null=True, blank=True)
    opening_balance = models.DecimalField(max_digits=10, decimal_places=2)
    closing_balance = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    cash_sales = models.DecimalField(max_digits=10, decimal_places=2, default=0)
    card_sales = models.DecimalField(max_digits=10, decimal_places=2, default=0)
    mobile_sales = models.DecimalField(max_digits=10, decimal_places=2, default=0)
    total_sales = models.DecimalField(max_digits=10, decimal_places=2, default=0)
    status = models.CharField(max_length=10, choices=STATUS_CHOICES, default='open')
    discrepancy = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    approved_by = models.ForeignKey('users.UserProfile', on_delete=models.SET_NULL, null=True, blank=True, related_name='approved_shifts')
    
    def __str__(self):
        return f"Shift {self.id} - {self.cashier.user.username} - {self.status}"
