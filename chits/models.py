from django.db import models

# Create your models here.

class Chit(models.Model):
    STATUS_CHOICES = [
        ('open', 'Open'),
        ('closed', 'Closed'),
    ]

    customer = models.ForeignKey('customers.Customer', on_delete=models.CASCADE, null=True, blank=True)
    customer_name = models.CharField(max_length=200, blank=True)  # For walk-in customers
    table_number = models.CharField(max_length=20, blank=True)
    amount = models.DecimalField(max_digits=10, decimal_places=2)
    description = models.TextField(blank=True)
    status = models.CharField(max_length=10, choices=STATUS_CHOICES, default='open')
    created_at = models.DateTimeField(auto_now_add=True)
    closed_at = models.DateTimeField(null=True, blank=True)

    def __str__(self):
        customer_name = self.customer.name if self.customer else self.customer_name
        return f"Chit {self.id} - {customer_name} - {self.status}"
