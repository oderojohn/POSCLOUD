from django.db import models

# Create your models here.

class Repair(models.Model):
    STATUS_CHOICES = [
        ('pending', 'Pending'),
        ('in_progress', 'In Progress'),
        ('completed', 'Completed'),
        ('cancelled', 'Cancelled'),
    ]
    
    customer = models.ForeignKey('customers.Customer', on_delete=models.CASCADE)
    device_model = models.CharField(max_length=200)
    device_type = models.CharField(max_length=100)  # Phone, Laptop, etc.
    serial_number = models.CharField(max_length=100, blank=True)
    issue_description = models.TextField()
    estimated_cost = models.DecimalField(max_digits=10, decimal_places=2, default=0)
    actual_cost = models.DecimalField(max_digits=10, decimal_places=2, default=0)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='pending')
    technician = models.ForeignKey('users.UserProfile', on_delete=models.SET_NULL, null=True, blank=True)
    assigned_at = models.DateTimeField(null=True, blank=True)
    completed_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    def __str__(self):
        return f"Repair {self.id} - {self.device_model}"

class RepairPart(models.Model):
    repair = models.ForeignKey(Repair, on_delete=models.CASCADE)
    product = models.ForeignKey('inventory.Product', on_delete=models.CASCADE)
    quantity = models.PositiveIntegerField()
    unit_cost = models.DecimalField(max_digits=10, decimal_places=2)
    
    def __str__(self):
        return f"{self.product.name} - {self.quantity} for Repair {self.repair.id}"
