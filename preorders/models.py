from django.db import models

# Create your models here.

class Preorder(models.Model):
    STATUS_CHOICES = [
        ('pending', 'Pending'),
        ('available', 'Available'),
        ('fulfilled', 'Fulfilled'),
        ('cancelled', 'Cancelled'),
    ]
    
    customer = models.ForeignKey('customers.Customer', on_delete=models.CASCADE)
    product = models.ForeignKey('inventory.Product', on_delete=models.CASCADE)
    quantity = models.PositiveIntegerField()
    deposit_amount = models.DecimalField(max_digits=10, decimal_places=2)
    outstanding_balance = models.DecimalField(max_digits=10, decimal_places=2)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='pending')
    preorder_date = models.DateField(auto_now_add=True)
    expected_date = models.DateField(null=True, blank=True)
    fulfilled_date = models.DateField(null=True, blank=True)
    
    def __str__(self):
        return f"Preorder {self.id} - {self.product.name} for {self.customer.name}"

class PreorderPayment(models.Model):
    preorder = models.ForeignKey(Preorder, on_delete=models.CASCADE)
    amount = models.DecimalField(max_digits=10, decimal_places=2)
    payment_date = models.DateField(auto_now_add=True)
    reference = models.CharField(max_length=100, blank=True)
    
    def __str__(self):
        return f"Payment {self.amount} for Preorder {self.preorder.id}"
