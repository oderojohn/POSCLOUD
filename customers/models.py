from django.db import models

# Create your models here.

class Customer(models.Model):
    CUSTOMER_TYPES = [
        ('retail', 'Retail Customer'),
        ('wholesale', 'Wholesale Customer'),
    ]

    name = models.CharField(max_length=200)
    phone = models.CharField(max_length=15, unique=True)
    email = models.EmailField(blank=True)
    address = models.TextField(blank=True)
    customer_type = models.CharField(max_length=10, choices=CUSTOMER_TYPES, default='retail')
    business_name = models.CharField(max_length=200, blank=True)  # For wholesale customers
    tax_id = models.CharField(max_length=50, blank=True)  # VAT/Tax ID for wholesale
    loyalty_points = models.PositiveIntegerField(default=0)
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    
    def __str__(self):
        return f"{self.name} - {self.phone}"

class LoyaltyTransaction(models.Model):
    TRANSACTION_TYPES = [
        ('earn', 'Earned'),
        ('redeem', 'Redeemed'),
    ]
    
    customer = models.ForeignKey(Customer, on_delete=models.CASCADE)
    transaction_type = models.CharField(max_length=10, choices=TRANSACTION_TYPES)
    points = models.PositiveIntegerField()
    reason = models.CharField(max_length=200)
    created_at = models.DateTimeField(auto_now_add=True)
    
    def __str__(self):
        return f"{self.customer.name} - {self.transaction_type} {self.points} points"
