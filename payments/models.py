from django.db import models

# Create your models here.

class Payment(models.Model):
    PAYMENT_TYPES = [
        ('cash', 'Cash'),
        ('mpesa', 'MPesa'),
        ('split', 'Split'),
    ]

    sale = models.ForeignKey('sales.Sale', on_delete=models.CASCADE, null=True, blank=True)
    customer = models.ForeignKey('customers.Customer', on_delete=models.CASCADE, null=True, blank=True)
    payment_type = models.CharField(max_length=20, choices=PAYMENT_TYPES)
    amount = models.DecimalField(max_digits=10, decimal_places=2, help_text="Positive for payments received, negative for credit reductions")
    reference_number = models.CharField(max_length=100, blank=True, null=True)
    mpesa_number = models.CharField(max_length=20, blank=True, null=True)  # Store MPesa number for MPesa payments
    gateway_response = models.TextField(blank=True)
    description = models.CharField(max_length=255, blank=True, null=True)
    split_data = models.JSONField(blank=True, null=True, help_text="Split payment data for split payments")
    status = models.CharField(max_length=20, choices=[
        ('pending', 'Pending'),
        ('completed', 'Completed'),
        ('failed', 'Failed'),
        ('refunded', 'Refunded'),
    ], default='pending')
    created_at = models.DateTimeField(auto_now_add=True)
    
    def __str__(self):
        return f"{self.payment_type} - {self.amount} for Sale {self.sale.receipt_number}"

class PaymentLog(models.Model):
    payment = models.ForeignKey(Payment, on_delete=models.CASCADE)
    log_message = models.TextField()
    timestamp = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"Log for Payment {self.payment.id}"

class InstallmentPlan(models.Model):
    sale = models.ForeignKey('sales.Sale', on_delete=models.CASCADE)
    total_amount = models.DecimalField(max_digits=10, decimal_places=2)
    number_of_installments = models.PositiveIntegerField()
    installment_amount = models.DecimalField(max_digits=10, decimal_places=2)
    remaining_balance = models.DecimalField(max_digits=10, decimal_places=2)
    due_date = models.DateField()
    status = models.CharField(max_length=20, choices=[
        ('active', 'Active'),
        ('completed', 'Completed'),
        ('defaulted', 'Defaulted'),
    ], default='active')
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"Installment for Sale {self.sale.receipt_number}"
