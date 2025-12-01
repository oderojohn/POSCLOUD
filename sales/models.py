from django.db import models

# Create your models here.

class Cart(models.Model):
    STATUS_CHOICES = [
        ('open', 'Open'),
        ('closed', 'Closed'),
        ('held', 'Held'),
        ('voided', 'Voided'),
    ]
    
    customer = models.ForeignKey('customers.Customer', on_delete=models.SET_NULL, null=True, blank=True)
    status = models.CharField(max_length=10, choices=STATUS_CHOICES, default='open')
    void_reason = models.TextField(blank=True, null=True)  # Reason for voiding held orders
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    cashier = models.ForeignKey('users.UserProfile', on_delete=models.SET_NULL, null=True)
    
    def __str__(self):
        return f"Cart {self.id} - {self.status}"

class CartItem(models.Model):
    cart = models.ForeignKey(Cart, on_delete=models.CASCADE)
    product = models.ForeignKey('inventory.Product', on_delete=models.CASCADE)
    quantity = models.PositiveIntegerField()
    unit_price = models.DecimalField(max_digits=10, decimal_places=2)
    discount = models.DecimalField(max_digits=10, decimal_places=2, default=0)
    
    def __str__(self):
        return f"{self.product.name} - {self.quantity}"

class Sale(models.Model):
    SALE_TYPES = [
        ('retail', 'Retail Sale'),
        ('wholesale', 'Wholesale Sale'),
    ]

    cart = models.OneToOneField(Cart, on_delete=models.CASCADE)
    customer = models.ForeignKey('customers.Customer', on_delete=models.SET_NULL, null=True, blank=True)
    shift = models.ForeignKey('shifts.Shift', on_delete=models.SET_NULL, null=True, blank=True)
    sale_type = models.CharField(max_length=10, choices=SALE_TYPES, default='retail')
    total_amount = models.DecimalField(max_digits=10, decimal_places=2)
    tax_amount = models.DecimalField(max_digits=10, decimal_places=2, default=0)
    discount_amount = models.DecimalField(max_digits=10, decimal_places=2, default=0)
    final_amount = models.DecimalField(max_digits=10, decimal_places=2)
    sale_date = models.DateTimeField(auto_now_add=True)
    receipt_number = models.CharField(max_length=50, unique=True)

    # Void functionality
    voided = models.BooleanField(default=False)
    void_reason = models.TextField(blank=True, null=True)
    voided_at = models.DateTimeField(null=True, blank=True)
    voided_by = models.ForeignKey('users.UserProfile', on_delete=models.SET_NULL, null=True, blank=True, related_name='voided_sales')
    
    def __str__(self):
        return f"Sale {self.receipt_number}"

class SaleItem(models.Model):
    sale = models.ForeignKey(Sale, on_delete=models.CASCADE)
    product = models.ForeignKey('inventory.Product', on_delete=models.CASCADE)
    quantity = models.PositiveIntegerField()
    unit_price = models.DecimalField(max_digits=10, decimal_places=2)
    discount = models.DecimalField(max_digits=10, decimal_places=2, default=0)

    def __str__(self):
        return f"{self.product.name} - {self.quantity}"

class Return(models.Model):
    sale = models.ForeignKey(Sale, on_delete=models.CASCADE)
    return_date = models.DateTimeField(auto_now_add=True)
    reason = models.TextField()
    total_refund_amount = models.DecimalField(max_digits=10, decimal_places=2)
    processed_by = models.ForeignKey('users.UserProfile', on_delete=models.SET_NULL, null=True)

    def __str__(self):
        return f"Return for Sale {self.sale.receipt_number}"

class Invoice(models.Model):
    INVOICE_STATUS = [
        ('draft', 'Draft'),
        ('sent', 'Sent'),
        ('paid', 'Paid'),
        ('overdue', 'Overdue'),
        ('cancelled', 'Cancelled'),
    ]

    invoice_number = models.CharField(max_length=50, unique=True)
    sale = models.OneToOneField(Sale, on_delete=models.SET_NULL, null=True, blank=True, related_name='invoice')
    customer = models.ForeignKey('customers.Customer', on_delete=models.CASCADE)
    invoice_date = models.DateField(auto_now_add=True)
    due_date = models.DateField()
    status = models.CharField(max_length=20, choices=INVOICE_STATUS, default='draft')

    # Financial details
    subtotal = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    tax_amount = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    discount_amount = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    total_amount = models.DecimalField(max_digits=12, decimal_places=2)

    # Additional fields
    notes = models.TextField(blank=True)
    terms = models.TextField(blank=True, default="Payment due within 30 days")
    created_by = models.ForeignKey('users.UserProfile', on_delete=models.SET_NULL, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"Invoice {self.invoice_number}"

    def save(self, *args, **kwargs):
        if not self.invoice_number:
            # Generate invoice number
            from django.utils import timezone
            self.invoice_number = f"INV{timezone.now().strftime('%Y%m%d')}{self.id or 1:04d}"
        super().save(*args, **kwargs)

    @property
    def is_overdue(self):
        from django.utils import timezone
        return self.status in ['sent', 'draft'] and self.due_date < timezone.now().date()

class InvoiceItem(models.Model):
    invoice = models.ForeignKey(Invoice, on_delete=models.CASCADE, related_name='items')
    product = models.ForeignKey('inventory.Product', on_delete=models.CASCADE, null=True, blank=True)
    description = models.CharField(max_length=200)
    quantity = models.DecimalField(max_digits=10, decimal_places=2)
    unit_price = models.DecimalField(max_digits=10, decimal_places=2)
    tax_rate = models.DecimalField(max_digits=5, decimal_places=2, default=0)  # Percentage
    discount_amount = models.DecimalField(max_digits=10, decimal_places=2, default=0)

    def __str__(self):
        return f"{self.description} - {self.quantity}"

    @property
    def subtotal(self):
        return self.quantity * self.unit_price

    @property
    def tax_amount(self):
        return self.subtotal * (self.tax_rate / 100)

    @property
    def total(self):
        return self.subtotal + self.tax_amount - self.discount_amount
