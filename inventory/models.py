from django.db import models

# Create your models here.

class Category(models.Model):
    name = models.CharField(max_length=100, unique=True)
    description = models.TextField(blank=True)
    
    def __str__(self):
        return self.name

class Product(models.Model):
    sku = models.CharField(max_length=50, unique=True)
    name = models.CharField(max_length=200)
    serial_number = models.CharField(max_length=100, blank=True, null=True)  # IMEI
    category = models.ForeignKey(Category, on_delete=models.SET_NULL, null=True)
    cost_price = models.DecimalField(max_digits=10, decimal_places=2)
    selling_price = models.DecimalField(max_digits=10, decimal_places=2)  # Retail price
    wholesale_price = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)  # Wholesale price
    wholesale_min_qty = models.PositiveIntegerField(default=10)  # Minimum quantity for wholesale pricing
    stock_quantity = models.PositiveIntegerField(default=0)
    low_stock_threshold = models.PositiveIntegerField(default=10)
    barcode = models.CharField(max_length=100, blank=True)
    description = models.TextField(blank=True)
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.sku} - {self.name}"

    @property
    def is_low_stock(self):
        return self.stock_quantity <= self.low_stock_threshold

    def save(self, *args, **kwargs):
        # Track changes for history logging
        is_new = self.pk is None
        old_instance = None

        if not is_new:
            try:
                old_instance = Product.objects.get(pk=self.pk)
            except Product.DoesNotExist:
                is_new = True

        super().save(*args, **kwargs)  # Save first to get pk for new instances

        if is_new:
            # New product
            ProductHistory.objects.create(
                product=self,
                change_type='create',
                notes='Product created'
            )
        else:
            # Existing product - log changes
            self._log_changes(old_instance)

    def _log_changes(self, old_instance):
        """Log changes to product fields"""
        from django.contrib.auth import get_user_model
        User = get_user_model()

        # Fields to track
        tracked_fields = [
            'name', 'sku', 'cost_price', 'selling_price', 'wholesale_price',
            'wholesale_min_qty', 'stock_quantity', 'low_stock_threshold',
            'barcode', 'description', 'is_active', 'category'
        ]

        for field in tracked_fields:
            old_value = getattr(old_instance, field)
            new_value = getattr(self, field)

            if old_value != new_value:
                # Get current user from middleware
                from branches.middleware import BranchContextMiddleware
                user = BranchContextMiddleware.get_current_user()

                ProductHistory.objects.create(
                    product=self,
                    field_changed=field,
                    old_value=str(old_value) if old_value is not None else '',
                    new_value=str(new_value) if new_value is not None else '',
                    change_type='update',
                    user=user.userprofile if user else None,
                    notes=f'Updated {field}'
                )

class Batch(models.Model):
    BATCH_STATUS = [
        ('ordered', 'Ordered'),
        ('received', 'Received'),
        ('expired', 'Expired'),
    ]

    product = models.ForeignKey(Product, on_delete=models.CASCADE)
    batch_number = models.CharField(max_length=50)
    quantity = models.PositiveIntegerField()
    cost_price = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True, help_text="Cost price per unit for this batch")
    expiry_date = models.DateField(null=True, blank=True)
    purchase_date = models.DateField()
    supplier = models.ForeignKey('suppliers.Supplier', on_delete=models.SET_NULL, null=True)
    purchase_order_item = models.ForeignKey('suppliers.PurchaseOrderItem', on_delete=models.SET_NULL, null=True, blank=True, related_name='batches')
    status = models.CharField(max_length=20, choices=BATCH_STATUS, default='ordered')
    received_date = models.DateField(null=True, blank=True)

    def __str__(self):
        return f"{self.product.name} - Batch {self.batch_number}"

    def receive_batch(self, actual_quantity=None):
        """Mark batch as received and add to stock"""
        from django.utils import timezone
        from .models import StockMovement

        if self.status != 'received':
            quantity_to_add = actual_quantity if actual_quantity is not None else self.quantity

            self.status = 'received'
            if not self.received_date:
                self.received_date = timezone.now().date()
            self.save()

            # Add to product stock
            self.product.stock_quantity += quantity_to_add
            self.product.save()

            # Create stock movement record
            StockMovement.objects.create(
                product=self.product,
                movement_type='in',
                quantity=quantity_to_add,
                reason=f'Batch {self.batch_number} received',
                user=None  # TODO: Add user when authentication is available
            )

            # Update purchase order item
            if self.purchase_order_item:
                self.purchase_order_item.received_quantity += quantity_to_add
                self.purchase_order_item.save()
                self.purchase_order_item.purchase_order.update_status()

class StockMovement(models.Model):
    MOVEMENT_TYPES = [
        ('in', 'Stock In'),
        ('out', 'Stock Out'),
        ('adjustment', 'Adjustment'),
    ]

    product = models.ForeignKey(Product, on_delete=models.CASCADE)
    movement_type = models.CharField(max_length=10, choices=MOVEMENT_TYPES)
    quantity = models.IntegerField()  # positive for in, negative for out
    reason = models.CharField(max_length=200, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    user = models.ForeignKey('users.UserProfile', on_delete=models.SET_NULL, null=True)

    def __str__(self):
        return f"{self.product.name} - {self.movement_type} - {self.quantity}"

class Supplier(models.Model):
    name = models.CharField(max_length=200)
    contact_person = models.CharField(max_length=100, blank=True)
    phone = models.CharField(max_length=15)
    email = models.EmailField(blank=True)
    address = models.TextField(blank=True)
    is_active = models.BooleanField(default=True)

    def __str__(self):
        return self.name

class Purchase(models.Model):
    product = models.ForeignKey(Product, on_delete=models.CASCADE)
    supplier = models.ForeignKey(Supplier, on_delete=models.SET_NULL, null=True)
    quantity = models.PositiveIntegerField()
    unit_price = models.DecimalField(max_digits=10, decimal_places=2)
    total_price = models.DecimalField(max_digits=10, decimal_places=2)
    purchase_date = models.DateField(auto_now_add=True)
    batch_number = models.CharField(max_length=50, blank=True)

    def __str__(self):
        return f"Purchase {self.product.name} - {self.quantity}"

class PriceHistory(models.Model):
    supplier = models.ForeignKey(Supplier, on_delete=models.CASCADE)
    product = models.ForeignKey(Product, on_delete=models.CASCADE)
    price = models.DecimalField(max_digits=10, decimal_places=2)
    date = models.DateField(auto_now_add=True)

    def __str__(self):
        return f"{self.supplier.name} - {self.product.name} - {self.price} on {self.date}"

class SalesHistory(models.Model):
    product = models.ForeignKey(Product, on_delete=models.CASCADE)
    batch = models.ForeignKey(Batch, on_delete=models.SET_NULL, null=True, blank=True)
    customer = models.ForeignKey('customers.Customer', on_delete=models.SET_NULL, null=True)
    quantity = models.PositiveIntegerField()
    unit_price = models.DecimalField(max_digits=10, decimal_places=2)  # Selling price
    cost_price = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)  # Cost from batch
    total_price = models.DecimalField(max_digits=10, decimal_places=2)
    profit = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    sale_date = models.DateTimeField(auto_now_add=True)
    receipt_number = models.CharField(max_length=50)

    def save(self, *args, **kwargs):
        if self.cost_price and self.unit_price:
            # Ensure both are Decimal types for calculation
            from decimal import Decimal
            unit_price = Decimal(str(self.unit_price))
            cost_price = Decimal(str(self.cost_price))
            self.profit = (unit_price - cost_price) * Decimal(str(self.quantity))
        super().save(*args, **kwargs)

    def __str__(self):
        return f"Sale {self.receipt_number} - {self.product.name} - Batch {self.batch.batch_number if self.batch else 'N/A'}"

class ProductHistory(models.Model):
    product = models.ForeignKey(Product, on_delete=models.CASCADE)
    field_changed = models.CharField(max_length=100)  # e.g., 'selling_price', 'stock_quantity'
    old_value = models.TextField(blank=True, null=True)
    new_value = models.TextField(blank=True, null=True)
    change_type = models.CharField(max_length=20, choices=[
        ('create', 'Created'),
        ('update', 'Updated'),
        ('delete', 'Deleted'),
    ], default='update')
    changed_at = models.DateTimeField(auto_now_add=True)
    user = models.ForeignKey('users.UserProfile', on_delete=models.SET_NULL, null=True, blank=True)
    notes = models.TextField(blank=True, null=True)  # Optional notes about the change

    def __str__(self):
        return f"{self.product.name} - {self.field_changed}: {self.old_value} -> {self.new_value} at {self.changed_at}"

    class Meta:
        ordering = ['-changed_at']
