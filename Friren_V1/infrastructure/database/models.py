from django.db import models
import uuid

class TransactionHistory(models.Model):
  transaction_id = models.UUIDField(
    primary_key=True,
    default=uuid.uuid4,
    editable=False
  )

  symbol = models.CharField(max_length=20)
  quantity = models.DecimalField(max_digits=15, decimal_places=6)
  price = models.DecimalField(max_digits=15, decimal_places = 6)
  timestamp = models.DateTimeField()

  order_id = models.CharField(max_length=50, blank=True, null=True)

  sentiment_score = models.DecimalField(
    max_digits=4,
    decimal_places=3,
    blank=True,
    null=True
  )

  confidence_score = models.DecimalField(
    max_digits=3,
    decimal_places=2,
    blank=True,
    null=True
  )

  xgBoost_SHAP = models.TextField(blank=True, null=True)

  created_at = models.DateTimeField(auto_now_add=True)

  class Meta:
    db_table='transaction_history'
    indexes=[
      models.Index(fields=['symbol']),
      models.Index(fields=['timestamp']),
      models.Index(fields=['order_id']),
    ]

  def __str__(self):
    return f"{self.symbol}: {self.quantity} @ {self.price}"

class CurrentHoldings(models.Model):
  holdings_id = models.UUIDField(
        primary_key=True,
        default=uuid.uuid4,
        editable=False
    )
  symbol = models.CharField(max_length=20, unique=True)

  # Position details (calculated from transaction_history)
  net_quantity = models.DecimalField(max_digits=15, decimal_places=6)
  avg_cost_basis = models.DecimalField(max_digits=15, decimal_places=6)
  total_invested = models.DecimalField(max_digits=15, decimal_places=2)

  # Performance tracking (from realized transactions)
  realized_pnl = models.DecimalField(max_digits=15, decimal_places=2, default=0)

  # Position metadata
  first_purchase_date = models.DateTimeField()
  last_transaction_date = models.DateTimeField()
  number_of_transactions = models.IntegerField(default=0)

  # Status
  is_active = models.BooleanField(default=True)  # False when net_quantity = 0

  # Timestamps
  created_at = models.DateTimeField(auto_now_add=True)
  updated_at = models.DateTimeField(auto_now=True)

  class Meta:
      db_table = 'current_holdings'
      indexes = [
          models.Index(fields=['symbol']),
          models.Index(fields=['is_active']),
          models.Index(fields=['last_transaction_date']),
      ]

  def __str__(self):
      return f"{self.symbol}: {self.net_quantity} shares @ ${self.avg_cost_basis}"

  @property
  def position_status(self):
      if self.net_quantity > 0:
          return "LONG"
      elif self.net_quantity < 0:
          return "SHORT"
      else:
          return "NEUTRAL"
