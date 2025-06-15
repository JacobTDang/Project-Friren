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
