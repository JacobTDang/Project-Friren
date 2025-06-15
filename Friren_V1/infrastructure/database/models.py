from django.db import models
import uuid

class TransactionHistory(models.Model):
    """
    Store Transaction history with model scores
    """
    transaction_id = models.UUIDField(
        primary_key=True,
        default=uuid.uuid4,
        editable=False
    )

    symbol = models.CharField(max_length=20)
    quantity = models.DecimalField(max_digits=15, decimal_places=6)
    price = models.DecimalField(max_digits=15, decimal_places=6)
    timestamp = models.DateTimeField()

    order_id = models.CharField(max_length=50, blank=True, null=True)

    # Sentiment Scores
    finbert_sentiment = models.DecimalField(
        max_digits=4,
        decimal_places=3,
        blank=True,
        null=True
    )

    regime_sentiment = models.DecimalField(
        max_digits=4,
        decimal_places=3,
        blank=True,
        null=True
    )

    # ML Model Outputs
    confidence_score = models.DecimalField(
        max_digits=3,
        decimal_places=2,
        blank=True,
        null=True
    )

    xgboost_shap = models.JSONField(blank=True, null=True)  # Changed to JSONField

    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'transaction_history'
        indexes = [
            models.Index(fields=['symbol']),
            models.Index(fields=['timestamp']),
            models.Index(fields=['order_id']),
        ]

    def __str__(self):
        return f"{self.symbol}: {self.quantity} @ {self.price}"

class CurrentHoldings(models.Model):
  """
    Store the current position that are open
    """
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

class MLFeatures(models.Model):
    """
    Store ML features and market data at decision time
    Used for training and prediction tracking
    """
    feature_id = models.UUIDField(
        primary_key=True,
        default=uuid.uuid4,
        editable=False
    )
    symbol = models.CharField(max_length=20)
    timestamp = models.DateTimeField()

    # Link to transaction (if this led to a trade)
    transaction = models.ForeignKey(
        TransactionHistory,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='features'
    )

    # Price/Volume Features
    price = models.DecimalField(max_digits=15, decimal_places=6)
    volume = models.BigIntegerField(null=True, blank=True)

    # Technical Indicators
    rsi_14 = models.DecimalField(max_digits=5, decimal_places=2, null=True, blank=True)
    bb_upper = models.DecimalField(max_digits=15, decimal_places=6, null=True, blank=True)
    bb_lower = models.DecimalField(max_digits=15, decimal_places=6, null=True, blank=True)
    bb_middle = models.DecimalField(max_digits=15, decimal_places=6, null=True, blank=True)

    # Moving Averages
    sma_20 = models.DecimalField(max_digits=15, decimal_places=6, null=True, blank=True)
    sma_50 = models.DecimalField(max_digits=15, decimal_places=6, null=True, blank=True)
    ema_12 = models.DecimalField(max_digits=15, decimal_places=6, null=True, blank=True)
    ema_26 = models.DecimalField(max_digits=15, decimal_places=6, null=True, blank=True)

    # Sentiment Features (Updated to match TransactionHistory)
    finbert_sentiment = models.DecimalField(max_digits=4, decimal_places=3, null=True, blank=True)
    regime_sentiment = models.DecimalField(max_digits=4, decimal_places=3, null=True, blank=True)
    news_sentiment = models.DecimalField(max_digits=4, decimal_places=3, null=True, blank=True)

    # Market Context
    vix_level = models.DecimalField(max_digits=6, decimal_places=2, null=True, blank=True)
    market_regime = models.CharField(max_length=20, null=True, blank=True)

    # Strategy Information
    strategy_used = models.CharField(max_length=50, null=True, blank=True)
    strategy_signal_strength = models.DecimalField(max_digits=3, decimal_places=2, null=True, blank=True)

    # ML Explainability
    xgboost_shap_values = models.JSONField(null=True, blank=True)
    model_confidence = models.DecimalField(max_digits=3, decimal_places=2, null=True, blank=True)
    decision_reasoning = models.TextField(null=True, blank=True)

    # Custom/Extensible Features
    custom_features = models.JSONField(null=True, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'ml_features'
        indexes = [
            models.Index(fields=['symbol', 'timestamp']),
            models.Index(fields=['timestamp']),
            models.Index(fields=['transaction']),
            models.Index(fields=['strategy_used']),
        ]

    def __str__(self):
        return f"{self.symbol} {self.strategy_used} @ {self.timestamp}"
