from django.db import models
import uuid
from django.utils import timezone

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
        app_label = 'database'
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
    realized_pnl = models.DecimalField(max_digits=15, decimal_places=2, default='0')

    # Position metadata
    first_purchase_date = models.DateTimeField()
    last_transaction_date = models.DateTimeField()
    number_of_transactions = models.IntegerField(default=0)

    # Strategy Assignment (New fields for three-scenario system)
    current_strategy = models.CharField(max_length=50, null=True, blank=True, help_text="Currently assigned trading strategy")
    assignment_scenario = models.CharField(
        max_length=30, 
        choices=[
            ('user_buy_hold', 'User Buy & Hold'),
            ('decision_engine_choice', 'Decision Engine Algorithmic'),
            ('strategy_reevaluation', 'Position Health Monitor Reassignment')
        ],
        null=True, 
        blank=True,
        help_text="How this strategy was assigned"
    )
    assignment_reason = models.CharField(
        max_length=30,
        choices=[
            ('new_position', 'New Position'),
            ('regime_change', 'Market Regime Change'),
            ('poor_performance', 'Poor Performance'),
            ('diversification', 'Portfolio Diversification'),
            ('manual_override', 'Manual Override'),
            ('rebalance', 'Periodic Rebalance'),
            ('user_buy_hold', 'User Buy & Hold'),
            ('decision_engine_choice', 'Decision Engine Choice'),
            ('strategy_reevaluation', 'Strategy Reevaluation')
        ],
        null=True,
        blank=True,
        help_text="Reason for current strategy assignment"
    )
    strategy_confidence = models.DecimalField(
        max_digits=5, 
        decimal_places=2, 
        null=True, 
        blank=True,
        help_text="Confidence score for current strategy assignment (0-100)"
    )
    strategy_assigned_at = models.DateTimeField(
        null=True, 
        blank=True,
        help_text="When current strategy was assigned"
    )
    previous_strategy = models.CharField(
        max_length=50, 
        null=True, 
        blank=True,
        help_text="Previous strategy before reassignment"
    )
    assignment_metadata = models.JSONField(
        null=True, 
        blank=True,
        help_text="Additional metadata about strategy assignment"
    )

    # Status
    is_active = models.BooleanField(default=True)  # False when net_quantity = 0

    # Timestamps
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        app_label = 'database'
        db_table = 'current_holdings'
        indexes = [
            models.Index(fields=['symbol']),
            models.Index(fields=['is_active']),
            models.Index(fields=['last_transaction_date']),
            models.Index(fields=['current_strategy']),
            models.Index(fields=['assignment_scenario']),
            models.Index(fields=['strategy_assigned_at']),
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
    
    def assign_strategy(self, strategy_name, scenario, reason, confidence=None, metadata=None):
        """Update strategy assignment for this holding"""
        from django.utils import timezone
        
        # Store previous strategy for history
        self.previous_strategy = self.current_strategy
        
        # Update current assignment
        self.current_strategy = strategy_name
        self.assignment_scenario = scenario
        self.assignment_reason = reason
        self.strategy_confidence = confidence
        self.strategy_assigned_at = timezone.now()
        self.assignment_metadata = metadata or {}
        
        self.save(update_fields=[
            'current_strategy', 'assignment_scenario', 'assignment_reason',
            'strategy_confidence', 'strategy_assigned_at', 'previous_strategy',
            'assignment_metadata'
        ])
    
    def is_user_controlled(self):
        """Check if this is a user buy-hold position"""
        return self.assignment_scenario == 'user_buy_hold'
    
    def is_algorithmic(self):
        """Check if this is algorithmically managed"""
        return self.assignment_scenario in ['decision_engine_choice', 'strategy_reevaluation']
    
    def days_since_strategy_assigned(self):
        """Calculate days since strategy was assigned"""
        if self.strategy_assigned_at:
            from django.utils import timezone
            return (timezone.now() - self.strategy_assigned_at).days
        return None
    
    def needs_strategy_reevaluation(self, threshold_days=30):
        """Check if strategy needs reevaluation based on time"""
        days_since = self.days_since_strategy_assigned()
        return days_since is not None and days_since >= threshold_days

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
        app_label = 'database'
        db_table = 'ml_features'
        indexes = [
            models.Index(fields=['symbol', 'timestamp']),
            models.Index(fields=['timestamp']),
            models.Index(fields=['transaction']),
            models.Index(fields=['strategy_used']),
        ]

    def __str__(self):
        return f"{self.symbol} {self.strategy_used} @ {self.timestamp}"

class TradingWatchlist(models.Model):
    """
    Comprehensive watchlist for stocks we're monitoring
    Includes both current holdings and potential opportunities
    """
    watchlist_id = models.UUIDField(
        primary_key=True,
        default=uuid.uuid4,
        editable=False
    )
    symbol = models.CharField(max_length=20, unique=True)

    # Watchlist categories
    class WatchlistStatus(models.TextChoices):
        HOLDING = 'HOLDING', 'Currently Holding'
        WATCHING = 'WATCHING', 'Watching for Entry'
        ANALYZING = 'ANALYZING', 'Under Analysis'
        BLACKLIST = 'BLACKLIST', 'Temporarily Blacklisted'
        SOLD = 'SOLD', 'Recently Sold - Monitoring'

    status = models.CharField(
        max_length=20,
        choices=WatchlistStatus.choices,
        default=WatchlistStatus.WATCHING
    )

    # Priority and monitoring settings
    priority = models.IntegerField(default=5)  # 1-10 scale, 10 = highest priority
    monitor_frequency = models.IntegerField(default=60)  # seconds between checks

    # Entry/Exit criteria
    target_entry_price = models.DecimalField(max_digits=15, decimal_places=6, null=True, blank=True)
    stop_loss_price = models.DecimalField(max_digits=15, decimal_places=6, null=True, blank=True)
    take_profit_price = models.DecimalField(max_digits=15, decimal_places=6, null=True, blank=True)
    max_position_size_pct = models.DecimalField(max_digits=5, decimal_places=2, default='10.00')  # Max % of portfolio

    # Analysis tracking
    sentiment_score = models.DecimalField(max_digits=4, decimal_places=3, null=True, blank=True)
    news_sentiment = models.DecimalField(max_digits=4, decimal_places=3, null=True, blank=True)
    technical_score = models.DecimalField(max_digits=4, decimal_places=3, null=True, blank=True)
    risk_score = models.DecimalField(max_digits=4, decimal_places=3, null=True, blank=True)

    # Strategy preferences
    preferred_strategies = models.JSONField(default=list, blank=True)  # List of strategy names
    blacklisted_strategies = models.JSONField(default=list, blank=True)

    # Notes and metadata
    notes = models.TextField(blank=True)
    tags = models.JSONField(default=list, blank=True)  # Custom tags like ['tech', 'growth', 'dividend']
    sector = models.CharField(max_length=50, blank=True)
    market_cap = models.CharField(max_length=20, blank=True)  # small, mid, large

    # Relationship to current holdings
    current_holding = models.ForeignKey(
        CurrentHoldings,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='watchlist_entry'
    )

    # Monitoring timestamps
    added_date = models.DateTimeField(auto_now_add=True)
    last_analyzed = models.DateTimeField(null=True, blank=True)
    last_price_check = models.DateTimeField(null=True, blank=True)
    last_news_check = models.DateTimeField(null=True, blank=True)

    # Status tracking
    is_active = models.BooleanField(default=True)
    analysis_count = models.IntegerField(default=0)
    alert_triggered_count = models.IntegerField(default=0)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        app_label = 'database'
        db_table = 'trading_watchlist'
        indexes = [
            models.Index(fields=['symbol']),
            models.Index(fields=['status']),
            models.Index(fields=['priority', 'is_active']),
            models.Index(fields=['last_analyzed']),
            models.Index(fields=['sector']),
        ]
        ordering = ['-priority', 'symbol']

    def __str__(self):
        return f"{self.symbol} ({self.status}) - Priority: {self.priority}"

    def is_holding_position(self):
        return self.status == self.WatchlistStatus.HOLDING and self.current_holding is not None

    def days_since_added(self):
        if self.added_date:
            return (timezone.now() - self.added_date).days
        return 0

    def update_analysis_timestamp(self):
        """Update the last analyzed timestamp"""
        self.last_analyzed = timezone.now()
        self.save(update_fields=['last_analyzed'])

    def set_holding_status(self, holding_record):
        """Link this watchlist entry to a current holding"""
        self.status = self.WatchlistStatus.HOLDING
        self.current_holding = holding_record
        self.save(update_fields=['status', 'current_holding'])

    def remove_holding_status(self, sold_recently=True):
        """Remove holding status and optionally mark as recently sold"""
        self.current_holding = None
        if sold_recently:
            self.status = self.WatchlistStatus.SOLD
        else:
            self.status = self.WatchlistStatus.WATCHING
        self.save(update_fields=['status', 'current_holding'])
