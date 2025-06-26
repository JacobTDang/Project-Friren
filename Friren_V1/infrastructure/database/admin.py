from django.contrib import admin
from .models import TransactionHistory, CurrentHoldings, MLFeatures

@admin.register(TransactionHistory)
class TransactionHistoryAdmin(admin.ModelAdmin):
    list_display = ['symbol', 'quantity', 'price', 'timestamp', 'finbert_sentiment', 'regime_sentiment']
    list_filter = ['symbol', 'timestamp']
    search_fields = ['symbol', 'order_id']
    readonly_fields = ['transaction_id', 'created_at']

@admin.register(CurrentHoldings)
class CurrentHoldingsAdmin(admin.ModelAdmin):
    list_display = ['symbol', 'net_quantity', 'avg_cost_basis', 'is_active', 'last_transaction_date']
    list_filter = ['is_active', 'last_transaction_date']
    search_fields = ['symbol']
    readonly_fields = ['holdings_id', 'created_at', 'updated_at']

@admin.register(MLFeatures)
class MLFeaturesAdmin(admin.ModelAdmin):
    list_display = ['symbol', 'strategy_used', 'finbert_sentiment', 'regime_sentiment', 'timestamp']
    list_filter = ['strategy_used', 'symbol', 'market_regime']
    search_fields = ['symbol']
    readonly_fields = ['feature_id', 'created_at']
