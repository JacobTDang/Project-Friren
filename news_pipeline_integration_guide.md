# News Pipeline Integration Guide

## Overview

This guide shows how to integrate news collection, FinBERT sentiment analysis, and XGBoost recommendations into your existing `orchestrator.py` system. The pipeline runs every 15 minutes during market hours and sends processed results to your decision engine via the queue system.

## Files Created

### 1. `enhanced_news_finbert_xgboost_pipeline.py`
- **Core news processing pipeline**
- **15-minute scheduled execution**
- **FinBERT sentiment analysis**
- **XGBoost recommendation engine**
- **Queue-based communication**
- **Resource management for t3.micro**

### 2. `orchestrator_news_integration.py`
- **Integration wrapper for your orchestrator.py**
- **Seamless startup/shutdown management**
- **Configuration and monitoring**
- **Error handling and recovery**

### 3. `queue_based_news_xgboost_test.py` (Enhanced)
- **Comprehensive testing framework**
- **Queue message flow validation**
- **Performance metrics**

## Integration Steps

### Step 1: Import the Integration Module

Add this to your main orchestrator script:

```python
from orchestrator_news_integration import (
    create_enhanced_orchestrator,
    NewsIntegrationConfig
)
```

### Step 2: Configure News Pipeline

```python
# Configure news pipeline for 15-minute intervals
news_config = NewsIntegrationConfig(
    enable_news_pipeline=True,
    news_collection_interval=15,  # Every 15 minutes
    include_extended_hours=True,  # Pre/post market
    use_orchestrator_symbols=True,  # Use your existing symbols
    additional_symbols=['SPY', 'QQQ'],  # Add market ETFs
    max_articles_per_symbol=12,
    auto_start_with_orchestrator=True,
    send_to_decision_engine=True
)
```

### Step 3: Create Enhanced Orchestrator

```python
# Replace your existing orchestrator creation with:
enhanced_orchestrator = create_enhanced_orchestrator(
    system_config=your_system_config,
    news_config=news_config
)
```

### Step 4: Use Enhanced Orchestrator

```python
# Initialize and start (same API as your existing orchestrator)
success = enhanced_orchestrator.initialize_system()
if success:
    enhanced_orchestrator.start_system()

    # Monitor status
    status = enhanced_orchestrator.get_system_status()
    print(f"News pipeline: {status['news_pipeline']['state']}")

    # Your existing trading logic continues unchanged...

    # Stop when done
    enhanced_orchestrator.stop_system()
```

## News Pipeline Features

### Scheduled Execution
- **Every 15 minutes during market hours**
- **Configurable extended hours support**
- **Automatic market hours detection**
- **Weekend/holiday pause**

### Stock-Specific Tracking
```python
# The pipeline automatically tracks your monitored symbols
# Updates every 15 minutes with:
# - Latest news articles
# - FinBERT sentiment analysis
# - XGBoost trading recommendations
# - Market event detection
```

### Queue Integration
```python
# News pipeline sends messages to your decision engine:
{
    "symbol": "AAPL",
    "news_data": {
        "news_volume": 8,
        "sentiment_score": 0.35,
        "confidence": 0.78,
        "market_events": ["earnings_report"]
    },
    "sentiment_data": {
        "average_confidence": 0.82,
        "articles_processed": 8
    },
    "recommendation": {
        "action": "BUY",
        "confidence": 0.84,
        "reasoning": "positive news sentiment, high news volume"
    }
}
```

## Integration with Your Existing Functions

### News Collector Functions
Your existing news collector functions are automatically integrated:

```python
# Your EnhancedNewsCollector.collect_symbol_news() is used
# Your EnhancedNewsCollector.collect_watchlist_news() is used
# Individual symbol tracking is handled automatically
```

### FinBERT Analysis
Your existing FinBERT analysis is integrated:

```python
# Your EnhancedFinBERT.analyze_batch() is used
# Sentiment processing is automated
# Results are sent to decision engine queue
```

### Decision Engine Integration
```python
# In your decision engine, handle news updates:
def handle_sentiment_update(self, message):
    symbol = message.payload['symbol']
    news_data = message.payload['news_data']
    recommendation = message.payload['recommendation']

    # Process news-based recommendation
    if recommendation and recommendation['confidence'] > 0.7:
        self.consider_news_recommendation(symbol, recommendation)
```

## Configuration Options

### Resource Management (t3.micro optimized)
```python
news_config = NewsIntegrationConfig(
    # Memory limits
    news_memory_limit_mb=300,
    finbert_batch_size=4,
    max_concurrent_processing=3,

    # Processing limits
    max_articles_per_symbol=12,
    processing_timeout_seconds=45
)
```

### Symbol Management
```python
# Use orchestrator symbols
use_orchestrator_symbols=True

# Add additional symbols
additional_symbols=['SPY', 'QQQ', 'VIX']

# Update symbols dynamically
enhanced_orchestrator.update_monitored_symbols(['AAPL', 'MSFT', 'NVDA'])
```

### Monitoring Features
```python
# Get detailed metrics
metrics = enhanced_orchestrator.get_news_pipeline_metrics()
print(f"Articles per cycle: {metrics['performance_indicators']['articles_per_cycle']}")
print(f"Success rate: {metrics['performance_indicators']['success_rate']:.1f}%")

# Health monitoring
health = metrics['health_indicators']['status_health']  # healthy/degraded/critical
```

## Example Production Setup

```python
#!/usr/bin/env python3
"""
production_orchestrator.py - Your enhanced trading system
"""

from orchestrator_news_integration import (
    create_enhanced_orchestrator,
    NewsIntegrationConfig
)
from Friren_V1.trading_engine.portfolio_manager.py.orchestrator import SystemConfig, TradingMode

def main():
    # Your existing system configuration
    system_config = SystemConfig(
        trading_mode=TradingMode.LIVE_TRADING,  # or PAPER_TRADING
        symbols=['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'NVDA'],
        max_positions=5,
        portfolio_value=100000.0
    )

    # News pipeline configuration
    news_config = NewsIntegrationConfig(
        enable_news_pipeline=True,
        news_collection_interval=15,  # Every 15 minutes
        include_extended_hours=True,
        use_orchestrator_symbols=True,
        additional_symbols=['SPY', 'QQQ'],  # Market indicators
        auto_start_with_orchestrator=True,
        send_to_decision_engine=True
    )

    # Create enhanced orchestrator
    orchestrator = create_enhanced_orchestrator(system_config, news_config)

    try:
        # Initialize system
        if orchestrator.initialize_system():
            print("✅ Enhanced orchestrator initialized")

            # Start system (includes news pipeline)
            if orchestrator.start_system():
                print("✅ Enhanced orchestrator started")
                print("📰 News collection every 15 minutes")
                print("🤖 FinBERT analysis active")
                print("📊 XGBoost recommendations enabled")

                # Your trading system runs here...
                # The news pipeline operates in background every 15 minutes

                # Monitor status periodically
                import time
                while True:
                    time.sleep(300)  # Check every 5 minutes
                    status = orchestrator.get_system_status()
                    print(f"System status: {status['integration_status']}")
                    print(f"News pipeline: {status['news_pipeline']['state']}")

            else:
                print("❌ Failed to start enhanced orchestrator")
        else:
            print("❌ Failed to initialize enhanced orchestrator")

    except KeyboardInterrupt:
        print("\n🛑 Shutting down enhanced orchestrator...")
        orchestrator.stop_system()
        print("✅ Enhanced orchestrator stopped")

if __name__ == "__main__":
    main()
```

## Testing and Validation

### Run the Test Suite
```bash
python queue_based_news_xgboost_test.py
```

### Test Enhanced Integration
```bash
python orchestrator_news_integration.py
```

### Monitor Logs
The system provides comprehensive logging:
- News collection activities
- FinBERT processing results
- XGBoost recommendations
- Queue message flows
- Error handling and recovery

## Troubleshooting

### Common Issues

1. **News Pipeline Not Starting**
   - Check import dependencies
   - Verify API keys for news sources
   - Check memory limits

2. **FinBERT Errors**
   - Pipeline falls back to mock sentiment
   - Check transformers library installation
   - Monitor memory usage

3. **Queue Communication Issues**
   - Verify queue manager initialization
   - Check message format compatibility
   - Monitor queue sizes

### Performance Monitoring
```python
# Check pipeline health
status = orchestrator.get_system_status()
news_health = status['news_pipeline']['metrics']['health_indicators']['status_health']

if news_health == 'critical':
    print("⚠️ News pipeline needs attention")
    # Restart pipeline
    orchestrator.restart_news_pipeline()
```

## Resource Usage (t3.micro)

The integration is optimized for AWS t3.micro:
- **Memory**: ~300MB for news pipeline
- **CPU**: Burst-friendly processing
- **Network**: Efficient API usage
- **Storage**: Minimal caching

## Next Steps

1. **Deploy the enhanced orchestrator**
2. **Monitor 15-minute collection cycles**
3. **Review news-based recommendations**
4. **Fine-tune symbol lists and parameters**
5. **Scale up resources as needed**

The news pipeline will now automatically:
- ✅ Collect news every 15 minutes
- ✅ Process sentiment with FinBERT
- ✅ Generate XGBoost recommendations
- ✅ Send results to your decision engine
- ✅ Track stock-specific patterns
- ✅ Handle errors and recovery

Your existing orchestrator functionality remains unchanged while gaining powerful news analysis capabilities!
