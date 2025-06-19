# Enhanced News Pipeline Integration

## Overview

The Enhanced News Pipeline has been successfully integrated into your existing portfolio manager infrastructure. This integration provides a complete news-to-recommendation pipeline that runs every 15 minutes during market hours and delivers high-quality trading signals to your decision engine.

## Architecture Integration

### Process Hierarchy

Your portfolio manager now includes the enhanced news pipeline as one of its 5 core processes:

1. **Market Decision Engine** (main process) - Coordinates trading decisions
2. **Position Health Monitor** - Monitors portfolio health and risk
3. **Strategy Analyzer** - Analyzes trading strategies and performance
4. **Sentiment Analyzer (FinBERT)** - Handles additional sentiment tasks
5. **Enhanced News Pipeline** - Complete news collection, FinBERT analysis, and XGBoost recommendations

### Integration Points

#### 1. Process Infrastructure
- **BaseProcess Integration**: Inherits from your existing `BaseProcess` class
- **Lifecycle Management**: Follows standard initialization, processing, and cleanup patterns
- **Error Handling**: Integrates with your process restart and error tracking systems

#### 2. Communication Layer
- **Queue-Based Messaging**: Uses your existing `QueueManager` for inter-process communication
- **Message Types**: Sends `TRADING_RECOMMENDATION` and `PIPELINE_STATUS` messages
- **Priority System**: High-confidence recommendations get priority routing

#### 3. State Management
- **SharedStateManager**: Integrates with your shared state system
- **Metrics Tracking**: Provides comprehensive pipeline performance metrics
- **Symbol Tracking**: Individual monitoring for each watchlist symbol

## Features

### Complete Pipeline Integration

#### News Collection
- **Multi-Source Collection**: Gathers news from multiple sources per symbol
- **Quality Filtering**: Only processes high-quality, relevant articles
- **Resource Management**: Optimized for AWS t3.micro memory constraints

#### FinBERT Sentiment Analysis
- **Batch Processing**: Processes articles in memory-efficient batches
- **Confidence Filtering**: Only includes high-confidence sentiment results
- **Enhanced Metrics**: Provides detailed sentiment scoring and market impact

#### XGBoost Recommendations
- **Feature Engineering**: Creates ML features from news and sentiment data
- **Trading Signals**: Generates BUY, SELL, or HOLD recommendations
- **Risk Assessment**: Includes risk scoring and expected return estimates
- **Reasoning**: Provides human-readable explanation for each recommendation

### Performance Optimization

#### Resource Management
- **Memory Limit**: Configured for 300MB maximum usage on t3.micro
- **Batch Processing**: Processes articles in small batches (4 per batch)
- **Efficient Caching**: Optional caching with TTL for repeated requests

#### Scheduling
- **Market Hours Only**: Runs only during market hours (9:30 AM - 4:00 PM ET)
- **15-Minute Intervals**: Configurable cycle time (default 15 minutes)
- **Concurrent Protection**: Prevents overlapping pipeline runs

## Configuration

### Default Configuration (t3.micro optimized)

```python
PipelineConfig(
    cycle_interval_minutes=15,        # Run every 15 minutes
    batch_size=4,                     # Process 4 articles at once
    max_memory_mb=300,                # Memory limit for t3.micro
    max_articles_per_symbol=12,       # Articles per symbol per cycle
    hours_back=6,                     # Look back 6 hours for news
    quality_threshold=0.7,            # Quality filter threshold
    finbert_batch_size=4,             # FinBERT batch size
    min_confidence_threshold=0.6,     # Minimum FinBERT confidence
    enable_xgboost=True,              # Enable recommendations
    recommendation_threshold=0.65,    # Minimum recommendation confidence
    enable_caching=True,              # Enable news caching
    cache_ttl_minutes=30              # Cache timeout
)
```

### Custom Configurations

#### High Performance Mode
- **Use Case**: Larger instances with more resources
- **Settings**: Larger batches, more articles, higher thresholds
- **Memory**: 500MB+ recommended

#### Low Resource Mode
- **Use Case**: Minimal resource usage
- **Settings**: Smaller batches, fewer articles, longer intervals
- **Memory**: 200MB limit

#### Testing Mode
- **Use Case**: Development and testing
- **Settings**: Fast cycles, reduced data, lower thresholds
- **Memory**: Minimal usage

## Integration with Existing Components

### Decision Engine Integration

The enhanced pipeline sends recommendations directly to your decision engine:

```python
# Message format sent to decision engine
{
    'type': 'TRADING_RECOMMENDATION',
    'priority': 'HIGH' | 'MEDIUM',
    'payload': {
        'symbol': 'AAPL',
        'recommendation': {
            'action': 'BUY',
            'confidence': 0.85,
            'reasoning': 'Positive news sentiment (0.75), high news volume, high-quality sources',
            'risk_score': 0.25,
            'expected_return': 0.03,
            'time_horizon': '1-4 hours'
        },
        'supporting_data': {
            'news_volume': 15,
            'sentiment_count': 8,
            'data_quality': 0.88
        }
    }
}
```

### Process Manager Integration

The pipeline integrates seamlessly with your existing process manager:

```python
# Automatic registration in orchestrator
processes = [
    ('decision_engine', MarketDecisionEngine),
    ('position_health_monitor', PositionHealthMonitor),
    ('strategy_analyzer', StrategyAnalyzerProcess),
    ('sentiment_analyzer', FinBERTSentimentProcess),
    ('enhanced_news_pipeline', EnhancedNewsPipelineProcess)  # New addition
]
```

### Monitoring Integration

The pipeline provides comprehensive metrics through your existing monitoring system:

- **Processing Metrics**: Articles processed, recommendations generated, processing time
- **Quality Metrics**: Average confidence, data quality scores, error rates
- **Resource Metrics**: Memory usage, CPU usage
- **Symbol Tracking**: Individual performance per symbol

## Usage Examples

### Basic Usage

```python
from Friren_V1.trading_engine.portfolio_manager.py.orchestrator import create_main_orchestrator, SystemConfig, TradingMode

# Create system configuration
config = SystemConfig(
    trading_mode=TradingMode.PAPER_TRADING,
    symbols=['AAPL', 'MSFT', 'GOOGL', 'TSLA', 'NVDA'],
    max_positions=5,
    portfolio_value=100000.0
)

# Create and start orchestrator (includes enhanced pipeline)
orchestrator = create_main_orchestrator(config)
orchestrator.initialize_system()
orchestrator.start_system()
```

### Standalone Pipeline Usage

```python
from Friren_V1.trading_engine.portfolio_manager.py.processes.enhanced_news_pipeline_process import (
    create_enhanced_news_pipeline_process,
    get_default_pipeline_config
)

# Create custom configuration
config = get_default_pipeline_config()
config.cycle_interval_minutes = 10  # Faster updates

# Create pipeline
pipeline = create_enhanced_news_pipeline_process(
    watchlist_symbols=['AAPL', 'MSFT', 'GOOGL'],
    config=config
)

# Initialize and run
pipeline._initialize()
pipeline._process_cycle()

# Get results
info = pipeline.get_process_info()
print(f"Processed {info['metrics']['articles_processed']} articles")
print(f"Generated {info['metrics']['recommendations_generated']} recommendations")
```

### Custom Configuration

```python
from Friren_V1.trading_engine.portfolio_manager.py.processes.enhanced_news_pipeline_process import PipelineConfig

# High-performance configuration
high_perf_config = PipelineConfig(
    cycle_interval_minutes=10,
    batch_size=8,
    max_articles_per_symbol=20,
    enable_xgboost=True,
    recommendation_threshold=0.7
)

# Low-resource configuration
low_resource_config = PipelineConfig(
    cycle_interval_minutes=30,
    batch_size=2,
    max_articles_per_symbol=6,
    enable_xgboost=True,
    recommendation_threshold=0.6
)
```

## Monitoring and Debugging

### Process Status

Check pipeline status through the orchestrator:

```python
status = orchestrator.get_system_status()
print(f"Processes: {status['processes']['healthy']}/{status['processes']['total']}")

# Check individual process
if orchestrator.process_manager:
    process_status = orchestrator.process_manager.get_process_status()
    pipeline_status = process_status.get('enhanced_news_pipeline', {})
    print(f"Pipeline healthy: {pipeline_status.get('is_healthy', False)}")
```

### Pipeline Metrics

Access detailed pipeline metrics:

```python
# Through shared state (if available)
if orchestrator.shared_state:
    metrics = orchestrator.shared_state.get('enhanced_news_pipeline_metrics')
    symbol_tracking = orchestrator.shared_state.get('enhanced_news_pipeline_symbol_tracking')

    print(f"Articles processed: {metrics['articles_processed']}")
    print(f"Average confidence: {metrics['average_confidence']:.3f}")

    for symbol, tracking in symbol_tracking.items():
        print(f"{symbol}: {tracking['recommendation_count']} recommendations")
```

### Error Tracking

Monitor pipeline errors:

```python
# Get process info for error details
info = pipeline.get_process_info()
print(f"Error count: {info['error_count']}")
print(f"Recent errors: {info['recent_errors']}")

# Performance history
perf = info['performance_history']
print(f"Cycles completed: {perf['cycles_completed']}")
print(f"Average processing time: {perf['avg_processing_time_ms']:.1f}ms")
```

## Best Practices

### Resource Management

1. **Memory Monitoring**: Keep track of memory usage to stay within t3.micro limits
2. **Batch Sizing**: Adjust batch sizes based on available resources
3. **Caching Strategy**: Use caching for frequently requested data

### Configuration Tuning

1. **Market Hours**: Ensure pipeline only runs during market hours
2. **Cycle Timing**: Balance between freshness and resource usage
3. **Quality Thresholds**: Set appropriate thresholds for your strategy

### Error Handling

1. **Process Monitoring**: Monitor process health and restart policies
2. **Graceful Degradation**: Handle component failures gracefully
3. **Logging**: Use appropriate logging levels for debugging

## Troubleshooting

### Common Issues

#### Pipeline Not Running
- **Check Market Hours**: Pipeline only runs during market hours
- **Check Configuration**: Verify cycle interval and other settings
- **Check Resources**: Ensure sufficient memory and CPU available

#### No Recommendations Generated
- **Check Quality Thresholds**: Lower thresholds if needed for testing
- **Check News Volume**: Verify news is being collected for symbols
- **Check FinBERT Confidence**: Ensure sentiment analysis is working

#### High Memory Usage
- **Reduce Batch Sizes**: Lower `batch_size` and `finbert_batch_size`
- **Reduce Articles**: Lower `max_articles_per_symbol`
- **Enable Caching**: Use caching to reduce repeated processing

### Debug Mode

Enable debug logging for detailed information:

```python
import logging
logging.basicConfig(level=logging.DEBUG)

# Run pipeline with debug logging
pipeline._process_cycle()
```

## Testing

Run the integration test suite:

```bash
python test_enhanced_pipeline_integration.py
```

This will test:
- Standalone pipeline functionality
- Orchestrator integration
- Configuration options
- Error handling

## Future Enhancements

### Planned Features

1. **Real-time Market Data Integration**: Incorporate live market data into recommendations
2. **Advanced ML Models**: Support for additional ML models beyond XGBoost
3. **Portfolio Context**: Consider current portfolio positions in recommendations
4. **Risk Integration**: Deep integration with your risk management system

### Performance Improvements

1. **Asynchronous Processing**: Further async optimization for better performance
2. **Distributed Processing**: Support for multi-instance deployments
3. **Smart Caching**: More intelligent caching strategies
4. **Resource Scaling**: Automatic resource adjustment based on load

## Conclusion

The Enhanced News Pipeline has been successfully integrated into your portfolio manager with:

✅ **Complete Integration**: Seamless integration with existing infrastructure
✅ **Resource Optimization**: Optimized for AWS t3.micro deployment
✅ **Queue Communication**: Full integration with your messaging system
✅ **Comprehensive Monitoring**: Detailed metrics and tracking
✅ **Flexible Configuration**: Multiple configuration options for different needs
✅ **Error Handling**: Robust error handling and recovery

The pipeline is now ready to provide high-quality trading recommendations every 15 minutes during market hours, enhancing your trading system's decision-making capabilities with real-time news sentiment analysis.
