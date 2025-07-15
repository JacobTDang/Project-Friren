"""
News Analysis Package - Modular FinBERT sentiment analysis components

This package provides modular components for FinBERT sentiment analysis,
extracted from enhanced_news_pipeline_process.py for better maintainability
and reusability.

Components:
- FinBERTProcessor: Core FinBERT sentiment analysis with lazy loading
- SentimentAggregator: Multi-article sentiment aggregation and consensus analysis
- ImpactCalculator: Market impact scoring and risk assessment

Key Features:
- Real data usage (no mock/placeholder implementations)
- OutputCoordinator integration for standardized output
- Memory optimization with lazy loading
- Comprehensive error handling
- Modular design for easy testing and maintenance
"""

from .finbert_processor import (
    FinBERTProcessor,
    EnhancedSentimentResult,
    SentimentLabel,
    create_finbert_processor
)

from .sentiment_aggregator import (
    SentimentAggregator,
    AggregatedSentimentResult,
    create_sentiment_aggregator
)

from .impact_calculator import (
    ImpactCalculator,
    MarketImpactResult,
    RiskLevel,
    create_impact_calculator
)

__all__ = [
    # FinBERT Processor
    'FinBERTProcessor',
    'EnhancedSentimentResult',
    'SentimentLabel',
    'create_finbert_processor',
    
    # Sentiment Aggregator
    'SentimentAggregator',
    'AggregatedSentimentResult',
    'create_sentiment_aggregator',
    
    # Impact Calculator
    'ImpactCalculator',
    'MarketImpactResult',
    'RiskLevel',
    'create_impact_calculator'
]

# Version info
__version__ = '1.0.0'
__author__ = 'Project Friren'
__description__ = 'Modular FinBERT sentiment analysis components'