"""
Enhanced News Pipeline Modules Package
=====================================

Modular components for the enhanced news pipeline process, refactored for
better maintainability and separation of concerns.

Modules:
--------
- news_collection_module: News collection and terminal bridge functionality
- finbert_analysis_module: FinBERT sentiment analysis and impact calculation  
- xgboost_recommendation_module: XGBoost-based trading recommendations
- pipeline_orchestrator: Main process coordination and workflow management

Usage:
------
```python
from .pipeline_orchestrator import EnhancedNewsPipelineOrchestrator
from .news_collection_module import NewsCollectionModule
from .finbert_analysis_module import FinBERTAnalysisModule
from .xgboost_recommendation_module import XGBoostRecommendationModule
```
"""

from .news_collection_module import (
    NewsCollectionModule,
    NewsCollectionConfig,
    CollectionMetrics,
    send_colored_business_output
)

from .finbert_analysis_module import (
    FinBERTAnalysisModule,
    FinBERTConfig,
    AnalysisMetrics
)

from .xgboost_recommendation_module import (
    XGBoostRecommendationModule,
    XGBoostConfig,
    RecommendationMetrics,
    TradingAction
)

from .pipeline_orchestrator import (
    EnhancedNewsPipelineOrchestrator,
    EnhancedNewsPipelineProcess,  # Backward compatibility alias
    PipelineConfig,
    PipelineMetrics,
    MarketRegime
)

__all__ = [
    # Main orchestrator
    'EnhancedNewsPipelineOrchestrator',
    'EnhancedNewsPipelineProcess',
    
    # Individual modules
    'NewsCollectionModule',
    'FinBERTAnalysisModule', 
    'XGBoostRecommendationModule',
    
    # Configuration classes
    'PipelineConfig',
    'NewsCollectionConfig',
    'FinBERTConfig',
    'XGBoostConfig',
    
    # Metrics classes
    'PipelineMetrics',
    'CollectionMetrics',
    'AnalysisMetrics',
    'RecommendationMetrics',
    
    # Utility classes
    'MarketRegime',
    'TradingAction',
    'send_colored_business_output'
]