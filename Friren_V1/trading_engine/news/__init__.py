"""
Enhanced News Pipeline Module for Friren Trading System
======================================================

This module contains the refactored news pipeline components, breaking down
the monolithic enhanced_news_pipeline_process.py into manageable, modular
components for better maintainability and memory optimization.

Module Structure:
- collectors/: News collection from multiple sources
- analysis/: FinBERT sentiment analysis and aggregation  
- recommendations/: XGBoost recommendation engine
- pipeline_coordinator.py: Orchestrates all components

All components follow the zero-hardcoding principle and use real market data only.
"""

# Import only what exists to avoid import errors
try:
    from .recommendations.xgboost_engine import XGBoostRecommendationEngine, create_xgboost_engine
    from .recommendations.feature_engineer import FeatureEngineer, create_feature_engineer  
    from .recommendations.recommendation_validator import RecommendationValidator, create_recommendation_validator
    RECOMMENDATIONS_AVAILABLE = True
except ImportError:
    RECOMMENDATIONS_AVAILABLE = False

__all__ = []

if RECOMMENDATIONS_AVAILABLE:
    __all__.extend([
        'XGBoostRecommendationEngine',
        'FeatureEngineer',
        'RecommendationValidator',
        'create_xgboost_engine',
        'create_feature_engineer', 
        'create_recommendation_validator'
    ])