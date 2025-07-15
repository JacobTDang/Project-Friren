"""
News-Based Recommendation Modules for Friren Trading System
===========================================================

This module contains modular XGBoost recommendation engine components extracted
from the Enhanced News Pipeline Process for better maintainability and testing.

Components:
- xgboost_engine.py: Core ML recommendation generation with model loading
- feature_engineer.py: Feature extraction from news and sentiment data
- recommendation_validator.py: Recommendation validation and formatting logic

Factory Functions:
- create_xgboost_engine(): Create XGBoost engine with OutputCoordinator integration
- create_feature_engineer(): Create feature engineering component
- create_recommendation_validator(): Create recommendation validator

All components use real ML models and market data to generate actual
trading recommendations with confidence scores and feature importance.
No hardcoded or simulated data is used - all calculations are market-driven.
"""

from .xgboost_engine import XGBoostRecommendationEngine, create_xgboost_engine
from .feature_engineer import FeatureEngineer, create_feature_engineer, EnhancedSentimentResult, ProcessedNewsData
from .recommendation_validator import RecommendationValidator, create_recommendation_validator, TradingRecommendation

__all__ = [
    # Core classes
    'XGBoostRecommendationEngine',
    'FeatureEngineer', 
    'RecommendationValidator',
    
    # Data classes
    'EnhancedSentimentResult',
    'ProcessedNewsData',
    'TradingRecommendation',
    
    # Factory functions
    'create_xgboost_engine',
    'create_feature_engineer',
    'create_recommendation_validator'
]