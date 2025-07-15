"""
XGBoost Recommendation Module for Enhanced News Pipeline
=======================================================

Handles XGBoost-based trading recommendations using sentiment analysis results
and market data. This module provides intelligent trading recommendations
with confidence scoring and risk assessment.

Extracted from enhanced_news_pipeline_process.py for better maintainability.
"""

import time
import logging
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from enum import Enum

# Import XGBoost recommendation components
from Friren_V1.trading_engine.news.recommendations import (
    create_xgboost_engine, 
    XGBoostRecommendationEngine,
    TradingRecommendation
)
from Friren_V1.trading_engine.data.news.analysis import EnhancedSentimentResult
from Friren_V1.trading_engine.analytics.market_metrics import get_market_metrics, MarketMetricsResult
from Friren_V1.trading_engine.output.output_coordinator import OutputCoordinator


@dataclass
class XGBoostConfig:
    """Configuration for XGBoost recommendations - PRODUCTION CONFIG MANAGED"""
    model_path: str = field(default_factory=lambda: _get_xgboost_model_path())
    enable_xgboost: bool = True
    recommendation_threshold: float = field(default_factory=lambda: _get_xgboost_threshold('recommendation'))
    buy_threshold: float = field(default_factory=lambda: _get_xgboost_threshold('buy'))
    sell_threshold: float = field(default_factory=lambda: _get_xgboost_threshold('sell'))
    max_recommendations_per_symbol: int = 3
    feature_importance_threshold: float = 0.1


def _get_xgboost_model_path() -> str:
    """Get XGBoost model path from configuration - NO HARDCODED FALLBACKS"""
    try:
        from Friren_V1.infrastructure.configuration_manager import get_config
        model_path = get_config('XGBOOST_MODEL_PATH')
        if not model_path:
            raise ValueError("PRODUCTION: XGBOOST_MODEL_PATH must be configured")
        return str(model_path)
    except ImportError:
        raise ImportError("CRITICAL: Configuration manager required. No hardcoded model paths allowed.")


def _get_xgboost_threshold(threshold_type: str) -> float:
    """Get XGBoost threshold from configuration - NO HARDCODED FALLBACKS"""
    try:
        from Friren_V1.infrastructure.configuration_manager import get_config
        
        if threshold_type == 'recommendation':
            threshold = get_config('XGBOOST_RECOMMENDATION_THRESHOLD')
        elif threshold_type == 'buy':
            threshold = get_config('XGBOOST_BUY_THRESHOLD')
        elif threshold_type == 'sell':
            threshold = get_config('XGBOOST_SELL_THRESHOLD')
        else:
            raise ValueError(f"Unknown threshold type: {threshold_type}")
        
        if threshold is None:
            raise ValueError(f"PRODUCTION: XGBOOST_{threshold_type.upper()}_THRESHOLD must be configured")
        return float(threshold)
    except ImportError:
        raise ImportError("CRITICAL: Configuration manager required. No hardcoded thresholds allowed.")


@dataclass
class RecommendationMetrics:
    """XGBoost recommendation metrics"""
    recommendations_generated: int = 0
    symbols_processed: int = 0
    recommendation_time_ms: float = 0.0
    average_confidence: float = 0.0
    buy_recommendations: int = 0
    sell_recommendations: int = 0
    hold_recommendations: int = 0
    high_confidence_recommendations: int = 0
    errors: int = 0
    model_load_time_ms: float = 0.0
    timestamp: datetime = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()


class TradingAction(Enum):
    """Trading action recommendations"""
    BUY = "BUY"
    SELL = "SELL"
    HOLD = "HOLD"


class XGBoostRecommendationModule:
    """
    XGBoost Recommendation Module
    
    Generates intelligent trading recommendations using XGBoost machine learning
    models, incorporating sentiment analysis results, market metrics, and
    technical indicators for comprehensive trading decisions.
    """
    
    def __init__(self, config: Optional[XGBoostConfig] = None,
                 output_coordinator: Optional[OutputCoordinator] = None,
                 process_id: str = "xgboost_recommendation"):
        """
        Initialize XGBoost recommendation module
        
        Args:
            config: XGBoost configuration
            output_coordinator: Output coordinator for standardized output
            process_id: Process identifier for output
        """
        self.config = config or XGBoostConfig()
        self.output_coordinator = output_coordinator
        self.process_id = process_id
        self.logger = logging.getLogger(__name__)
        
        # Core components
        self.recommendation_engine: Optional[XGBoostRecommendationEngine] = None
        
        # State tracking
        self.last_recommendation_time: Optional[datetime] = None
        self.recommendation_cache: Dict[str, List[TradingRecommendation]] = {}
        self.metrics = RecommendationMetrics()
        
        # Model state
        self.model_loaded = False
        self.model_load_time: Optional[datetime] = None
        
        # Performance tracking
        self.recommendation_history = []
        self.error_count = 0
        self.last_error_time: Optional[datetime] = None
    
    def initialize(self) -> bool:
        """
        Initialize XGBoost recommendation components
        
        Returns:
            True if initialization successful
        """
        try:
            self.logger.info("Initializing XGBoost recommendation module...")
            
            if not self.config.enable_xgboost:
                self.logger.info("XGBoost disabled in configuration, skipping initialization")
                return True
            
            # Validate model path
            if not self._validate_model_path():
                self.logger.error(f"XGBoost model not found at {self.config.model_path}")
                return False
            
            # Initialize recommendation engine
            start_time = time.time()
            self.recommendation_engine = create_xgboost_engine(self.config.model_path)
            
            if self.recommendation_engine:
                self.model_loaded = True
                self.model_load_time = datetime.now()
                load_time = (time.time() - start_time) * 1000
                self.metrics.model_load_time_ms = load_time
                
                self.logger.info(f"XGBoost recommendation engine initialized successfully in {load_time:.1f}ms")
                return True
            else:
                self.logger.error("Failed to create XGBoost recommendation engine")
                return False
            
        except Exception as e:
            self.logger.error(f"Failed to initialize XGBoost recommendation module: {e}")
            self.error_count += 1
            self.last_error_time = datetime.now()
            return False
    
    def generate_recommendations(self, sentiment_data: Dict[str, List[EnhancedSentimentResult]]) -> Dict[str, List[TradingRecommendation]]:
        """
        Generate trading recommendations based on sentiment analysis
        
        Args:
            sentiment_data: Dictionary mapping symbols to sentiment results
            
        Returns:
            Dictionary mapping symbols to trading recommendations
        """
        start_time = time.time()
        recommendations = {}
        
        try:
            if not self.config.enable_xgboost or not self.recommendation_engine:
                self.logger.warning("XGBoost engine not available, skipping recommendations")
                return {}
            
            self.logger.info(f"Generating XGBoost recommendations for {len(sentiment_data)} symbols")
            
            # Process each symbol
            for symbol, sentiment_results in sentiment_data.items():
                try:
                    if not sentiment_results:
                        recommendations[symbol] = []
                        continue
                    
                    # Generate recommendations for this symbol
                    symbol_recommendations = self._generate_symbol_recommendations(symbol, sentiment_results)
                    recommendations[symbol] = symbol_recommendations
                    
                    # Output recommendations using OutputCoordinator
                    if symbol_recommendations and self.output_coordinator:
                        for recommendation in symbol_recommendations:
                            self._output_recommendation_result(symbol, recommendation)
                    
                except Exception as e:
                    self.logger.error(f"Error generating recommendations for {symbol}: {e}")
                    self.metrics.errors += 1
                    recommendations[symbol] = []
            
            # Update metrics
            recommendation_time = (time.time() - start_time) * 1000
            self.metrics.recommendation_time_ms = recommendation_time
            self.metrics.symbols_processed = len(sentiment_data)
            self.metrics.recommendations_generated = sum(len(recs) for recs in recommendations.values())
            self.last_recommendation_time = datetime.now()
            
            # Calculate recommendation distribution
            all_recommendations = [rec for recs in recommendations.values() for rec in recs]
            if all_recommendations:
                self.metrics.average_confidence = sum(r.confidence for r in all_recommendations) / len(all_recommendations)
                self.metrics.buy_recommendations = sum(1 for r in all_recommendations if r.action == TradingAction.BUY.value)
                self.metrics.sell_recommendations = sum(1 for r in all_recommendations if r.action == TradingAction.SELL.value)
                self.metrics.hold_recommendations = sum(1 for r in all_recommendations if r.action == TradingAction.HOLD.value)
                self.metrics.high_confidence_recommendations = sum(1 for r in all_recommendations if r.confidence > 0.8)
            
            self.logger.info(f"XGBoost recommendations completed: {self.metrics.recommendations_generated} recommendations in {recommendation_time:.1f}ms")
            
            return recommendations
            
        except Exception as e:
            self.logger.error(f"XGBoost recommendation generation failed: {e}")
            self.error_count += 1
            self.last_error_time = datetime.now()
            return {}
    
    def _generate_symbol_recommendations(self, symbol: str, sentiment_results: List[EnhancedSentimentResult]) -> List[TradingRecommendation]:
        """
        Generate recommendations for a specific symbol
        
        Args:
            symbol: Stock symbol
            sentiment_results: List of sentiment analysis results
            
        Returns:
            List of trading recommendations
        """
        try:
            if not self.recommendation_engine:
                return []
            
            recommendations = []
            
            # Prepare features from sentiment results
            features = self._prepare_features(symbol, sentiment_results)
            if not features:
                self.logger.debug(f"No features available for {symbol}")
                return []
            
            # Get market metrics for additional context
            market_metrics = self._get_market_metrics(symbol)
            
            # Generate recommendation using XGBoost engine
            recommendation = self.recommendation_engine.generate_recommendation(
                symbol=symbol,
                sentiment_results=sentiment_results,
                market_data=market_metrics,
                features=features
            )
            
            if recommendation:
                # Apply thresholds and filters
                if self._validate_recommendation(recommendation):
                    recommendations.append(recommendation)
                    self.logger.debug(f"Generated {recommendation.action} recommendation for {symbol} (confidence: {recommendation.confidence:.2f})")
            
            return recommendations[:self.config.max_recommendations_per_symbol]
            
        except Exception as e:
            self.logger.error(f"Error generating recommendations for {symbol}: {e}")
            return []
    
    def _prepare_features(self, symbol: str, sentiment_results: List[EnhancedSentimentResult]) -> Dict[str, float]:
        """
        Prepare features for XGBoost model from sentiment results
        
        Args:
            symbol: Stock symbol
            sentiment_results: List of sentiment results
            
        Returns:
            Feature dictionary for model input
        """
        try:
            if not sentiment_results:
                return {}
            
            features = {}
            
            # Sentiment aggregation features
            positive_count = sum(1 for r in sentiment_results if r.sentiment_label.value.lower() == 'positive')
            negative_count = sum(1 for r in sentiment_results if r.sentiment_label.value.lower() == 'negative')
            neutral_count = sum(1 for r in sentiment_results if r.sentiment_label.value.lower() == 'neutral')
            total_count = len(sentiment_results)
            
            features['sentiment_positive_ratio'] = positive_count / total_count if total_count > 0 else 0
            features['sentiment_negative_ratio'] = negative_count / total_count if total_count > 0 else 0
            features['sentiment_neutral_ratio'] = neutral_count / total_count if total_count > 0 else 0
            
            # Confidence features
            confidences = [r.confidence for r in sentiment_results]
            features['avg_confidence'] = sum(confidences) / len(confidences) if confidences else 0
            features['max_confidence'] = max(confidences) if confidences else 0
            features['min_confidence'] = min(confidences) if confidences else 0
            
            # Impact features
            impact_scores = [r.impact_score for r in sentiment_results]
            features['avg_impact_score'] = sum(impact_scores) / len(impact_scores) if impact_scores else 0
            features['max_impact_score'] = max(impact_scores) if impact_scores else 0
            
            # Volume features
            features['article_count'] = total_count
            features['high_confidence_count'] = sum(1 for r in sentiment_results if r.confidence > 0.8)
            features['high_impact_count'] = sum(1 for r in sentiment_results if r.impact_score > 0.7)
            
            # Time-based features
            now = datetime.now()
            recent_articles = sum(1 for r in sentiment_results 
                                if (now - r.published_date).total_seconds() < 3600)  # Last hour
            features['recent_article_ratio'] = recent_articles / total_count if total_count > 0 else 0
            
            # Sentiment momentum (trend)
            if len(sentiment_results) >= 3:
                sorted_results = sorted(sentiment_results, key=lambda x: x.published_date)
                recent_sentiment = sum(1 if r.sentiment_label.value.lower() == 'positive' else -1 
                                     for r in sorted_results[-3:])
                features['sentiment_momentum'] = recent_sentiment / 3
            else:
                features['sentiment_momentum'] = 0
            
            # Source reliability
            source_scores = [r.metadata.get('source_reliability', 0.6) for r in sentiment_results if r.metadata]
            features['avg_source_reliability'] = sum(source_scores) / len(source_scores) if source_scores else 0.6
            
            return features
            
        except Exception as e:
            self.logger.error(f"Error preparing features for {symbol}: {e}")
            return {}
    
    def _get_market_metrics(self, symbol: str) -> Optional[MarketMetricsResult]:
        """
        Get market metrics for the symbol
        
        Args:
            symbol: Stock symbol
            
        Returns:
            Market metrics result or None
        """
        try:
            return get_market_metrics(symbol)
        except Exception as e:
            self.logger.debug(f"Error getting market metrics for {symbol}: {e}")
            return None
    
    def _validate_recommendation(self, recommendation: TradingRecommendation) -> bool:
        """
        Validate recommendation against thresholds and filters
        
        Args:
            recommendation: Trading recommendation to validate
            
        Returns:
            True if recommendation passes validation
        """
        try:
            # Check minimum confidence threshold
            if recommendation.confidence < self.config.recommendation_threshold:
                return False
            
            # Check action-specific thresholds
            if recommendation.action == TradingAction.BUY.value:
                return recommendation.prediction_score >= self.config.buy_threshold
            elif recommendation.action == TradingAction.SELL.value:
                return recommendation.prediction_score <= self.config.sell_threshold
            
            # HOLD recommendations always pass if above general threshold
            return True
            
        except Exception as e:
            self.logger.debug(f"Error validating recommendation: {e}")
            return False
    
    def _validate_model_path(self) -> bool:
        """
        Validate that the XGBoost model file exists
        
        Returns:
            True if model file exists and is accessible
        """
        try:
            return os.path.exists(self.config.model_path) and os.path.isfile(self.config.model_path)
        except Exception:
            return False
    
    def _output_recommendation_result(self, symbol: str, recommendation: TradingRecommendation):
        """
        Output recommendation result using OutputCoordinator
        
        Args:
            symbol: Stock symbol
            recommendation: Trading recommendation
        """
        try:
            if self.output_coordinator:
                # Prepare key features for output
                features = {
                    'confidence': recommendation.confidence,
                    'prediction_score': recommendation.prediction_score,
                    'risk_score': recommendation.risk_score,
                    'expected_return': recommendation.expected_return
                }
                
                # Output using standardized format
                self.output_coordinator.output_xgboost_recommendation(
                    symbol=symbol,
                    action=recommendation.action,
                    score=recommendation.prediction_score,
                    features=features
                )
            
        except Exception as e:
            self.logger.warning(f"Error outputting recommendation result for {symbol}: {e}")
    
    def get_recommendation_summary(self, recommendations: Dict[str, List[TradingRecommendation]]) -> Dict[str, Any]:
        """
        Get summary statistics for generated recommendations
        
        Args:
            recommendations: Dictionary of recommendations by symbol
            
        Returns:
            Summary statistics dictionary
        """
        try:
            all_recommendations = [rec for recs in recommendations.values() for rec in recs]
            
            if not all_recommendations:
                return {"total_recommendations": 0}
            
            summary = {
                "total_recommendations": len(all_recommendations),
                "symbols_with_recommendations": len([s for s, recs in recommendations.items() if recs]),
                "average_confidence": sum(r.confidence for r in all_recommendations) / len(all_recommendations),
                "action_distribution": {
                    "BUY": sum(1 for r in all_recommendations if r.action == TradingAction.BUY.value),
                    "SELL": sum(1 for r in all_recommendations if r.action == TradingAction.SELL.value),
                    "HOLD": sum(1 for r in all_recommendations if r.action == TradingAction.HOLD.value)
                },
                "high_confidence_count": sum(1 for r in all_recommendations if r.confidence > 0.8),
                "average_expected_return": sum(r.expected_return for r in all_recommendations) / len(all_recommendations),
                "average_risk_score": sum(r.risk_score for r in all_recommendations) / len(all_recommendations)
            }
            
            return summary
            
        except Exception as e:
            self.logger.error(f"Error generating recommendation summary: {e}")
            return {"error": str(e)}
    
    def get_metrics(self) -> RecommendationMetrics:
        """
        Get current recommendation metrics
        
        Returns:
            Current metrics snapshot
        """
        return self.metrics
    
    def clear_cache(self):
        """Clear the recommendation cache"""
        self.recommendation_cache.clear()
        self.logger.info("XGBoost recommendation cache cleared")
    
    def cleanup(self):
        """Cleanup module resources"""
        try:
            self.clear_cache()
            if self.recommendation_engine and hasattr(self.recommendation_engine, 'cleanup'):
                self.recommendation_engine.cleanup()
            self.logger.info("XGBoost recommendation module cleanup completed")
        except Exception as e:
            self.logger.error(f"Error during XGBoost recommendation cleanup: {e}")