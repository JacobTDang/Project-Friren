"""
Analysis Output Formatter for Friren Trading System
==================================================

Formats FinBERT sentiment analysis and XGBoost recommendation output to provide
real-time visibility into ML-based trading intelligence.

Target Formats (Dynamic Templates):
[FINBERT] {symbol}: {sentiment} (confidence: {confidence}%) | article: '{article_snippet}' | impact: {impact}
[XGBOOST] {symbol}: {action} (score: {score}) | features: sentiment={sentiment}, volume={volume}, impact={impact}
"""

from datetime import datetime
from typing import Dict, List, Optional, Any
from dataclasses import dataclass


@dataclass
class FinBERTResult:
    """Real FinBERT analysis result - NO HARDCODED DEFAULTS"""
    symbol: str
    article_id: str
    article_snippet: str
    sentiment: str                # POSITIVE/NEGATIVE/NEUTRAL from actual FinBERT model
    confidence: float            # Real model confidence (0-100) - NO FALLBACKS
    impact_score: float          # Market-calculated impact (0-1) - NO DEFAULTS
    processing_time_ms: float    # Actual processing time measurement
    model_version: str           # Real FinBERT model version identifier
    model_probability: Dict[str, float]  # Raw model output probabilities
    financial_keywords: List[str]       # Extracted financial terms from content
    market_correlation: float           # Real-time market correlation calculation


@dataclass
class XGBoostFeatures:
    """Real XGBoost features - 100% REAL MARKET DATA"""
    symbol: str
    sentiment_score: float        # Weighted real FinBERT scores (NO FALLBACKS)
    article_volume: int          # Actual article count from news APIs
    impact_score: float          # Market-calculated impact from price correlation
    recency_factor: float        # Time-decay based on market hours (REAL)
    source_reliability: float    # Historical source accuracy (PERFORMANCE-BASED)
    market_correlation: float    # Real-time correlation with symbol price movement
    volatility_factor: float     # Live volatility from Yahoo Finance
    volume_anomaly: float        # Trading volume deviation from historical
    sector_sentiment: float      # Real sector-wide sentiment aggregation
    earnings_proximity: float    # Days to/from actual earnings (NO ESTIMATES)


class AnalysisFormatter:
    """Standardized analysis output formatter for ML-based trading intelligence"""
    
    @staticmethod
    def format_finbert_result(symbol: str, sentiment: str, confidence: float,
                             article_snippet: str, impact: float) -> str:
        """
        Format FinBERT sentiment analysis result
        
        Args:
            symbol: Stock symbol (e.g., 'AAPL')
            sentiment: Real sentiment from FinBERT model (POSITIVE/NEGATIVE/NEUTRAL)
            confidence: Actual model confidence percentage (0-100)
            article_snippet: First 40 characters of analyzed article
            impact: Real market impact score (0-1) calculated from price correlation
            
        Returns:
            Formatted FinBERT analysis string matching exact target format
        """
        # Truncate article snippet to 40 characters for display
        if len(article_snippet) > 40:
            snippet = article_snippet[:40] + "..."
        else:
            snippet = article_snippet
            
        return f"[FINBERT] {symbol}: {sentiment} (confidence: {confidence:.1f}%) | article: '{snippet}' | impact: {impact:.2f}"
    
    @staticmethod
    def format_xgboost_recommendation(symbol: str, action: str, score: float,
                                    features: Dict[str, float]) -> str:
        """
        Format XGBoost trading recommendation
        
        Args:
            symbol: Stock symbol
            action: Recommended action (BUY/SELL/HOLD)
            score: Real XGBoost model score (0-1)
            features: Key features used in prediction with real values
            
        Returns:
            Formatted XGBoost recommendation matching exact standard format
        """
        return f"[XGBOOST] XGBoost decision: {action} {symbol} (confidence: {score:.3f})"
    
    @staticmethod
    def format_sentiment_aggregation(symbol: str, articles_analyzed: int,
                                   overall_sentiment: str, average_confidence: float,
                                   sentiment_distribution: Dict[str, int]) -> str:
        """
        Format sentiment aggregation across multiple articles
        
        Args:
            symbol: Stock symbol
            articles_analyzed: Number of articles processed
            overall_sentiment: Aggregated sentiment result
            average_confidence: Average confidence across all articles
            sentiment_distribution: Count of each sentiment type
            
        Returns:
            Formatted sentiment aggregation summary
        """
        dist_str = ", ".join([f"{k}: {v}" for k, v in sentiment_distribution.items()])
        return f"[SENTIMENT AGGREGATION] {symbol}: {overall_sentiment} from {articles_analyzed} articles (avg confidence: {average_confidence:.1f}%) | distribution: {dist_str}"
    
    @staticmethod
    def format_model_confidence(symbol: str, model_name: str, confidence: float,
                              prediction: str, model_metadata: Dict[str, Any]) -> str:
        """
        Format model confidence and metadata
        
        Args:
            symbol: Stock symbol
            model_name: Name of the ML model (FinBERT, XGBoost, etc.)
            confidence: Model confidence score
            prediction: Model prediction/output
            model_metadata: Additional model information
            
        Returns:
            Formatted model confidence information
        """
        version = model_metadata.get('version', 'unknown')
        processing_time = model_metadata.get('processing_time_ms', 0)
        return f"[{model_name.upper()} CONFIDENCE] {symbol}: {prediction} (confidence: {confidence:.1f}%) | version: {version} | time: {processing_time:.0f}ms"
    
    @staticmethod
    def format_feature_importance(symbol: str, model_type: str,
                                 feature_importance: Dict[str, float]) -> str:
        """
        Format feature importance analysis
        
        Args:
            symbol: Stock symbol
            model_type: Type of model (XGBoost, etc.)
            feature_importance: Dictionary of feature names and importance scores
            
        Returns:
            Formatted feature importance analysis
        """
        # Sort features by importance (descending)
        sorted_features = sorted(feature_importance.items(), key=lambda x: x[1], reverse=True)
        top_features = sorted_features[:5]  # Show top 5 features
        feature_str = ", ".join([f"{k}: {v:.3f}" for k, v in top_features])
        return f"[{model_type.upper()} FEATURES] {symbol}: top features: {feature_str}"
    
    @staticmethod
    def format_prediction_explanation(symbol: str, prediction: str, 
                                    reasoning: List[str], confidence: float) -> str:
        """
        Format detailed prediction explanation
        
        Args:
            symbol: Stock symbol
            prediction: Model prediction (BUY/SELL/HOLD)
            reasoning: List of reasoning factors
            confidence: Prediction confidence
            
        Returns:
            Formatted prediction explanation
        """
        reasoning_str = "; ".join(reasoning[:3])  # Show top 3 reasons
        return f"[PREDICTION EXPLANATION] {symbol}: {prediction} (confidence: {confidence:.1f}%) | reasoning: {reasoning_str}"
    
    @staticmethod
    def format_model_performance(symbol: str, model_name: str,
                               accuracy: float, precision: float, recall: float) -> str:
        """
        Format model performance metrics
        
        Args:
            symbol: Stock symbol
            model_name: Name of the model
            accuracy: Model accuracy
            precision: Model precision
            recall: Model recall
            
        Returns:
            Formatted model performance metrics
        """
        return f"[{model_name.upper()} PERFORMANCE] {symbol}: accuracy: {accuracy:.1f}% | precision: {precision:.1f}% | recall: {recall:.1f}%"
    
    @staticmethod
    def format_analysis_error(symbol: str, model_name: str, error_type: str,
                            error_message: str, retry_count: int) -> str:
        """
        Format analysis errors
        
        Args:
            symbol: Stock symbol
            model_name: Name of the model that failed
            error_type: Type of error (MODEL_ERROR, DATA_ERROR, etc.)
            error_message: Error description
            retry_count: Number of retries attempted
            
        Returns:
            Formatted error message
        """
        return f"[{model_name.upper()} ERROR] {symbol}: {error_type} - {error_message} | retries: {retry_count}"
    
    @staticmethod
    def format_analysis_metrics(symbol: str, metrics: Dict[str, Any]) -> str:
        """
        Format comprehensive analysis metrics
        
        Args:
            symbol: Stock symbol
            metrics: Dictionary containing analysis metrics
            
        Returns:
            Formatted metrics summary
        """
        # ZERO-HARDCODING: All metrics must be provided by caller - NO DEFAULTS
        total_predictions = metrics['total_predictions']
        success_rate = metrics['success_rate']
        avg_confidence = metrics['avg_confidence']
        avg_processing_time = metrics['avg_processing_time_ms']
        
        return f"[ANALYSIS METRICS] {symbol}: {total_predictions} predictions | {success_rate:.1f}% success | {avg_confidence:.1f}% avg confidence | {avg_processing_time:.0f}ms avg"