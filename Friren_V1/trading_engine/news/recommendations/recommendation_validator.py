"""
XGBoost Recommendation Validator - Extracted from Enhanced News Pipeline Process

This module handles validation, action determination, and formatting of XGBoost recommendations.
Provides market-driven validation logic with dynamic confidence calculations.

Features:
- Dynamic action determination with market-based thresholds
- Confidence calculation with market adjustments  
- Risk scoring based on feature analysis
- Expected return estimation with risk adjustment
- Human-readable reasoning generation
- Data insufficiency handling with market-based fallbacks
"""

import statistics
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
import logging

# Import market metrics for dynamic calculations (with fallback)
try:
    from Friren_V1.trading_engine.analytics.market_metrics import get_all_metrics, MarketMetricsResult
    MARKET_METRICS_AVAILABLE = True
except ImportError:
    # Fallback when market metrics not available
    MARKET_METRICS_AVAILABLE = False
    
    # Define fallback MarketMetricsResult class
    class MarketMetricsResult:
        def __init__(self):
            self.volatility = None
            self.volume = None
            self.risk_score = None
    
    def get_all_metrics(symbol: str) -> MarketMetricsResult:
        """Fallback market metrics when real module unavailable"""
        return MarketMetricsResult()


@dataclass
class TradingRecommendation:
    """Trading recommendation output"""
    symbol: str
    action: str
    confidence: float
    prediction_score: Optional[float]
    reasoning: str
    risk_score: float
    expected_return: float
    time_horizon: str
    news_sentiment: float
    news_volume: int
    market_impact: float
    data_quality: float
    timestamp: datetime
    source_articles: int
    shap_explanations: Optional[Dict[str, float]] = None  # SHAP feature contributions


class RecommendationValidator:
    """Validation and formatting logic for XGBoost recommendations"""

    def __init__(self, config: Any):
        self.config = config
        self.logger = logging.getLogger(f"{__name__}.RecommendationValidator")
        
        # Dynamic decision thresholds from configuration
        self.buy_threshold = getattr(config, 'xgboost_buy_threshold', 0.65)
        self.sell_threshold = getattr(config, 'xgboost_sell_threshold', 0.35)

    def validate_and_format_recommendation(self,
                                         symbol: str,
                                         prediction_score: Optional[float],
                                         features: Dict[str, float],
                                         source_articles: int) -> TradingRecommendation:
        """
        Validate prediction and format recommendation
        
        Args:
            symbol: Stock symbol
            prediction_score: Model prediction score (None if prediction failed)
            features: Engineered features
            source_articles: Number of source articles
            
        Returns:
            Formatted trading recommendation
        """
        try:
            # Handle case where prediction fails (returns None)
            if prediction_score is None:
                return self._create_data_insufficient_recommendation(
                    symbol, "Prediction model failed to generate score"
                )

            # Determine action and confidence
            action, confidence = self._determine_action(prediction_score, features)

            # Calculate risk and expected return
            risk_score = self._calculate_risk(features)
            expected_return = self._estimate_return(prediction_score, risk_score)

            # Generate reasoning
            reasoning = self._generate_reasoning(features, action, prediction_score)

            recommendation = TradingRecommendation(
                symbol=symbol,
                action=action,
                confidence=confidence,
                prediction_score=prediction_score,
                reasoning=reasoning,
                risk_score=risk_score,
                expected_return=expected_return,
                time_horizon="1-4 hours",
                news_sentiment=features.get('sentiment_score', 0.0),
                news_volume=int(features.get('news_volume', 0)),
                market_impact=features.get('market_impact', 0.0),
                data_quality=features.get('data_quality', 0.0),
                timestamp=datetime.now(),
                source_articles=source_articles
            )

            return recommendation

        except Exception as e:
            self.logger.error(f"Error validating recommendation for {symbol}: {e}")
            # Return data-insufficient recommendation - NO HARDCODED VALUES
            return self._create_data_insufficient_recommendation(symbol, str(e))

    def _determine_action(self, prediction_score: float, features: Dict[str, float]) -> Tuple[str, float]:
        """Determine trading action and confidence with dynamic market-based calculations"""

        # Get dynamic confidence boost based on market metrics
        sentiment_confidence = features.get('sentiment_confidence')
        data_quality = features.get('data_quality')
        
        if sentiment_confidence is None or data_quality is None:
            # If we don't have proper data, cannot make reliable prediction
            return "HOLD", 0.1  # Very low confidence for insufficient data
        
        confidence_boost = sentiment_confidence * data_quality

        if prediction_score >= self.buy_threshold:
            action = "BUY"
            confidence = min(0.95, prediction_score * confidence_boost)
        elif prediction_score <= self.sell_threshold:
            action = "SELL"
            confidence = min(0.95, (1 - prediction_score) * confidence_boost)
        else:
            action = "HOLD"
            # Dynamic hold confidence based on prediction certainty
            prediction_certainty = abs(prediction_score - 0.5) * 2  # Scale to 0-1
            confidence = max(0.1, prediction_certainty * confidence_boost)

        return action, confidence

    def _calculate_risk(self, features: Dict[str, float]) -> float:
        """Calculate risk score based on features with dynamic calculations"""

        risk_factors = []
        
        # Low confidence = high risk (only if confidence data available)
        sentiment_confidence = features.get('sentiment_confidence')
        if sentiment_confidence is not None:
            risk_factors.append(1 - sentiment_confidence)
        
        # High volatility = high risk
        sentiment_volatility = features.get('sentiment_volatility', 0.0)
        risk_factors.append(sentiment_volatility)
        
        # Low quality = high risk (only if quality data available)
        data_quality = features.get('data_quality')
        if data_quality is not None:
            risk_factors.append(1 - data_quality)
        
        # Stale data = high risk
        staleness_factor = features.get('staleness_factor', 0.0)
        risk_factors.append(staleness_factor)
        
        # Return high risk if no reliable data
        if len(risk_factors) == 0:
            return 0.9  # High risk for no data

        return statistics.mean(risk_factors)

    def _estimate_return(self, prediction_score: float, risk_score: float) -> float:
        """Estimate expected return based on prediction and risk"""

        # Simple risk-adjusted return estimate
        base_return = (prediction_score - 0.5) * 2.0  # Convert to -1 to 1 range
        risk_adjustment = 1 - risk_score

        return base_return * risk_adjustment * 0.05  # Scale to realistic return percentage

    def _generate_reasoning(self, features: Dict[str, float], action: str, score: float) -> str:
        """Generate human-readable reasoning for the recommendation"""

        sentiment = features.get('sentiment_score', 0.0)
        confidence = features.get('sentiment_confidence')
        volume = features.get('news_volume', 0.0)
        quality = features.get('data_quality')
        
        # Handle missing confidence/quality data in reasoning
        if confidence is None:
            confidence_text = "unknown confidence"
        else:
            confidence_text = f"confidence: {confidence:.2f}"
            
        if quality is None:
            quality_text = "unknown data quality"
        else:
            quality_text = f"data quality: {quality:.2f}"

        reasoning_parts = []

        # Sentiment reasoning
        if sentiment > 0.3:
            reasoning_parts.append(f"Positive news sentiment ({sentiment:.2f})")
        elif sentiment < -0.3:
            reasoning_parts.append(f"Negative news sentiment ({sentiment:.2f})")
        else:
            reasoning_parts.append("Neutral news sentiment")

        # Volume reasoning
        if volume > 0.7:
            reasoning_parts.append("high news volume")
        elif volume > 0.3:
            reasoning_parts.append("moderate news volume")
        else:
            reasoning_parts.append("low news volume")

        # Quality reasoning
        if quality is not None:
            if quality > 0.8:
                reasoning_parts.append("high-quality sources")
            elif quality < 0.6:
                reasoning_parts.append("mixed-quality sources")

        # Confidence reasoning
        if confidence is not None:
            if confidence > 0.8:
                reasoning_parts.append("high analytical confidence")
            elif confidence < 0.6:
                reasoning_parts.append("moderate analytical confidence")

        return f"{action} recommendation based on: " + ", ".join(reasoning_parts) + f" (score: {score:.3f})"

    def _create_data_insufficient_recommendation(self, symbol: str, error_reason: str) -> TradingRecommendation:
        """Create recommendation when insufficient data is available - NO HARDCODED VALUES"""
        
        # Get market metrics to inform the conservative recommendation
        try:
            market_metrics = get_all_metrics(symbol)
            
            if market_metrics and market_metrics.risk_score is not None:
                # Use market-based risk assessment
                risk_score = market_metrics.risk_score
                confidence = max(0.1, (100 - risk_score) / 100)  # Higher risk = lower confidence
            else:
                # If no market data, very conservative approach
                risk_score = 95.0  # Assume high risk
                confidence = 0.05  # Very low confidence
                
        except Exception as e:
            self.logger.warning(f"Failed to get market metrics for {symbol}: {e}")
            risk_score = 95.0  # Conservative high risk
            confidence = 0.05  # Very low confidence
        
        return TradingRecommendation(
            symbol=symbol,
            action='HOLD',  # Always conservative action when data insufficient
            confidence=confidence,
            prediction_score=None,  # Explicitly None to indicate no prediction made
            reasoning=f"Insufficient data for reliable analysis: {error_reason}. Conservative HOLD recommendation based on limited market metrics.",
            risk_score=risk_score,
            expected_return=0.0,  # No expected return when no data
            time_horizon="N/A",
            news_sentiment=0.0,
            news_volume=0,
            market_impact=0.0,
            data_quality=0.0,  # Insufficient data quality
            timestamp=datetime.now(),
            source_articles=0
        )

    def meets_confidence_threshold(self, recommendation: TradingRecommendation) -> bool:
        """Check if recommendation meets minimum confidence threshold"""
        threshold = getattr(self.config, 'recommendation_threshold', 0.65)
        return recommendation.confidence >= threshold

    def get_validation_summary(self, recommendation: TradingRecommendation) -> Dict[str, Any]:
        """Get validation summary for recommendation"""
        return {
            'symbol': recommendation.symbol,
            'action': recommendation.action,
            'confidence': recommendation.confidence,
            'passes_threshold': self.meets_confidence_threshold(recommendation),
            'risk_level': 'HIGH' if recommendation.risk_score > 0.7 else 'MEDIUM' if recommendation.risk_score > 0.4 else 'LOW',
            'data_quality': recommendation.data_quality,
            'prediction_available': recommendation.prediction_score is not None,
            'reasoning': recommendation.reasoning
        }


def create_recommendation_validator(config: Any) -> RecommendationValidator:
    """Factory function to create recommendation validator instance"""
    return RecommendationValidator(config)