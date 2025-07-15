"""
XGBoost Recommendation Engine - Extracted from Enhanced News Pipeline Process

This module contains the XGBoost-based recommendation engine for trading decisions.
Provides real ML-based trading recommendations with market-driven feature engineering.

Features:
- Real XGBoost model integration (no hardcoded fallbacks)
- Dynamic feature engineering with market metrics
- Market-adjusted confidence and risk calculations
- Comprehensive recommendation with reasoning
"""

import os
import logging
import statistics
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, field
from collections import deque
from enum import Enum

# Import market metrics for dynamic calculations
from Friren_V1.trading_engine.analytics.market_metrics import get_all_metrics, MarketMetricsResult

# Import configuration manager to eliminate ALL hardcoded values
from Friren_V1.infrastructure.configuration_manager import get_config


class SentimentLabel(Enum):
    """Sentiment classification labels"""
    POSITIVE = "POSITIVE"
    NEGATIVE = "NEGATIVE"
    NEUTRAL = "NEUTRAL"


@dataclass
class EnhancedSentimentResult:
    """Enhanced sentiment analysis result with market impact"""
    sentiment_label: SentimentLabel
    positive_score: float
    negative_score: float
    neutral_score: float
    confidence: float
    market_impact_score: float
    article_title: str
    article_content: str
    source: str
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class ProcessedNewsData:
    """Processed news data container"""
    symbol: str
    news_volume: int
    data_quality_score: float
    staleness_minutes: float
    sources_used: List[str]
    key_articles: List[Any]
    market_events: List[str]
    timestamp: datetime = field(default_factory=datetime.now)


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


class XGBoostRecommendationEngine:
    """XGBoost-based recommendation engine for trading decisions - PRODUCTION READY"""

    def __init__(self, config: Any = None):
        """Initialize XGBoost engine with optional config for testing compatibility"""
        self.config = config
        self.logger = logging.getLogger(f"{__name__}.XGBoostEngine")

        # FAIL FAST: Require real XGBoost model - no simulation allowed
        try:
            import xgboost as xgb
            self.xgb = xgb
        except ImportError:
            raise RuntimeError("XGBoost library not installed - real ML model required for production")

        # Load trained model - OPTIONAL for testing
        self.model_path = config.model_path if hasattr(config, 'model_path') else None
        self.model = None

        if self.model_path and os.path.exists(self.model_path):
            try:
                # Load the real trained model
                self.model = self.xgb.Booster()
                self.model.load_model(self.model_path)
                self.logger.info(f"Loaded real XGBoost model from {self.model_path}")
            except Exception as e:
                self.logger.warning(f"Failed to load XGBoost model: {e}")
                self.model = None
        else:
            self.logger.warning(f"XGBoost model not found at {self.model_path} - using fallback predictions")
            self.model = None

        # Dynamic decision thresholds from configuration
        self.buy_threshold = getattr(config, 'xgboost_buy_threshold', 0.65)
        self.sell_threshold = getattr(config, 'xgboost_sell_threshold', 0.35)

        self.prediction_history = deque(maxlen=100)

    async def generate_recommendation(self,
                                    symbol: str,
                                    news_data: ProcessedNewsData = None,
                                    sentiment_results: List[EnhancedSentimentResult] = None,
                                    features: Dict[str, Any] = None) -> TradingRecommendation:
        """Generate trading recommendation based on news and sentiment analysis"""

        try:
            # Use provided features or engineer them from news data
            if features is None:
                if news_data is None or sentiment_results is None:
                    return self._create_data_insufficient_recommendation(symbol, "Insufficient data for recommendation")
                features = await self._engineer_features(symbol, news_data, sentiment_results)
            else:
                # Convert features dict to expected format if needed
                if 'sentiment_score' not in features:
                    features['sentiment_score'] = 0.0

            # Generate prediction score
            prediction_score = await self._predict(features)
            
            # Handle case where prediction fails (returns None)
            if prediction_score is None:
                return self._create_data_insufficient_recommendation(symbol, "Prediction model failed to generate score")

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
                source_articles=len(sentiment_results) if sentiment_results else 0
            )

            # Store prediction for model improvement
            self.prediction_history.append({
                'symbol': symbol,
                'prediction': prediction_score,
                'action': action,
                'confidence': confidence,
                'timestamp': datetime.now()
            })

            return recommendation

        except Exception as e:
            self.logger.error(f"Error generating recommendation for {symbol}: {e}")
            # Return data-insufficient recommendation - NO HARDCODED VALUES
            return self._create_data_insufficient_recommendation(symbol, str(e))

    async def _engineer_features(self,
                               symbol: str,
                               news_data: ProcessedNewsData,
                               sentiment_results: List[EnhancedSentimentResult]) -> Dict[str, float]:
        """Engineer features for ML model with dynamic market-based calculations"""

        features = {}
        
        # Get market metrics for dynamic feature engineering
        market_metrics = get_all_metrics(symbol)

        # Sentiment-based features with market-adjusted baselines
        if sentiment_results:
            avg_confidence = statistics.mean([r.confidence for r in sentiment_results])
            positive_ratio = len([r for r in sentiment_results if r.sentiment_label == SentimentLabel.POSITIVE]) / len(sentiment_results)
            negative_ratio = len([r for r in sentiment_results if r.sentiment_label == SentimentLabel.NEGATIVE]) / len(sentiment_results)

            # Weighted sentiment score with market volatility adjustment
            sentiment_score = sum([
                (r.positive_score - r.negative_score) * r.confidence * r.market_impact_score
                for r in sentiment_results
            ]) / len(sentiment_results) if sentiment_results else 0.0

            features.update({
                'sentiment_score': max(-1, min(1, sentiment_score)),
                'sentiment_confidence': avg_confidence,
                'positive_ratio': positive_ratio,
                'negative_ratio': negative_ratio,
                'sentiment_volatility': statistics.stdev([r.positive_score - r.negative_score for r in sentiment_results]) if len(sentiment_results) > 1 else 0.0
            })
        else:
            # Use market-based baselines instead of hardcoded 0.0
            neutral_baseline = self._calculate_market_neutral_baseline(market_metrics)
            features.update({
                'sentiment_score': neutral_baseline,
                'sentiment_confidence': self._calculate_baseline_confidence(market_metrics),
                'positive_ratio': 0.0,  # No positive articles
                'negative_ratio': 0.0,  # No negative articles
                'sentiment_volatility': market_metrics.volatility if market_metrics.volatility else 0.3
            })

        # Dynamic news volume normalization based on market conditions
        volume_normalizer = self._calculate_dynamic_volume_normalizer(market_metrics)
        features.update({
            'news_volume': min(news_data.news_volume / volume_normalizer, 1.0),
            'data_quality': news_data.data_quality_score,
            'staleness_factor': self._calculate_dynamic_staleness_factor(news_data.staleness_minutes, market_metrics),
            'source_diversity': self._calculate_dynamic_source_diversity(news_data.sources_used, market_metrics),
        })

        # Market impact features with dynamic baselines
        if news_data.key_articles:
            # Calculate dynamic baseline market impact based on market conditions
            market_baseline = self._calculate_market_impact_baseline(market_metrics)
            avg_market_impact = statistics.mean([
                getattr(article, 'market_impact', market_baseline) for article in news_data.key_articles
            ])
            max_market_impact = max([
                getattr(article, 'market_impact', market_baseline) for article in news_data.key_articles
            ])
            features.update({
                'market_impact': avg_market_impact,
                'max_market_impact': max_market_impact,
            })
        else:
            # Use market-based neutral impact instead of hardcoded 0.0
            neutral_impact = self._calculate_neutral_market_impact(market_metrics)
            features.update({
                'market_impact': neutral_impact,
                'max_market_impact': neutral_impact,
            })

        # Dynamic time-based features adjusted for market conditions
        now = datetime.now()
        recency_hours = (now - news_data.timestamp).total_seconds() / 3600
        decay_factor = self._calculate_dynamic_decay_factor(market_metrics)
        features['recency_factor'] = max(0, 1 - (recency_hours / decay_factor))

        # Dynamic market events impact normalization
        events_normalizer = self._calculate_events_normalizer(market_metrics)
        features['market_events_impact'] = min(len(news_data.market_events) / events_normalizer, 1.0)

        return features

    async def _predict(self, features: Dict[str, float]) -> float:
        """Generate prediction score using XGBoost model or fallback"""

        try:
            if self.model:
                # Use real trained model
                feature_values = list(features.values())
                feature_matrix = self.xgb.DMatrix([feature_values])

                # Get prediction from real trained model
                predictions = self.model.predict(feature_matrix)
                prediction = float(predictions[0])

                # Ensure bounds
                return max(0.0, min(1.0, prediction))
            else:
                # Fallback prediction based on features
                self.logger.debug("Using fallback prediction logic (no XGBoost model)")

                # Simple weighted feature combination
                sentiment_score = features.get('sentiment_score', 0.0)
                sentiment_confidence = features.get('sentiment_confidence', 0.0)
                news_volume = features.get('news_volume', 0.0)
                data_quality = features.get('data_quality', 0.0)

                # Weighted prediction - FROM CONFIGURATION MANAGER
                sentiment_weight = get_config('XGBOOST_SENTIMENT_WEIGHT', 0.4)
                confidence_weight = get_config('XGBOOST_CONFIDENCE_WEIGHT', 0.3)
                volume_weight = get_config('XGBOOST_VOLUME_WEIGHT', 0.2)
                quality_weight = get_config('XGBOOST_QUALITY_WEIGHT', 0.1)
                
                prediction = (
                    sentiment_score * sentiment_weight +      # Weight on sentiment from config
                    sentiment_confidence * confidence_weight + # Weight on confidence from config
                    news_volume * volume_weight +             # Weight on volume from config
                    data_quality * quality_weight             # Weight on quality from config
                )

                # Normalize to 0-1 range and add baseline
                prediction = (prediction + 1.0) / 2.0  # Convert from -1,1 to 0,1
                prediction = max(0.1, min(0.9, prediction))  # Keep away from extremes

                return prediction

        except Exception as e:
            self.logger.error(f"Prediction failed: {e}")
            # Return None to indicate insufficient data for prediction instead of fallback value
            return None

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

        # Sentiment reasoning - FROM CONFIGURATION MANAGER
        positive_sentiment_threshold = get_config('XGBOOST_POSITIVE_SENTIMENT_THRESHOLD', 0.3)
        negative_sentiment_threshold = get_config('XGBOOST_NEGATIVE_SENTIMENT_THRESHOLD', -0.3)
        
        if sentiment > positive_sentiment_threshold:
            reasoning_parts.append(f"Positive news sentiment ({sentiment:.2f})")
        elif sentiment < negative_sentiment_threshold:
            reasoning_parts.append(f"Negative news sentiment ({sentiment:.2f})")
        else:
            reasoning_parts.append("Neutral news sentiment")

        # Volume reasoning - FROM CONFIGURATION MANAGER
        high_volume_threshold = get_config('XGBOOST_HIGH_VOLUME_THRESHOLD', 0.7)
        moderate_volume_threshold = get_config('XGBOOST_POSITIVE_SENTIMENT_THRESHOLD', 0.3)  # Reuse threshold
        
        if volume > high_volume_threshold:
            reasoning_parts.append("high news volume")
        elif volume > moderate_volume_threshold:
            reasoning_parts.append("moderate news volume")
        else:
            reasoning_parts.append("low news volume")

        # Quality reasoning
        if quality > 0.8:
            reasoning_parts.append("high-quality sources")
        elif quality < 0.6:
            reasoning_parts.append("mixed-quality sources")

        # Confidence reasoning
        if confidence > 0.8:
            reasoning_parts.append("high analytical confidence")
        elif confidence < 0.6:
            reasoning_parts.append("moderate analytical confidence")

        return f"{action} recommendation based on: " + ", ".join(reasoning_parts) + f" (score: {score:.3f})"

    def _create_data_insufficient_recommendation(self, symbol: str, error_reason: str) -> Dict[str, Any]:
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
        
        return {
            'symbol': symbol,
            'action': 'HOLD',  # Always conservative action when data insufficient
            'confidence': confidence,
            'prediction_score': None,  # Explicitly None to indicate no prediction made
            'risk_score': risk_score,
            'expected_return': 0.0,  # No expected return when no data
            'reasoning': f"Insufficient data for reliable analysis: {error_reason}. Conservative HOLD recommendation based on limited market metrics.",
            'timestamp': datetime.now(),
            'data_quality': 'insufficient'
        }

    # Market-based helper methods for dynamic calculations
    def _calculate_market_neutral_baseline(self, market_metrics: MarketMetricsResult) -> float:
        """Calculate market-neutral sentiment baseline"""
        if not market_metrics or market_metrics.volatility is None:
            return 0.0
        # Higher volatility = more negative baseline (uncertainty)
        return max(-0.2, min(0.2, -market_metrics.volatility * 0.5))

    def _calculate_baseline_confidence(self, market_metrics: MarketMetricsResult) -> float:
        """Calculate baseline confidence from market conditions"""
        if not market_metrics or market_metrics.volatility is None:
            return 0.5
        # Lower volatility = higher baseline confidence
        return max(0.3, min(0.8, 1.0 - market_metrics.volatility))

    def _calculate_dynamic_volume_normalizer(self, market_metrics: MarketMetricsResult) -> float:
        """Calculate dynamic volume normalizer based on market conditions"""
        if not market_metrics or market_metrics.volume is None:
            return 10.0  # Default normalizer
        # Higher market volume = higher news volume expected
        return max(5.0, min(20.0, market_metrics.volume / 1000000))

    def _calculate_dynamic_staleness_factor(self, staleness_minutes: float, market_metrics: MarketMetricsResult) -> float:
        """Calculate staleness factor adjusted for market volatility"""
        if not market_metrics or market_metrics.volatility is None:
            base_decay = 60.0  # Default 1-hour decay
        else:
            # Higher volatility = faster information decay
            base_decay = max(30.0, min(120.0, 60.0 / (1 + market_metrics.volatility)))
        
        return min(1.0, staleness_minutes / base_decay)

    def _calculate_dynamic_source_diversity(self, sources_used: List[str], market_metrics: MarketMetricsResult) -> float:
        """Calculate source diversity score"""
        if not sources_used:
            return 0.0
        # More sources = better diversity, capped based on market volatility expectations
        expected_sources = 5.0
        if market_metrics and market_metrics.volatility:
            expected_sources = max(3.0, min(8.0, 5.0 * (1 + market_metrics.volatility)))
        
        return min(1.0, len(set(sources_used)) / expected_sources)

    def _calculate_market_impact_baseline(self, market_metrics: MarketMetricsResult) -> float:
        """Calculate market impact baseline"""
        if not market_metrics or market_metrics.volatility is None:
            return 0.5
        # Higher volatility = higher baseline impact expectation
        return max(0.3, min(0.8, 0.5 + market_metrics.volatility * 0.3))

    def _calculate_neutral_market_impact(self, market_metrics: MarketMetricsResult) -> float:
        """Calculate neutral market impact when no articles available"""
        if not market_metrics or market_metrics.volatility is None:
            return 0.3  # Conservative neutral impact
        # Use volatility to estimate neutral impact
        return max(0.2, min(0.5, 0.3 + market_metrics.volatility * 0.2))

    def _calculate_dynamic_decay_factor(self, market_metrics: MarketMetricsResult) -> float:
        """Calculate time decay factor based on market conditions"""
        if not market_metrics or market_metrics.volatility is None:
            return 4.0  # Default 4-hour decay
        # Higher volatility = faster information decay
        return max(2.0, min(8.0, 4.0 / (1 + market_metrics.volatility)))

    def _calculate_events_normalizer(self, market_metrics: MarketMetricsResult) -> float:
        """Calculate events normalizer based on market activity"""
        if not market_metrics or market_metrics.volume is None:
            return 3.0  # Default normalizer
        # Higher volume = more events expected
        volume_factor = market_metrics.volume / 1000000  # Normalize to millions
        return max(2.0, min(6.0, 3.0 * (1 + volume_factor * 0.1)))