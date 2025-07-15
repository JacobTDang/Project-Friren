"""
XGBoost Feature Engineer - Extracted from Enhanced News Pipeline Process

This module handles feature engineering for XGBoost recommendations from news and sentiment data.
Provides market-driven feature extraction with dynamic calculations based on real market metrics.

Features:
- Dynamic market-based feature calculations (no hardcoded values)
- Sentiment aggregation with market volatility adjustment
- News volume normalization based on market conditions
- Market impact calculations with dynamic baselines
- Time-based feature engineering with market-adjusted decay
"""

import statistics
from datetime import datetime
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from enum import Enum
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
    title: str = ""
    article_content: str = ""
    source: str = ""
    timestamp: datetime = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()


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
    timestamp: datetime = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()


class FeatureEngineer:
    """Feature engineering for XGBoost recommendations with market-driven calculations"""

    def __init__(self):
        self.logger = logging.getLogger(f"{__name__}.FeatureEngineer")

    def engineer_features(self,
                               symbol: str,
                               news_data: ProcessedNewsData,
                               sentiment_results: List[EnhancedSentimentResult]) -> Dict[str, float]:
        """
        Engineer features for ML model with dynamic market-based calculations
        
        Args:
            symbol: Stock symbol
            news_data: Processed news data
            sentiment_results: List of sentiment analysis results
            
        Returns:
            Dictionary of engineered features for ML model
        """
        try:
            features = {}
            
            # Get market metrics for dynamic feature engineering
            market_metrics = get_all_metrics(symbol)

            # Sentiment-based features with market-adjusted baselines
            sentiment_features = self._extract_sentiment_features(sentiment_results, market_metrics)
            features.update(sentiment_features)

            # News volume and quality features
            news_features = self._extract_news_features(news_data, market_metrics)
            features.update(news_features)

            # Market impact features with dynamic baselines
            impact_features = self._extract_market_impact_features(news_data, market_metrics)
            features.update(impact_features)

            # Time-based features adjusted for market conditions
            temporal_features = self._extract_temporal_features(news_data, market_metrics)
            features.update(temporal_features)

            # Market events impact features
            events_features = self._extract_events_features(news_data, market_metrics)
            features.update(events_features)

            # CRITICAL FIX: Ensure exactly 21 features for XGBoost model
            features = self._ensure_21_features(features, symbol, market_metrics)
            
            self.logger.debug(f"Engineered {len(features)} features for {symbol}")
            return features

        except Exception as e:
            self.logger.error(f"Error engineering features for {symbol}: {e}")
            return self._get_fallback_21_features(market_metrics)

    def _extract_sentiment_features(self, 
                                  sentiment_results: List[EnhancedSentimentResult],
                                  market_metrics: MarketMetricsResult) -> Dict[str, float]:
        """Extract sentiment-based features with market adjustment"""
        features = {}
        
        # DEBUG: Log what we receive
        self.logger.debug(f"Feature engineering received {len(sentiment_results) if sentiment_results else 0} sentiment results")
        if sentiment_results:
            for i, result in enumerate(sentiment_results):
                self.logger.debug(f"  Result {i}: {result.sentiment_label.value} confidence={result.confidence:.3f}")
        
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
                'sentiment_volatility': getattr(market_metrics, 'volatility', 0.3) if market_metrics else 0.3
            })

        return features

    def _extract_news_features(self, 
                             news_data: ProcessedNewsData,
                             market_metrics: MarketMetricsResult) -> Dict[str, float]:
        """Extract news volume and quality features"""
        # Dynamic news volume normalization based on market conditions
        volume_normalizer = self._calculate_dynamic_volume_normalizer(market_metrics)
        
        return {
            'news_volume': min(news_data.news_volume / volume_normalizer, 1.0),
            'data_quality': news_data.data_quality_score,
            'staleness_factor': self._calculate_dynamic_staleness_factor(news_data.staleness_minutes, market_metrics),
            'source_diversity': self._calculate_dynamic_source_diversity(news_data.sources_used, market_metrics),
        }

    def _extract_market_impact_features(self, 
                                      news_data: ProcessedNewsData,
                                      market_metrics: MarketMetricsResult) -> Dict[str, float]:
        """Extract market impact features with dynamic baselines"""
        if news_data.key_articles:
            # Calculate dynamic baseline market impact based on market conditions
            market_baseline = self._calculate_market_impact_baseline(market_metrics)
            avg_market_impact = statistics.mean([
                getattr(article, 'market_impact', market_baseline) for article in news_data.key_articles
            ])
            max_market_impact = max([
                getattr(article, 'market_impact', market_baseline) for article in news_data.key_articles
            ])
            
            return {
                'market_impact': avg_market_impact,
                'max_market_impact': max_market_impact,
            }
        else:
            # Use market-based neutral impact instead of hardcoded 0.0
            neutral_impact = self._calculate_neutral_market_impact(market_metrics)
            return {
                'market_impact': neutral_impact,
                'max_market_impact': neutral_impact,
            }

    def _extract_temporal_features(self, 
                                 news_data: ProcessedNewsData,
                                 market_metrics: MarketMetricsResult) -> Dict[str, float]:
        """Extract time-based features adjusted for market conditions"""
        now = datetime.now()
        recency_hours = (now - news_data.timestamp).total_seconds() / 3600
        decay_factor = self._calculate_dynamic_decay_factor(market_metrics)
        
        return {
            'recency_factor': max(0, 1 - (recency_hours / decay_factor))
        }

    def _extract_events_features(self, 
                               news_data: ProcessedNewsData,
                               market_metrics: MarketMetricsResult) -> Dict[str, float]:
        """Extract market events impact features"""
        events_normalizer = self._calculate_events_normalizer(market_metrics)
        
        return {
            'market_events_impact': min(len(news_data.market_events) / events_normalizer, 1.0)
        }

    def _get_fallback_features(self, market_metrics: MarketMetricsResult) -> Dict[str, float]:
        """Get fallback features when feature engineering fails"""
        return {
            'sentiment_score': self._calculate_market_neutral_baseline(market_metrics),
            'sentiment_confidence': self._calculate_baseline_confidence(market_metrics),
            'positive_ratio': 0.0,
            'negative_ratio': 0.0,
            'sentiment_volatility': market_metrics.volatility if market_metrics.volatility else 0.3,
            'news_volume': 0.0,
            'data_quality': 0.5,
            'staleness_factor': 1.0,
            'source_diversity': 0.0,
            'market_impact': self._calculate_neutral_market_impact(market_metrics),
            'max_market_impact': self._calculate_neutral_market_impact(market_metrics),
            'recency_factor': 0.0,
            'market_events_impact': 0.0
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
        if not market_metrics or not hasattr(market_metrics, 'liquidity_score'):
            return 10.0  # Default normalizer
        # Higher liquidity = higher news volume expected
        liquidity_factor = getattr(market_metrics, 'liquidity_score', 50.0) / 100.0
        return max(5.0, min(20.0, 10.0 * (1 + liquidity_factor)))

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
        # MarketMetricsResult doesn't have volume, use liquidity_score as proxy
        if not market_metrics or not hasattr(market_metrics, 'liquidity_score'):
            return 3.0  # Default normalizer
        # Higher liquidity = more events expected
        liquidity_factor = getattr(market_metrics, 'liquidity_score', 50.0) / 100.0  # Normalize to 0-1
        return max(2.0, min(6.0, 3.0 * (1 + liquidity_factor * 0.2)))
    
    def _ensure_21_features(self, features: Dict[str, float], symbol: str, market_metrics: MarketMetricsResult) -> Dict[str, float]:
        """
        CRITICAL FIX: Ensure exactly 21 features for XGBoost model compatibility
        
        The XGBoost model was trained with 21 features. This method adds missing features
        or removes extra features to match the exact requirement.
        """
        try:
            # Define the exact 21 feature names in order (indices 0-20)
            required_features = [
                'sentiment_score',           # 0: Primary sentiment (-1 to 1)
                'sentiment_confidence',      # 1: Confidence in sentiment (0-1)
                'market_impact',            # 2: Market impact score (0-1)
                'volatility_score',         # 3: Market volatility factor (0-1)
                'news_volume',              # 4: News volume normalized (0-1)
                'data_quality',             # 5: Data quality score (0-1)
                'positive_ratio',           # 6: Ratio of positive articles (0-1)
                'negative_ratio',           # 7: Ratio of negative articles (0-1)
                'neutral_ratio',            # 8: Ratio of neutral articles (0-1)
                'max_market_impact',        # 9: Maximum impact from articles (0-1)
                'recency_factor',           # 10: Time-based recency (0-1)
                'source_diversity',         # 11: Source diversity score (0-1)
                'has_news',                 # 12: Binary flag for news presence (0 or 1)
                'staleness_factor',         # 13: Data staleness factor (0-1)
                'sentiment_volatility',     # 14: Sentiment volatility (0-1)
                'market_hours_factor',      # 15: Market hours adjustment (0-1)
                'sector_correlation',       # 16: Sector correlation score (0-1)
                'technical_score',          # 17: Technical indicator score (0-1)
                'data_freshness',          # 18: Data freshness score (0-1)
                'risk_adjustment',         # 19: Risk adjustment factor (0-1)
                'confidence_weighted'       # 20: Confidence-weighted composite (0-1)
            ]
            
            # Start with ordered feature dict
            ordered_features = {}
            
            # Fill in features we have, calculate missing ones
            for feature_name in required_features:
                if feature_name in features:
                    # Use existing feature
                    ordered_features[feature_name] = features[feature_name]
                else:
                    # Calculate missing feature with market-based logic
                    ordered_features[feature_name] = self._calculate_missing_feature(
                        feature_name, features, market_metrics
                    )
            
            # Verify we have exactly 21 features
            if len(ordered_features) != 21:
                self.logger.warning(f"Feature count mismatch for {symbol}: {len(ordered_features)} != 21")
                # Pad or trim to exactly 21
                feature_list = list(ordered_features.items())[:21]
                while len(feature_list) < 21:
                    feature_list.append((f'padding_{len(feature_list)}', 0.5))
                ordered_features = dict(feature_list)
            
            self.logger.debug(f"Ensured exactly 21 features for {symbol}")
            return ordered_features
            
        except Exception as e:
            self.logger.error(f"Error ensuring 21 features for {symbol}: {e}")
            # Return fallback 21 features
            return self._get_fallback_21_features(market_metrics)
    
    def _calculate_missing_feature(self, feature_name: str, existing_features: Dict[str, float], 
                                 market_metrics: MarketMetricsResult) -> float:
        """Calculate value for missing feature based on existing data and market conditions"""
        try:
            # Market-based fallback calculations
            if feature_name == 'volatility_score':
                return market_metrics.volatility if market_metrics and hasattr(market_metrics, 'volatility') and market_metrics.volatility else 0.3
            
            elif feature_name == 'neutral_ratio':
                pos_ratio = existing_features.get('positive_ratio', 0.0)
                neg_ratio = existing_features.get('negative_ratio', 0.0)
                return max(0.0, 1.0 - pos_ratio - neg_ratio)
            
            elif feature_name == 'has_news':
                return 1.0 if existing_features.get('news_volume', 0.0) > 0 else 0.0
            
            elif feature_name == 'market_hours_factor':
                # Simple market hours calculation (can be enhanced)
                from datetime import datetime
                now = datetime.now()
                hour = now.hour
                if 9 <= hour <= 16:  # Market hours (EST)
                    return 1.0
                elif 7 <= hour <= 18:  # Extended hours
                    return 0.7
                else:
                    return 0.3
            
            elif feature_name == 'sector_correlation':
                # Use market impact as proxy for sector correlation
                return existing_features.get('market_impact', 0.5)
            
            elif feature_name == 'technical_score':
                # Combine sentiment and market impact
                sentiment = existing_features.get('sentiment_score', 0.0)
                impact = existing_features.get('market_impact', 0.5)
                return (abs(sentiment) + impact) / 2.0
            
            elif feature_name == 'data_freshness':
                # Inverse of staleness
                staleness = existing_features.get('staleness_factor', 0.5)
                return 1.0 - staleness
            
            elif feature_name == 'risk_adjustment':
                # Based on volatility and sentiment volatility
                vol = market_metrics.volatility if market_metrics and hasattr(market_metrics, 'volatility') and market_metrics.volatility else 0.3
                sent_vol = existing_features.get('sentiment_volatility', 0.3)
                return (vol + sent_vol) / 2.0
            
            elif feature_name == 'confidence_weighted':
                # Weighted combination of key features
                confidence = existing_features.get('sentiment_confidence', 0.5)
                impact = existing_features.get('market_impact', 0.5)
                quality = existing_features.get('data_quality', 0.5)
                return (confidence * 0.4 + impact * 0.4 + quality * 0.2)
            
            else:
                # Default neutral value for unknown features
                return 0.5
                
        except Exception as e:
            self.logger.debug(f"Error calculating missing feature {feature_name}: {e}")
            return 0.5
    
    def _get_fallback_21_features(self, market_metrics: MarketMetricsResult) -> Dict[str, float]:
        """Get fallback 21 features when everything fails"""
        vol = market_metrics.volatility if market_metrics and hasattr(market_metrics, 'volatility') and market_metrics.volatility else 0.3
        
        return {
            'sentiment_score': 0.0,
            'sentiment_confidence': 0.5,
            'market_impact': 0.5,
            'volatility_score': vol,
            'news_volume': 0.0,
            'data_quality': 0.5,
            'positive_ratio': 0.0,
            'negative_ratio': 0.0,
            'neutral_ratio': 1.0,
            'max_market_impact': 0.5,
            'recency_factor': 0.5,
            'source_diversity': 0.0,
            'has_news': 0.0,
            'staleness_factor': 0.5,
            'sentiment_volatility': vol,
            'market_hours_factor': 0.7,
            'sector_correlation': 0.5,
            'technical_score': 0.5,
            'data_freshness': 0.5,
            'risk_adjustment': vol,
            'confidence_weighted': 0.5
        }


def create_feature_engineer() -> FeatureEngineer:
    """Factory function to create feature engineer instance"""
    return FeatureEngineer()