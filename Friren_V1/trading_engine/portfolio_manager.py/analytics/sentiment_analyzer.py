"""
Sentiment Analyzer - Unified Sentiment Analysis Pipeline

This component integrates your existing sentiment analysis systems:
- EnhancedFinBERT (sophisticated NLP sentiment analysis)
- NewsSentimentCollector (multi-source news aggregation)

Provides real-time sentiment analysis for strategy selection and portfolio management.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from datetime import datetime, timedelta
import asyncio
import threading
import time
from collections import deque
import warnings
warnings.filterwarnings('ignore')

# Import your existing sentiment systems
from trading_engine.sentiment.finBERT_analysis import EnhancedFinBERT
from trading_engine.sentiment.news_sentiment import NewsSentimentCollector, NewsArticle

@dataclass
class SentimentReading:
    """Individual sentiment reading result"""
    symbol: str
    sentiment_score: float  # -1.0 to +1.0 (negative to positive)
    confidence: float  # 0-100 confidence in sentiment reading
    article_count: int  # Number of articles analyzed
    source_breakdown: Dict[str, float]  # Sentiment by source (NewsAPI, Yahoo, Reddit)
    timestamp: datetime

    # Detailed metrics
    positive_ratio: float  # % of positive articles
    negative_ratio: float  # % of negative articles
    neutral_ratio: float  # % of neutral articles
    sentiment_momentum: float  # Change in sentiment over time
    news_volume: int  # Total news volume
    social_buzz: float  # Social media activity level

@dataclass
class MarketSentimentSummary:
    """Overall market sentiment summary"""
    overall_sentiment: float  # -1.0 to +1.0 market-wide sentiment
    sentiment_confidence: float  # 0-100 confidence in overall reading
    sentiment_trend: str  # IMPROVING, DETERIORATING, STABLE
    sentiment_strength: str  # WEAK, MODERATE, STRONG

    # Market-wide metrics
    bullish_stocks_pct: float  # % of stocks with positive sentiment
    bearish_stocks_pct: float  # % of stocks with negative sentiment
    sentiment_dispersion: float  # How varied sentiment is across stocks
    news_flow_intensity: float  # Overall news volume

    # Alerts and signals
    sentiment_extremes: List[str]  # Stocks with extreme sentiment
    sentiment_reversals: List[str]  # Stocks with recent sentiment changes
    market_stress_signals: List[str]  # Sentiment-based stress indicators

    timestamp: datetime

class SentimentAnalyzer:
    """
    Unified Sentiment Analysis Component

    Integrates FinBERT analysis with multi-source news collection to provide
    comprehensive sentiment analysis for individual stocks and market-wide sentiment.

    Features:
    - Real-time sentiment scoring
    - Multi-source news aggregation
    - Sentiment momentum tracking
    - Market-wide sentiment synthesis
    - Caching for performance optimization
    - Async processing for scalability
    """

    def __init__(self, config: Optional[Dict] = None):
        self.config = config or self._default_config()

        # Initialize sentiment engines
        self.finbert = EnhancedFinBERT()
        self.news_collector = NewsSentimentCollector()

        # Sentiment caching and history
        self.sentiment_cache = {}  # symbol -> SentimentReading
        self.sentiment_history = {}  # symbol -> deque of historical readings
        self.market_sentiment_history = deque(maxlen=self.config['market_history_length'])

        # Performance optimization
        self.last_update = {}  # symbol -> timestamp
        self.batch_processing_queue = deque()

        # Threading for background updates
        self.background_update_thread = None
        self.is_running = False

        print("Sentiment Analyzer initialized with FinBERT and multi-source news")

    def _default_config(self) -> Dict:
        """Default configuration for sentiment analysis"""
        return {
            # Data collection
            'news_lookback_hours': 6,
            'min_articles_for_confidence': 3,
            'cache_expiry_minutes': 15,

            # Sentiment processing
            'sentiment_smoothing_alpha': 0.3,  # EWM smoothing for momentum
            'confidence_threshold': 30.0,  # Minimum confidence for valid reading
            'extreme_sentiment_threshold': 0.6,  # ±0.6 considered extreme

            # Market analysis
            'market_sentiment_min_stocks': 5,  # Min stocks needed for market sentiment
            'sentiment_dispersion_threshold': 0.4,  # High dispersion threshold
            'market_history_length': 100,  # Keep last 100 market readings

            # Performance
            'batch_size': 10,  # Process up to 10 stocks in batch
            'background_update_interval': 300,  # 5 minutes background updates
            'async_timeout': 30,  # 30 second timeout for async operations

            # Source weights
            'source_weights': {
                'NewsAPI': 0.4,
                'Yahoo Finance': 0.35,
                'Reddit': 0.25
            }
        }

    def analyze_sentiment(self, symbol: str, force_refresh: bool = False) -> SentimentReading:
        """
        MAIN SENTIMENT ANALYSIS METHOD

        Analyzes sentiment for a single symbol using cached data when available
        or fetching fresh data when needed.
        """
        # Check cache first
        if not force_refresh and self._is_cache_valid(symbol):
            return self.sentiment_cache[symbol]

        try:
            # Collect recent news
            symbol_news = self._collect_symbol_news(symbol)

            if not symbol_news:
                return self._no_news_fallback(symbol)

            # Analyze sentiment using FinBERT
            analyzed_articles = self.finbert.analyze_articles(symbol_news)

            # Process sentiment results
            sentiment_reading = self._process_sentiment_results(symbol, analyzed_articles)

            # Update cache and history
            self._update_sentiment_cache(symbol, sentiment_reading)

            return sentiment_reading

        except Exception as e:
            print(f"Error analyzing sentiment for {symbol}: {e}")
            return self._error_fallback(symbol)

    def analyze_market_sentiment(self, symbols: List[str],
                                force_refresh: bool = False) -> MarketSentimentSummary:
        """
        Analyze market-wide sentiment across multiple symbols

        Provides comprehensive market sentiment analysis including trends,
        extremes, and stress indicators.
        """
        if len(symbols) < self.config['market_sentiment_min_stocks']:
            return self._insufficient_symbols_fallback()

        # Get sentiment for all symbols (use batch processing for efficiency)
        symbol_sentiments = self._batch_analyze_symbols(symbols, force_refresh)

        # Filter valid sentiment readings
        valid_sentiments = [s for s in symbol_sentiments if s.confidence >= self.config['confidence_threshold']]

        if len(valid_sentiments) < self.config['market_sentiment_min_stocks']:
            return self._insufficient_data_fallback()

        # Calculate market-wide metrics
        market_summary = self._calculate_market_sentiment_summary(valid_sentiments)

        # Update market sentiment history
        self.market_sentiment_history.append(market_summary)

        return market_summary

    def get_sentiment_momentum(self, symbol: str, lookback_periods: int = 5) -> float:
        """
        Calculate sentiment momentum (trend) for a symbol

        Returns:
            float: -1.0 to +1.0 representing sentiment momentum
                  Positive = improving sentiment, Negative = deteriorating
        """
        if symbol not in self.sentiment_history:
            return 0.0

        history = list(self.sentiment_history[symbol])
        if len(history) < 2:
            return 0.0

        # Take recent readings
        recent_readings = history[-min(lookback_periods, len(history)):]

        if len(recent_readings) < 2:
            return 0.0

        # Calculate momentum using linear regression slope
        x = np.arange(len(recent_readings))
        y = [r.sentiment_score for r in recent_readings]

        if len(x) >= 2:
            slope = np.polyfit(x, y, 1)[0]
            # Normalize slope to reasonable range
            momentum = np.tanh(slope * 10)  # Scale and bound to [-1, 1]
            return momentum

        return 0.0

    def get_sentiment_alerts(self, symbols: List[str]) -> Dict[str, List[str]]:
        """
        Generate sentiment-based alerts and signals

        Returns:
            Dict with alert categories and affected symbols
        """
        alerts = {
            'extreme_positive': [],
            'extreme_negative': [],
            'sentiment_reversals': [],
            'low_confidence': [],
            'high_news_volume': [],
            'sentiment_divergence': []
        }

        for symbol in symbols:
            try:
                sentiment = self.analyze_sentiment(symbol)
                momentum = self.get_sentiment_momentum(symbol)

                # Extreme sentiment alerts
                if sentiment.sentiment_score > self.config['extreme_sentiment_threshold']:
                    alerts['extreme_positive'].append(symbol)
                elif sentiment.sentiment_score < -self.config['extreme_sentiment_threshold']:
                    alerts['extreme_negative'].append(symbol)

                # Sentiment reversal alerts
                if abs(momentum) > 0.5:  # Strong momentum change
                    alerts['sentiment_reversals'].append(symbol)

                # Low confidence alerts
                if sentiment.confidence < self.config['confidence_threshold']:
                    alerts['low_confidence'].append(symbol)

                # High news volume alerts
                if sentiment.news_volume > 50:  # Arbitrary threshold
                    alerts['high_news_volume'].append(symbol)

            except Exception as e:
                print(f"Error generating alerts for {symbol}: {e}")
                continue

        return alerts

    def _collect_symbol_news(self, symbol: str) -> List[NewsArticle]:
        """Collect news articles for a specific symbol"""
        try:
            # Get all recent news
            all_articles = self.news_collector.collect_all_news(
                hours_back=self.config['news_lookback_hours']
            )

            # Filter for symbol-specific articles
            symbol_articles = [
                article for article in all_articles
                if symbol.upper() in [s.upper() for s in article.symbols_mentioned]
            ]

            return symbol_articles

        except Exception as e:
            print(f"Error collecting news for {symbol}: {e}")
            return []

    def _process_sentiment_results(self, symbol: str, articles: List[NewsArticle]) -> SentimentReading:
        """Process analyzed articles into sentiment reading"""

        if not articles:
            return self._no_news_fallback(symbol)

        # Get articles with sentiment analysis
        analyzed_articles = [a for a in articles if hasattr(a, 'sentiment_score') and a.sentiment_score is not None]

        if not analyzed_articles:
            return self._no_sentiment_fallback(symbol)

        # Calculate weighted sentiment score
        sentiment_scores = []
        source_sentiments = {}
        confidence_scores = []

        for article in analyzed_articles:
            score = article.sentiment_score
            confidence = getattr(article, 'sentiment_confidence', 0.5)
            source = article.source.split('-')[0] if '-' in article.source else article.source

            sentiment_scores.append(score)
            confidence_scores.append(confidence)

            if source not in source_sentiments:
                source_sentiments[source] = []
            source_sentiments[source].append(score)

        # Calculate overall sentiment (weighted by confidence)
        if confidence_scores:
            weights = np.array(confidence_scores)
            weighted_sentiment = np.average(sentiment_scores, weights=weights)
        else:
            weighted_sentiment = np.mean(sentiment_scores)

        # Normalize to [-1, 1] range (FinBERT typically gives [0, 1])
        # Assuming FinBERT returns scores where 0.5 is neutral
        normalized_sentiment = (weighted_sentiment - 0.5) * 2
        normalized_sentiment = max(-1.0, min(1.0, normalized_sentiment))

        # Calculate confidence based on article count and agreement
        article_confidence = min(100, len(analyzed_articles) * 20)  # More articles = higher confidence
        agreement = 1.0 - np.std(sentiment_scores) if len(sentiment_scores) > 1 else 1.0
        agreement_confidence = agreement * 50
        overall_confidence = min(100, article_confidence + agreement_confidence)

        # Calculate source breakdown
        source_breakdown = {}
        for source, scores in source_sentiments.items():
            source_breakdown[source] = np.mean(scores)

        # Calculate sentiment distribution
        positive_articles = len([s for s in sentiment_scores if s > 0.55])
        negative_articles = len([s for s in sentiment_scores if s < 0.45])
        neutral_articles = len(analyzed_articles) - positive_articles - negative_articles

        total_articles = len(analyzed_articles)
        positive_ratio = positive_articles / total_articles
        negative_ratio = negative_articles / total_articles
        neutral_ratio = neutral_articles / total_articles

        # Calculate sentiment momentum
        momentum = self.get_sentiment_momentum(symbol)

        # Calculate social buzz (based on Reddit articles and engagement)
        reddit_articles = [a for a in analyzed_articles if 'Reddit' in a.source]
        social_buzz = len(reddit_articles) / max(1, len(analyzed_articles))

        # Add engagement metrics if available
        total_engagement = 0
        for article in reddit_articles:
            if hasattr(article, 'engagement_metrics'):
                engagement = article.engagement_metrics
                total_engagement += engagement.get('upvotes', 0) + engagement.get('comments', 0)

        social_buzz += min(1.0, total_engagement / 1000)  # Normalize engagement

        return SentimentReading(
            symbol=symbol,
            sentiment_score=normalized_sentiment,
            confidence=overall_confidence,
            article_count=len(analyzed_articles),
            source_breakdown=source_breakdown,
            timestamp=datetime.now(),
            positive_ratio=positive_ratio,
            negative_ratio=negative_ratio,
            neutral_ratio=neutral_ratio,
            sentiment_momentum=momentum,
            news_volume=len(articles),  # Total articles (including unanalyzed)
            social_buzz=social_buzz
        )

    def _calculate_market_sentiment_summary(self, sentiments: List[SentimentReading]) -> MarketSentimentSummary:
        """Calculate comprehensive market sentiment summary"""

        # Overall market sentiment (confidence-weighted)
        total_weight = sum(s.confidence for s in sentiments)
        if total_weight > 0:
            overall_sentiment = sum(s.sentiment_score * s.confidence for s in sentiments) / total_weight
        else:
            overall_sentiment = 0.0

        # Market sentiment confidence
        avg_confidence = np.mean([s.confidence for s in sentiments])
        sentiment_agreement = 1.0 - np.std([s.sentiment_score for s in sentiments])
        market_confidence = min(100, avg_confidence * sentiment_agreement)

        # Sentiment distribution
        bullish_count = len([s for s in sentiments if s.sentiment_score > 0.1])
        bearish_count = len([s for s in sentiments if s.sentiment_score < -0.1])
        total_count = len(sentiments)

        bullish_pct = bullish_count / total_count
        bearish_pct = bearish_count / total_count

        # Sentiment dispersion (how varied sentiment is)
        sentiment_scores = [s.sentiment_score for s in sentiments]
        sentiment_dispersion = np.std(sentiment_scores)

        # Sentiment trend analysis
        if len(self.market_sentiment_history) >= 3:
            recent_sentiments = [ms.overall_sentiment for ms in list(self.market_sentiment_history)[-3:]]
            trend_slope = np.polyfit(range(len(recent_sentiments)), recent_sentiments, 1)[0]

            if trend_slope > 0.05:
                sentiment_trend = 'IMPROVING'
            elif trend_slope < -0.05:
                sentiment_trend = 'DETERIORATING'
            else:
                sentiment_trend = 'STABLE'
        else:
            sentiment_trend = 'STABLE'

        # Sentiment strength classification
        abs_sentiment = abs(overall_sentiment)
        if abs_sentiment > 0.5:
            sentiment_strength = 'STRONG'
        elif abs_sentiment > 0.2:
            sentiment_strength = 'MODERATE'
        else:
            sentiment_strength = 'WEAK'

        # News flow intensity
        total_news_volume = sum(s.news_volume for s in sentiments)
        avg_news_per_stock = total_news_volume / len(sentiments)
        news_flow_intensity = min(1.0, avg_news_per_stock / 20)  # Normalize to 0-1

        # Identify extremes and alerts
        extreme_threshold = self.config['extreme_sentiment_threshold']
        sentiment_extremes = [
            s.symbol for s in sentiments
            if abs(s.sentiment_score) > extreme_threshold
        ]

        # Sentiment reversals (high momentum)
        sentiment_reversals = []
        for s in sentiments:
            momentum = self.get_sentiment_momentum(s.symbol)
            if abs(momentum) > 0.4:  # Strong momentum
                sentiment_reversals.append(s.symbol)

        # Market stress signals
        stress_signals = []
        if sentiment_dispersion > self.config['sentiment_dispersion_threshold']:
            stress_signals.append('High sentiment dispersion')
        if abs(overall_sentiment) > 0.7:
            stress_signals.append('Extreme market sentiment')
        if len(sentiment_extremes) > len(sentiments) * 0.3:
            stress_signals.append('Many stocks with extreme sentiment')

        return MarketSentimentSummary(
            overall_sentiment=overall_sentiment,
            sentiment_confidence=market_confidence,
            sentiment_trend=sentiment_trend,
            sentiment_strength=sentiment_strength,
            bullish_stocks_pct=bullish_pct,
            bearish_stocks_pct=bearish_pct,
            sentiment_dispersion=sentiment_dispersion,
            news_flow_intensity=news_flow_intensity,
            sentiment_extremes=sentiment_extremes,
            sentiment_reversals=sentiment_reversals,
            market_stress_signals=stress_signals,
            timestamp=datetime.now()
        )

    def _batch_analyze_symbols(self, symbols: List[str], force_refresh: bool = False) -> List[SentimentReading]:
        """Efficiently analyze sentiment for multiple symbols"""
        results = []

        # Process in batches for performance
        batch_size = self.config['batch_size']
        for i in range(0, len(symbols), batch_size):
            batch = symbols[i:i + batch_size]

            for symbol in batch:
                try:
                    sentiment = self.analyze_sentiment(symbol, force_refresh)
                    results.append(sentiment)
                except Exception as e:
                    print(f"Error in batch processing {symbol}: {e}")
                    continue

                # Small delay to avoid rate limiting
                time.sleep(0.1)

        return results

    def _is_cache_valid(self, symbol: str) -> bool:
        """Check if cached sentiment data is still valid"""
        if symbol not in self.sentiment_cache:
            return False

        cached_reading = self.sentiment_cache[symbol]
        age_minutes = (datetime.now() - cached_reading.timestamp).total_seconds() / 60

        return age_minutes < self.config['cache_expiry_minutes']

    def _update_sentiment_cache(self, symbol: str, reading: SentimentReading):
        """Update sentiment cache and history"""
        self.sentiment_cache[symbol] = reading
        self.last_update[symbol] = datetime.now()

        # Update history
        if symbol not in self.sentiment_history:
            self.sentiment_history[symbol] = deque(maxlen=50)  # Keep last 50 readings

        self.sentiment_history[symbol].append(reading)

    def _no_news_fallback(self, symbol: str) -> SentimentReading:
        """Fallback when no news is available for symbol"""
        return SentimentReading(
            symbol=symbol,
            sentiment_score=0.0,
            confidence=0.0,
            article_count=0,
            source_breakdown={},
            timestamp=datetime.now(),
            positive_ratio=0.0,
            negative_ratio=0.0,
            neutral_ratio=0.0,
            sentiment_momentum=0.0,
            news_volume=0,
            social_buzz=0.0
        )

    def _no_sentiment_fallback(self, symbol: str) -> SentimentReading:
        """Fallback when articles exist but no sentiment analysis"""
        return SentimentReading(
            symbol=symbol,
            sentiment_score=0.0,
            confidence=10.0,  # Low but not zero confidence
            article_count=0,
            source_breakdown={},
            timestamp=datetime.now(),
            positive_ratio=0.0,
            negative_ratio=0.0,
            neutral_ratio=0.0,
            sentiment_momentum=0.0,
            news_volume=1,  # Some news existed
            social_buzz=0.0
        )

    def _error_fallback(self, symbol: str) -> SentimentReading:
        """Fallback when analysis fails"""
        return SentimentReading(
            symbol=symbol,
            sentiment_score=0.0,
            confidence=0.0,
            article_count=0,
            source_breakdown={},
            timestamp=datetime.now(),
            positive_ratio=0.0,
            negative_ratio=0.0,
            neutral_ratio=0.0,
            sentiment_momentum=0.0,
            news_volume=0,
            social_buzz=0.0
        )

    def _insufficient_symbols_fallback(self) -> MarketSentimentSummary:
        """Fallback when insufficient symbols for market analysis"""
        return MarketSentimentSummary(
            overall_sentiment=0.0,
            sentiment_confidence=0.0,
            sentiment_trend='STABLE',
            sentiment_strength='WEAK',
            bullish_stocks_pct=0.0,
            bearish_stocks_pct=0.0,
            sentiment_dispersion=0.0,
            news_flow_intensity=0.0,
            sentiment_extremes=[],
            sentiment_reversals=[],
            market_stress_signals=['Insufficient symbols for analysis'],
            timestamp=datetime.now()
        )

    def _insufficient_data_fallback(self) -> MarketSentimentSummary:
        """Fallback when insufficient data for market analysis"""
        return MarketSentimentSummary(
            overall_sentiment=0.0,
            sentiment_confidence=0.0,
            sentiment_trend='STABLE',
            sentiment_strength='WEAK',
            bullish_stocks_pct=0.0,
            bearish_stocks_pct=0.0,
            sentiment_dispersion=0.0,
            news_flow_intensity=0.0,
            sentiment_extremes=[],
            sentiment_reversals=[],
            market_stress_signals=['Insufficient sentiment data'],
            timestamp=datetime.now()
        )

    def start_background_updates(self, symbols: List[str]):
        """Start background thread for continuous sentiment updates"""
        if self.is_running:
            return

        self.is_running = True
        self.background_symbols = symbols

        def background_worker():
            while self.is_running:
                try:
                    for symbol in self.background_symbols:
                        if not self.is_running:
                            break

                        # Update if cache is expired
                        if not self._is_cache_valid(symbol):
                            self.analyze_sentiment(symbol)

                        time.sleep(1)  # Small delay between symbols

                    time.sleep(self.config['background_update_interval'])

                except Exception as e:
                    print(f"Background update error: {e}")
                    time.sleep(30)  # Wait before retrying

        self.background_update_thread = threading.Thread(target=background_worker, daemon=True)
        self.background_update_thread.start()
        print(f"Started background sentiment updates for {len(symbols)} symbols")

    def stop_background_updates(self):
        """Stop background sentiment updates"""
        self.is_running = False
        if self.background_update_thread:
            self.background_update_thread.join(timeout=5)
        print("Stopped background sentiment updates")

    def get_sentiment_summary(self, symbol: str) -> Dict:
        """Get human-readable sentiment summary for a symbol"""
        try:
            reading = self.analyze_sentiment(symbol)
            momentum = self.get_sentiment_momentum(symbol)

            # Sentiment description
            if reading.sentiment_score > 0.3:
                sentiment_desc = "Positive"
            elif reading.sentiment_score > 0.1:
                sentiment_desc = "Slightly Positive"
            elif reading.sentiment_score < -0.3:
                sentiment_desc = "Negative"
            elif reading.sentiment_score < -0.1:
                sentiment_desc = "Slightly Negative"
            else:
                sentiment_desc = "Neutral"

            # Momentum description
            if momentum > 0.3:
                momentum_desc = "Improving"
            elif momentum > 0.1:
                momentum_desc = "Slightly Improving"
            elif momentum < -0.3:
                momentum_desc = "Deteriorating"
            elif momentum < -0.1:
                momentum_desc = "Slightly Deteriorating"
            else:
                momentum_desc = "Stable"

            return {
                'symbol': symbol,
                'sentiment': sentiment_desc,
                'score': f"{reading.sentiment_score:.2f}",
                'confidence': f"{reading.confidence:.0f}%",
                'momentum': momentum_desc,
                'articles': reading.article_count,
                'news_volume': reading.news_volume,
                'social_buzz': f"{reading.social_buzz:.1%}",
                'last_updated': reading.timestamp.strftime('%Y-%m-%d %H:%M:%S')
            }

        except Exception as e:
            return {
                'symbol': symbol,
                'error': str(e),
                'sentiment': 'Error',
                'confidence': '0%'
            }

    def clear_cache(self):
        """Clear all cached sentiment data"""
        self.sentiment_cache.clear()
        self.sentiment_history.clear()
        self.last_update.clear()
        print("Sentiment cache cleared")
