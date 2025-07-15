"""
News Collector - Modular News Collection Component

Core news collection functionality:
- Symbol-specific news collection
- Watchlist batch collection  
- Article processing and sentiment calculation
- Market event extraction
- Quality scoring and metrics

Extracted from news_collector.py for improved modularity.
"""

import math
import logging
import statistics
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from collections import defaultdict

from ..base import NewsArticle
from .source_manager import SourceManager
from .article_deduplicator import ArticleDeduplicator

# Import ProcessedNewsData - define it locally for modularity
from dataclasses import dataclass, field

@dataclass
class ProcessedNewsData:
    """Processed and weighted news data for decision engine"""
    symbol: str
    timestamp: datetime
    overall_sentiment_score: float = 0.0
    sentiment_confidence: float = 0.0
    news_volume: int = 0
    professional_sentiment: float = 0.0
    social_sentiment: float = 0.0
    market_events: List[str] = field(default_factory=list)
    key_articles: List[NewsArticle] = field(default_factory=list)
    sources_used: List[str] = field(default_factory=list)
    source_weights: Dict[str, float] = field(default_factory=dict)
    last_updated: datetime = field(default_factory=datetime.now)
    data_quality_score: float = 0.0
    staleness_minutes: int = 0


class NewsCollector:
    """
    Core news collection functionality
    Handles collection, processing, and quality assessment
    """

    def __init__(self, source_manager: Optional[SourceManager] = None, 
                 deduplicator: Optional[ArticleDeduplicator] = None):
        """Initialize the news collector with optional dependencies"""
        self.logger = logging.getLogger("news_collector")
        
        # Initialize components
        self.source_manager = source_manager or SourceManager()
        self.deduplicator = deduplicator or ArticleDeduplicator()
        
        # Quality thresholds
        self.min_articles_for_confidence = 3

    def collect_symbol_news(self, symbol: str, hours_back: int = 6,
                          max_articles_per_source: int = 15) -> ProcessedNewsData:
        """
        Collect and process news for a single symbol

        Args:
            symbol: Stock symbol to collect news for
            hours_back: How many hours back to look for news
            max_articles_per_source: Limit articles per source

        Returns:
            ProcessedNewsData: Weighted and processed news data
        """
        self.logger.info(f"Collecting news for {symbol}")

        # Ensure sources are initialized
        self.source_manager.ensure_sources_initialized()

        # Collect from all available sources
        all_articles = []
        sources_used = []
        source_article_counts = {}

        for source_name, news_source in self.source_manager.news_sources.items():
            try:
                self.logger.debug(f"Collecting from {source_name} for {symbol}")

                articles = news_source.get_symbol_news(
                    symbol=symbol,
                    hours_back=hours_back,
                    max_articles=max_articles_per_source
                )

                if articles:
                    all_articles.extend(articles)
                    sources_used.append(source_name)
                    source_article_counts[source_name] = len(articles)
                    self.logger.info(f"Got {len(articles)} articles from {source_name} for {symbol}")
                    
                    # BUSINESS LOGIC OUTPUT: Show collected articles in real-time
                    try:
                        # Import the guaranteed terminal output function
                        import sys
                        import os
                        project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))
                        sys.path.append(project_root)
                        from Friren_V1.trading_engine.portfolio_manager.processes.enhanced_news_pipeline_process import send_colored_business_output
                        
                        for article in articles:
                            article_title = article.title[:60] + "..." if len(article.title) > 60 else article.title
                            send_colored_business_output("news_collector", f"{symbol}: '{article_title}' from {source_name}", "news")
                    except Exception:
                        # Fallback to simple print if import fails
                        for article in articles:
                            article_title = article.title[:60] + "..." if len(article.title) > 60 else article.title
                            print(f"[NEWS COLLECTOR] {symbol}: '{article_title}' from {source_name}", flush=True)
                else:
                    self.logger.info(f"No articles from {source_name} for {symbol}")

            except Exception as e:
                self.logger.warning(f"Error collecting from {source_name} for {symbol}: {e}")
                # Continue with other sources

        if not all_articles:
            self.logger.info(f"No articles found for {symbol} from any source")
            return self._create_empty_news_data(symbol)

        # Deduplicate articles across sources
        unique_articles = self.deduplicator.deduplicate_articles(all_articles)
        self.logger.info(f"REAL NEWS: {symbol} - {len(unique_articles)} unique articles after deduplication")
        
        # Show sources used for visibility
        if sources_used:
            self.logger.info(f"NEWS SOURCES: {symbol} collected from {sources_used}")

        # Process into decision-ready format
        processed_data = self._process_articles_to_data(
            symbol=symbol,
            articles=unique_articles,
            sources_used=sources_used,
            source_counts=source_article_counts
        )

        return processed_data

    def collect_watchlist_news(self, symbols: List[str], hours_back: int = 6,
                             max_articles_per_symbol: int = 10) -> Dict[str, ProcessedNewsData]:
        """
        Efficiently collect news for multiple symbols
        Uses batch collection where possible to minimize API calls
        """
        self.logger.info(f"Collecting news for {len(symbols)} watchlist symbols")

        watchlist_news = {}

        # Strategy: Use watchlist endpoints where available, then individual collection
        try:
            # Collect from sources that support watchlist queries
            batch_articles = self._collect_watchlist_batch(symbols, hours_back, max_articles_per_symbol)

            # Process each symbol's articles
            for symbol in symbols:
                symbol_articles = batch_articles.get(symbol, [])

                if symbol_articles:
                    unique_articles = self.deduplicator.deduplicate_articles(symbol_articles)
                    processed_data = self._process_articles_to_data(
                        symbol=symbol,
                        articles=unique_articles,
                        sources_used=list(self.source_manager.news_sources.keys()),  # Approximate
                        source_counts={}  # We'll calculate this differently for batch
                    )
                else:
                    processed_data = self._create_empty_news_data(symbol)

                watchlist_news[symbol] = processed_data

        except Exception as e:
            self.logger.error(f"Error in watchlist collection: {e}")
            # Fallback: collect individually
            for symbol in symbols:
                watchlist_news[symbol] = self.collect_symbol_news(symbol, hours_back, max_articles_per_symbol)

        total_articles = sum(len(data.key_articles) for data in watchlist_news.values())
        self.logger.info(f"Watchlist collection complete: {total_articles} total articles for {len(symbols)} symbols")

        return watchlist_news

    def _collect_watchlist_batch(self, symbols: List[str], hours_back: int,
                                max_articles_per_symbol: int) -> Dict[str, List[NewsArticle]]:
        """Collect news using batch/watchlist endpoints where available"""
        all_symbol_articles = defaultdict(list)

        for source_name, news_source in self.source_manager.news_sources.items():
            try:
                # Check if source supports watchlist collection
                if hasattr(news_source, 'get_watchlist_news'):
                    self.logger.debug(f"Using watchlist endpoint for {source_name}")

                    watchlist_data = news_source.get_watchlist_news(
                        symbols=symbols,
                        hours_back=hours_back,
                        max_articles_per_symbol=max_articles_per_symbol
                    )

                    # Add to combined results
                    for symbol, articles in watchlist_data.items():
                        all_symbol_articles[symbol].extend(articles)

                else:
                    # Fallback to individual collection
                    self.logger.debug(f"Using individual collection for {source_name}")
                    for symbol in symbols:
                        articles = news_source.get_symbol_news(
                            symbol=symbol,
                            hours_back=hours_back,
                            max_articles=max_articles_per_symbol
                        )
                        all_symbol_articles[symbol].extend(articles)

            except Exception as e:
                self.logger.warning(f"Error in batch collection from {source_name}: {e}")
                continue

        return dict(all_symbol_articles)

    def _process_articles_to_data(self, symbol: str, articles: List[NewsArticle],
                                sources_used: List[str], source_counts: Dict[str, int]) -> ProcessedNewsData:
        """Process articles into structured data for decision engine"""

        if not articles:
            return self._create_empty_news_data(symbol)

        # Calculate sentiment scores
        overall_sentiment, confidence = self._calculate_weighted_sentiment(articles)
        professional_sentiment = self._calculate_professional_sentiment(articles)
        social_sentiment = self._calculate_social_sentiment(articles)

        # Extract market events
        market_events = self._extract_market_events(articles)

        # Select key articles (most important ones)
        key_articles = self._select_key_articles(articles, max_count=5)

        # Calculate quality metrics
        data_quality = self._calculate_data_quality(articles, sources_used)
        staleness = self._calculate_staleness_minutes(articles)

        return ProcessedNewsData(
            symbol=symbol,
            timestamp=datetime.now(),
            overall_sentiment_score=overall_sentiment,
            sentiment_confidence=confidence,
            news_volume=len(articles),
            professional_sentiment=professional_sentiment,
            social_sentiment=social_sentiment,
            market_events=market_events,
            key_articles=key_articles,
            sources_used=sources_used,
            source_weights=self.source_manager.source_weights,
            data_quality_score=data_quality,
            staleness_minutes=staleness
        )

    def _calculate_weighted_sentiment(self, articles: List[NewsArticle]) -> Tuple[float, float]:
        """Calculate overall sentiment with confidence score"""
        if not articles:
            return 0.0, 0.0

        weighted_scores = []
        total_weight = 0.0

        for article in articles:
            # Get article sentiment (from Alpha Vantage or default neutral)
            if hasattr(article, 'sentiment_score') and article.sentiment_score is not None:
                sentiment = article.sentiment_score
            else:
                # Use simple heuristic for articles without sentiment
                sentiment = self._simple_sentiment_heuristic(article)

            # Get source weight
            source_name = self.source_manager.extract_source_name(article.source)
            weight = self.source_manager.get_source_weight(source_name)

            # Weight by engagement for Reddit
            if 'reddit' in article.source.lower():
                engagement_weight = self._get_reddit_engagement_weight(article)
                weight *= engagement_weight

            weighted_scores.append(sentiment * weight)
            total_weight += weight

        if total_weight == 0:
            return 0.0, 0.0

        overall_sentiment = sum(weighted_scores) / total_weight

        # Calculate confidence based on volume and source diversity
        confidence = min(1.0, len(articles) / self.min_articles_for_confidence) * 0.8
        confidence += len(set(self.source_manager.extract_source_name(a.source) for a in articles)) * 0.05
        confidence = min(1.0, confidence)

        return overall_sentiment, confidence

    def _calculate_professional_sentiment(self, articles: List[NewsArticle]) -> float:
        """Calculate sentiment from professional sources only"""
        professional_sources = ['newsapi', 'alpha_vantage', 'marketaux']
        professional_articles = [
            a for a in articles
            if any(source in a.source.lower() for source in professional_sources)
        ]

        if not professional_articles:
            return 0.0

        sentiment_sum = 0.0
        count = 0

        for article in professional_articles:
            if hasattr(article, 'sentiment_score') and article.sentiment_score is not None:
                sentiment_sum += article.sentiment_score
                count += 1

        return sentiment_sum / count if count > 0 else 0.0

    def _calculate_social_sentiment(self, articles: List[NewsArticle]) -> float:
        """Calculate sentiment from social sources (Reddit)"""
        social_articles = [a for a in articles if 'reddit' in a.source.lower()]

        if not social_articles:
            return 0.0

        # Weight by engagement
        weighted_sentiment = 0.0
        total_weight = 0.0

        for article in social_articles:
            sentiment = self._simple_sentiment_heuristic(article)
            weight = self._get_reddit_engagement_weight(article)

            weighted_sentiment += sentiment * weight
            total_weight += weight

        return weighted_sentiment / total_weight if total_weight > 0 else 0.0

    def _extract_market_events(self, articles: List[NewsArticle]) -> List[str]:
        """Extract market-moving events from articles"""
        events = []

        for article in articles:
            title_lower = article.title.lower()

            # Look for earnings-related keywords
            if any(word in title_lower for word in ['earnings', 'revenue', 'profit', 'loss', 'eps']):
                events.append('earnings')

            # Look for analyst actions
            if any(word in title_lower for word in ['upgrade', 'downgrade', 'rating', 'target', 'analyst']):
                events.append('analyst_action')

            # Look for regulatory/legal events
            if any(word in title_lower for word in ['fda', 'approval', 'regulation', 'lawsuit', 'sec']):
                events.append('regulatory')

            # Look for M&A activity
            if any(word in title_lower for word in ['merger', 'acquisition', 'buyout', 'takeover']):
                events.append('ma_activity')

        return list(set(events))  # Remove duplicates

    def _select_key_articles(self, articles: List[NewsArticle], max_count: int = 5) -> List[NewsArticle]:
        """Select the most important articles for context"""
        if len(articles) <= max_count:
            return articles

        # Score articles by importance
        scored_articles = []

        for article in articles:
            score = 0.0

            # Source weight
            source_name = self.source_manager.extract_source_name(article.source)
            score += self.source_manager.get_source_weight(source_name) * 10

            # Recency (newer = better)
            age_hours = (datetime.now() - article.published_date).total_seconds() / 3600
            score += max(0, 10 - age_hours)  # 10 points for very recent, decreasing

            # Engagement (for Reddit)
            if 'reddit' in article.source.lower():
                score += self._get_reddit_engagement_weight(article) * 5

            # Content relevance (simple heuristic)
            if any(word in article.title.lower() for word in ['earnings', 'analyst', 'upgrade', 'downgrade']):
                score += 5

            scored_articles.append((score, article))

        # Sort by score and return top articles
        scored_articles.sort(key=lambda x: x[0], reverse=True)
        return [article for score, article in scored_articles[:max_count]]

    def _calculate_data_quality(self, articles: List[NewsArticle], sources_used: List[str]) -> float:
        """Calculate overall data quality score"""
        if not articles:
            return 0.0

        quality_score = 0.0

        # Source diversity bonus
        unique_sources = len(set(sources_used))
        quality_score += min(1.0, unique_sources / 3) * 0.3  # Up to 30% for source diversity

        # Volume bonus
        quality_score += min(1.0, len(articles) / 10) * 0.3  # Up to 30% for article volume

        # Recency bonus
        recent_articles = [
            a for a in articles
            if (datetime.now() - a.published_date).total_seconds() < 3600  # Last hour
        ]
        quality_score += min(1.0, len(recent_articles) / 5) * 0.2  # Up to 20% for recency

        # Professional source bonus
        professional_articles = [
            a for a in articles
            if any(source in a.source.lower() for source in ['newsapi', 'alpha_vantage'])
        ]
        quality_score += min(1.0, len(professional_articles) / 5) * 0.2  # Up to 20% for professional sources

        return min(1.0, quality_score)

    def _calculate_staleness_minutes(self, articles: List[NewsArticle]) -> int:
        """Calculate how old the newest article is in minutes"""
        if not articles:
            return 9999  # Very stale

        newest_article = max(articles, key=lambda x: x.published_date)
        staleness_seconds = (datetime.now() - newest_article.published_date).total_seconds()
        return int(staleness_seconds / 60)

    def _simple_sentiment_heuristic(self, article: NewsArticle) -> float:
        """Simple sentiment analysis for articles without sentiment scores"""
        title = article.title.lower()

        positive_words = ['up', 'gain', 'rise', 'surge', 'soar', 'bullish', 'positive', 'strong']
        negative_words = ['down', 'fall', 'drop', 'plunge', 'crash', 'bearish', 'negative', 'weak']

        positive_count = sum(1 for word in positive_words if word in title)
        negative_count = sum(1 for word in negative_words if word in title)

        if positive_count > negative_count:
            return 0.3  # Mildly positive
        elif negative_count > positive_count:
            return -0.3  # Mildly negative
        else:
            return 0.0  # Neutral

    def _get_reddit_engagement_weight(self, article: NewsArticle) -> float:
        """Calculate engagement weight for Reddit articles"""
        if not hasattr(article, 'additional_metadata'):
            return 1.0

        metadata = article.additional_metadata
        upvotes = metadata.get('upvotes', 0)
        comments = metadata.get('comments', 0)

        # Normalize engagement (log scale to prevent extreme weights)
        engagement_score = math.log(1 + upvotes + comments * 2) / 10
        return min(3.0, max(0.1, engagement_score))  # Between 0.1 and 3.0

    def _create_empty_news_data(self, symbol: str) -> ProcessedNewsData:
        """Create empty news data when no articles are found"""
        return ProcessedNewsData(
            symbol=symbol,
            timestamp=datetime.now(),
            overall_sentiment_score=0.0,
            sentiment_confidence=0.0,
            news_volume=0,
            professional_sentiment=0.0,
            social_sentiment=0.0,
            market_events=[],
            key_articles=[],
            sources_used=[],
            source_weights={},
            data_quality_score=0.0,
            staleness_minutes=9999
        )