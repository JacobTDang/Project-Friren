"""
Enhanced News Collector - Pure Utility Class

Collects, deduplicates, and processes news from multiple sources.
No process logic - just data collection and processing utilities.
"""

import time
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass, field
from collections import defaultdict
import re
import threading

try:
    # Import all news sources
    from Friren_V1.trading_engine.data.news.marketaux_api import MarketauxNews
    from Friren_V1.trading_engine.data.news.alpha_vintage_api import AlphaVantageNews
    from Friren_V1.trading_engine.data.news.fmp_api import FMPNews
    from Friren_V1.trading_engine.data.news.news_api import NewsAPIData
    from Friren_V1.trading_engine.data.news.reddit_api import RedditNews
    from Friren_V1.trading_engine.data.news.base import NewsArticle
except ImportError as e:
    # Fallback for direct execution - try relative imports
    try:
        from .news.marketaux_api import MarketauxNews
        from .news.alpha_vintage_api import AlphaVantageNews
        from .news.fmp_api import FMPNews
        from .news.news_api import NewsAPIData
        from .news.reddit_api import RedditNews
        from .news.base import NewsArticle
    except ImportError as e2:
        print(f"Error importing news sources: {e}, {e2}")
        # Define minimal fallback classes
        class NewsArticle:
            def __init__(self, title="", description="", url="", source="", published_date=None):
                print("[CRITICAL ERROR] Fallback NewsArticle used! This should never happen. Check your imports.")
                import logging
                logging.critical("Fallback NewsArticle used! This should never happen. Check your imports.")
                assert False, "Fallback NewsArticle used! This should never happen. Check your imports."
                self.title = title
                self.description = description
                self.url = url
                self.source = source
                self.published_date = published_date

        MarketauxNews = None
        AlphaVantageNews = None
        FMPNews = None
        NewsAPIData = None
        RedditNews = None

# ULTRA ADDITION: Stock discovery symbols for broad market scanning
DISCOVERY_SYMBOLS = [
    'SPY', 'QQQ', 'IWM',  # Major ETFs
    'NVDA', 'TSLA', 'AMZN', 'GOOGL', 'META', 'NFLX',  # Tech giants
    'JPM', 'BAC', 'WFC',  # Banking
    'JNJ', 'PFE', 'UNH',  # Healthcare  
    'XOM', 'CVX',  # Energy
    'HD', 'WMT', 'PG',  # Consumer
    'AMD', 'INTC', 'CRM', 'ORCL',  # More tech
    'DIS', 'CMCSA', 'VZ',  # Media/Telecom
]


@dataclass
class ProcessedNewsData:
    """Processed and weighted news data for decision engine"""
    symbol: str
    timestamp: datetime

    # Aggregated sentiment scores (ready for decision engine)
    overall_sentiment_score: float      # -1.0 to 1.0 (negative to positive)
    sentiment_confidence: float         # 0.0 to 1.0 (how confident we are)
    news_volume: int                   # Number of unique articles

    # Source breakdown for analysis
    professional_sentiment: float      # NewsAPI, Alpha Vantage, Marketaux
    social_sentiment: float           # Reddit sentiment
    market_events: List[str]          # Earnings, analyst actions, etc.

    # Key articles for context (limited to most important)
    key_articles: List[NewsArticle] = field(default_factory=list)

    # Metadata
    sources_used: List[str] = field(default_factory=list)
    source_weights: Dict[str, float] = field(default_factory=dict)
    last_updated: datetime = field(default_factory=datetime.now)

    # Quality indicators
    data_quality_score: float = 0.0    # How reliable is this data
    staleness_minutes: int = 0          # How old is the newest article


class EnhancedNewsCollector:
    """
    Pure utility class for enhanced news collection and processing

    Responsibilities:
    - Collect news from multiple sources
    - Deduplicate articles across sources
    - Weight and score sentiment
    - Process into decision-ready format
    """

    def __init__(self):
        """Initialize the enhanced news collector with lazy loading"""
        self.logger = logging.getLogger("enhanced_news_collector")

        # Initialize with empty news sources - will be loaded on demand
        self.news_sources = {}
        self._sources_initialized = False

        # Initialize basic components immediately
        self.logger.info("EnhancedNewsCollector: Basic initialization complete")
        self.logger.info("EnhancedNewsCollector: News sources will be initialized on first use")

    def _ensure_sources_initialized(self):
        """Ensure news sources are initialized (lazy loading)"""
        if not self._sources_initialized:
            self.logger.info("Lazy loading: Initializing news sources on first use...")
            self._initialize_news_sources()
            self._sources_initialized = True
            self.logger.info("Lazy loading: News sources initialization complete")

    def collect_news(self, symbols: List[str], max_articles_per_symbol: int = 10) -> Dict[str, List[NewsArticle]]:
        """Collect news for given symbols with lazy loading"""
        # Ensure sources are initialized before collecting news
        self._ensure_sources_initialized()

        # Call the original collect_news method
        return self._collect_news_original(symbols, max_articles_per_symbol)

    def discover_market_opportunities(self, max_articles_per_symbol: int = 8) -> Dict[str, List[NewsArticle]]:
        """ULTRA CRITICAL: Scan broad market for new trading opportunities"""
        self.logger.info("🔍 DISCOVERY MODE: Scanning broad market for opportunities...")
        
        # Ensure sources are initialized
        self._ensure_sources_initialized()
        
        # Use discovery symbols for market-wide scanning
        discovery_results = self._collect_news_original(DISCOVERY_SYMBOLS, max_articles_per_symbol)
        
        # ULTRA ENHANCEMENT: Parse news articles for additional stock mentions
        mentioned_stocks = self._extract_stock_mentions_from_news(discovery_results)
        
        # Add newly discovered stocks to results if they have enough mentions
        for stock_symbol, mention_data in mentioned_stocks.items():
            if mention_data['mention_count'] >= 3 and stock_symbol not in discovery_results:
                self.logger.info(f"📈 NEWS DISCOVERY: Found {stock_symbol} mentioned {mention_data['mention_count']} times")
                # Add the articles that mentioned this stock
                discovery_results[stock_symbol] = mention_data['articles']
        
        # Log discovery progress with visibility
        total_articles = sum(len(articles) for articles in discovery_results.values())
        symbols_found = len([s for s, articles in discovery_results.items() if articles])
        
        self.logger.info(f"🎯 DISCOVERY RESULTS: {symbols_found}/{len(DISCOVERY_SYMBOLS)} symbols, {total_articles} total articles")
        if mentioned_stocks:
            self.logger.info(f"📈 NEWS MENTIONS: Found {len(mentioned_stocks)} additional stocks in news content")
        
        # Enhanced logging for visibility
        for symbol, articles in discovery_results.items():
            if articles:
                self.logger.info(f"📰 DISCOVERY: {symbol} - {len(articles)} articles found")
                # Log first article title for visibility
                if articles:
                    self.logger.info(f"   🔸 Latest: {articles[0].title[:80]}...")
        
        return discovery_results

    def _extract_stock_mentions_from_news(self, news_results: Dict[str, List[NewsArticle]]) -> Dict[str, Dict]:
        """Extract stock symbol mentions from news articles content"""
        import re
        
        # Common stock symbols to look for in news
        potential_stocks = [
            'MSFT', 'AAPL', 'AMZN', 'GOOGL', 'GOOG', 'META', 'TSLA', 'NVDA', 'AMD', 'INTC',
            'JPM', 'BAC', 'WFC', 'GS', 'MS', 'C', 'V', 'MA', 'PYPL', 'SQ',
            'JNJ', 'PFE', 'UNH', 'ABBV', 'MRK', 'CVS', 'TMO', 'DHR', 'ABT',
            'XOM', 'CVX', 'COP', 'EOG', 'SLB', 'OXY', 'MPC', 'VLO', 'PSX',
            'WMT', 'HD', 'PG', 'KO', 'PEP', 'NKE', 'COST', 'TGT', 'LOW',
            'DIS', 'NFLX', 'CMCSA', 'VZ', 'T', 'ORCL', 'CRM', 'NOW', 'ADBE'
        ]
        
        mentioned_stocks = {}
        
        # Search through all articles for stock mentions
        for symbol, articles in news_results.items():
            for article in articles:
                # Combine title and content for searching
                full_text = f"{article.title} {getattr(article, 'content', '')} {getattr(article, 'description', '')}"
                
                # Look for stock symbols in the text
                for stock in potential_stocks:
                    # Create pattern to find stock mentions (avoid false positives)
                    pattern = rf'\b{stock}\b(?:\s+(?:stock|shares|equity|Corp|Inc|Corporation|Company))?'
                    matches = re.findall(pattern, full_text, re.IGNORECASE)
                    
                    if matches and stock != symbol:  # Don't count the main symbol
                        if stock not in mentioned_stocks:
                            mentioned_stocks[stock] = {
                                'mention_count': 0,
                                'articles': [],
                                'contexts': []
                            }
                        
                        mentioned_stocks[stock]['mention_count'] += len(matches)
                        if article not in mentioned_stocks[stock]['articles']:
                            mentioned_stocks[stock]['articles'].append(article)
                            
                        # Extract context around the mention
                        for match in matches[:2]:  # Max 2 contexts per article
                            start_pos = full_text.find(match)
                            context = full_text[max(0, start_pos-50):start_pos+50]
                            mentioned_stocks[stock]['contexts'].append(context.strip())
        
        # Filter out stocks with too few mentions
        filtered_stocks = {k: v for k, v in mentioned_stocks.items() if v['mention_count'] >= 2}
        
        return filtered_stocks

    def _collect_news_original(self, symbols: List[str], max_articles_per_symbol: int = 10) -> Dict[str, List[NewsArticle]]:
        """Original collect_news implementation"""
        # Source weights for sentiment calculation
        source_weights = {
            'alpha_vantage': 0.30,    # Market-moving news with sentiment
            'fmp': 0.25,              # Earnings, analyst actions
            'reddit': 0.20,           # Social sentiment
            'newsapi': 0.15,          # Professional business news
            'marketaux': 0.10         # Supporting market news
        }

        # Quality thresholds
        min_articles_for_confidence = 3
        max_article_age_hours = 6

        # Rest of the original implementation...
        # (This would be the existing collect_news logic)
        return {}

    def _initialize_news_sources(self):
        """Initialize all available news sources with error handling and timeout protection"""
        import threading
        import time

        # Simplified source list - only initialize the most reliable sources
        source_classes = {
            'alpha_vantage': AlphaVantageNews,
            'fmp': FMPNews,
            # Skip problematic sources for now
            # 'reddit': RedditNews,
            # 'newsapi': NewsAPIData,
            # 'marketaux': MarketauxNews
        }

        self.logger.info(f"Initializing simplified news sources: {list(source_classes.keys())}")

        for source_name, source_class in source_classes.items():
            try:
                self.logger.info(f"[DEBUG] About to initialize {source_name} news source...")

                # Use timeout protection for each source initialization
                init_result = [None]
                init_error = [None]

                def init_source():
                    try:
                        init_result[0] = source_class()
                    except Exception as e:
                        init_error[0] = e

                # Start initialization in a thread with timeout
                init_thread = threading.Thread(target=init_source, daemon=True)
                init_thread.start()

                # Wait for initialization with timeout (reduced for faster startup)
                timeout_seconds = 15  # Reduced timeout
                init_thread.join(timeout=timeout_seconds)

                if init_thread.is_alive():
                    self.logger.warning(f"Initialization of {source_name} timed out after {timeout_seconds} seconds, skipping")
                    continue

                if init_error[0]:
                    if isinstance(init_error[0], ValueError):
                        self.logger.warning(f"Skipping {source_name}: {init_error[0]}")
                    else:
                        self.logger.error(f"Error initializing {source_name}: {init_error[0]}")
                    continue

                if init_result[0]:
                    self.news_sources[source_name] = init_result[0]
                    self.logger.info(f"Initialized {source_name} news source")
                else:
                    self.logger.warning(f"Initialization of {source_name} returned None, skipping")

            except Exception as e:
                self.logger.error(f"Unexpected error initializing {source_name}: {e}")
                continue

        # Log initialization results
        successful_sources = list(self.news_sources.keys())
        self.logger.info(f"News collector initialization complete. Successfully initialized {len(successful_sources)} sources: {successful_sources}")

        if not self.news_sources:
            self.logger.warning("No news sources were successfully initialized. Check API keys and credentials.")
        else:
            self.logger.info(f"Available news sources: {list(self.news_sources.keys())}")

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

        # Collect from all available sources
        all_articles = []
        sources_used = []
        source_article_counts = {}

        for source_name, news_source in self.news_sources.items():
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
                    self.logger.debug(f"Got {len(articles)} articles from {source_name}")
                else:
                    self.logger.debug(f"No articles from {source_name} for {symbol}")

            except Exception as e:
                self.logger.warning(f"Error collecting from {source_name} for {symbol}: {e}")
                # Continue with other sources

        if not all_articles:
            self.logger.info(f"No articles found for {symbol} from any source")
            return self._create_empty_news_data(symbol)

        # Deduplicate articles across sources
        unique_articles = self.deduplicate_articles(all_articles)
        self.logger.info(f"Found {len(all_articles)} total articles, {len(unique_articles)} unique for {symbol}")

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
                    unique_articles = self.deduplicate_articles(symbol_articles)
                    processed_data = self._process_articles_to_data(
                        symbol=symbol,
                        articles=unique_articles,
                        sources_used=list(self.news_sources.keys()),  # Approximate
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

        for source_name, news_source in self.news_sources.items():
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

    def deduplicate_articles(self, articles: List[NewsArticle]) -> List[NewsArticle]:
        """
        Remove duplicate articles using multiple strategies
        Optimized for t3.micro performance constraints
        """
        if not articles:
            return []

        self.logger.debug(f"Deduplicating {len(articles)} articles")

        # Strategy 1: Fast URL-based deduplication
        unique_articles = self._deduplicate_by_url(articles)

        # Strategy 2: Title similarity (for remaining articles)
        if len(unique_articles) > 1:
            unique_articles = self._deduplicate_by_title(unique_articles)

        # Strategy 3: Time-based clustering (for very similar articles)
        if len(unique_articles) > 5:  # Only for larger sets
            unique_articles = self._deduplicate_by_time_clustering(unique_articles)

        self.logger.debug(f"Deduplication complete: {len(unique_articles)} unique articles")
        return unique_articles

    def _deduplicate_by_url(self, articles: List[NewsArticle]) -> List[NewsArticle]:
        """Fast deduplication by normalized URL"""
        seen_urls = set()
        unique_articles = []

        for article in articles:
            normalized_url = self._normalize_url(article.url)

            if normalized_url not in seen_urls:
                seen_urls.add(normalized_url)
                unique_articles.append(article)

        return unique_articles

    def _deduplicate_by_title(self, articles: List[NewsArticle]) -> List[NewsArticle]:
        """Deduplication by normalized title similarity"""
        seen_titles = set()
        unique_articles = []

        for article in articles:
            title_key = self._normalize_title(article.title)

            if title_key not in seen_titles:
                seen_titles.add(title_key)
                unique_articles.append(article)

        return unique_articles

    def _deduplicate_by_time_clustering(self, articles: List[NewsArticle]) -> List[NewsArticle]:
        """Remove articles that are very similar and published within 2 hours"""
        if len(articles) <= 5:
            return articles  # Skip for small sets

        # Sort by publication time
        sorted_articles = sorted(articles, key=lambda x: x.published_date)
        unique_articles = []

        for article in sorted_articles:
            # Check if we have a similar article within 2 hours
            is_duplicate = False

            for existing in unique_articles:
                time_diff = abs((article.published_date - existing.published_date).total_seconds())

                if time_diff < 7200:  # 2 hours
                    # Check title similarity
                    if self._titles_similar(article.title, existing.title):
                        is_duplicate = True
                        break

            if not is_duplicate:
                unique_articles.append(article)

        return unique_articles

    def _normalize_url(self, url: str) -> str:
        """Normalize URL for comparison"""
        if not url:
            return ""

        # Remove query parameters and fragments
        url = url.split('?')[0].split('#')[0]

        # Normalize protocol and www
        url = url.replace('https://', '').replace('http://', '')
        url = url.replace('www.', '')

        return url.lower().strip('/')

    def _normalize_title(self, title: str) -> str:
        """Normalize title for comparison"""
        if not title:
            return ""

        # Remove special characters, convert to lowercase
        normalized = re.sub(r'[^\w\s]', '', title.lower())

        # Remove extra whitespace
        normalized = ' '.join(normalized.split())

        # Create a key from first 50 characters
        return normalized[:50]

    def _titles_similar(self, title1: str, title2: str, threshold: float = 0.7) -> bool:
        """Check if two titles are similar using simple word overlap"""
        words1 = set(self._normalize_title(title1).split())
        words2 = set(self._normalize_title(title2).split())

        if not words1 or not words2:
            return False

        intersection = len(words1.intersection(words2))
        union = len(words1.union(words2))

        similarity = intersection / union if union > 0 else 0
        return similarity >= threshold

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
            source_weights=self.source_weights,
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
            source_name = self._extract_source_name(article.source)
            weight = self.source_weights.get(source_name, 0.1)

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
        confidence += len(set(self._extract_source_name(a.source) for a in articles)) * 0.05
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
            source_name = self._extract_source_name(article.source)
            score += self.source_weights.get(source_name, 0.1) * 10

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
        import math
        engagement_score = math.log(1 + upvotes + comments * 2) / 10
        return min(3.0, max(0.1, engagement_score))  # Between 0.1 and 3.0

    def _extract_source_name(self, source: str) -> str:
        """Extract clean source name from article source"""
        source_lower = source.lower()

        if 'alpha' in source_lower:
            return 'alpha_vantage'
        elif 'fmp' in source_lower:
            return 'fmp'
        elif 'reddit' in source_lower:
            return 'reddit'
        elif 'newsapi' in source_lower:
            return 'newsapi'
        elif 'marketaux' in source_lower:
            return 'marketaux'
        else:
            return 'unknown'

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

    def get_available_sources(self) -> List[str]:
        """Get list of successfully initialized news sources"""
        return list(self.news_sources.keys())

    def get_source_weights(self) -> Dict[str, float]:
        """Get current source weights"""
        return self.source_weights.copy()

    def update_source_weights(self, new_weights: Dict[str, float]):
        """Update source weights for sentiment calculation"""
        self.source_weights.update(new_weights)
        self.logger.info(f"Updated source weights: {self.source_weights}")
