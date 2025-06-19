"""
NewsAPI Data Collector - Cleaned

Professional financial news from NewsAPI.
Sources: CNBC, Reuters, Bloomberg, Financial Times, Business Insider.
"""

import os
import requests
import time
from datetime import datetime, timedelta
from typing import List, Optional, Dict
import logging

try:
    from .base import NewsDataSource, NewsArticle
    from .yahoo_news import SymbolExtractor
except ImportError:
    from base import NewsDataSource, NewsArticle
    from yahoo_news import SymbolExtractor


class NewsAPIData(NewsDataSource):
    """Clean NewsAPI collector optimized for trading decision engine"""

    def __init__(self, api_key: Optional[str] = None):
        super().__init__("NewsAPI")

        self.api_key = api_key or os.getenv('NEWS_API_KEY')
        if not self.api_key:
            raise ValueError("NewsAPI key not found. Set NEWS_API_KEY environment variable.")

        self.base_url = "https://newsapi.org/v2"
        self.session = requests.Session()
        self.daily_requests = 0
        self.max_daily_requests = 500  # Free tier limit
        self.logger = logging.getLogger(f"{__name__}.NewsAPIData")

        # Rate limiting and error tracking
        self.rate_limited = False
        self.rate_limit_detected_at = None
        self.rate_limit_reset_time = None
        self.consecutive_failures = 0
        self.max_consecutive_failures = 3

        self.symbol_extractor = SymbolExtractor()

        # Professional financial sources
        self.premium_sources = "cnbc,reuters,bloomberg,financial-times,business-insider,marketwatch,wsj"

    def _is_api_available(self) -> bool:
        """Check if API is available and not rate limited"""
        if self.rate_limited:
            # Check if enough time has passed since rate limit was detected
            if self.rate_limit_reset_time and datetime.now() < self.rate_limit_reset_time:
                return False
            else:
                # Reset rate limit status after 1 hour
                if self.rate_limit_detected_at and (datetime.now() - self.rate_limit_detected_at).total_seconds() > 3600:
                    self.logger.info("Resetting NewsAPI rate limit status after 1 hour")
                    self.rate_limited = False
                    self.consecutive_failures = 0
                    return True
                return False

        if self.daily_requests >= self.max_daily_requests:
            self.logger.warning("Daily NewsAPI limit reached")
            return False

        return True

    def _handle_api_error(self, response: Optional[requests.Response] = None, error: Optional[Exception] = None) -> bool:
        """Handle API errors and rate limiting. Returns True if should stop making requests"""
        if response is not None:
            if response.status_code == 429:
                self.logger.error("NewsAPI rate limit exceeded (429). Disabling API calls.")
                self.rate_limited = True
                self.rate_limit_detected_at = datetime.now()

                # Try to get reset time from headers
                reset_header = response.headers.get('X-RateLimit-Reset')
                if reset_header:
                    try:
                        reset_timestamp = int(reset_header)
                        self.rate_limit_reset_time = datetime.fromtimestamp(reset_timestamp)
                        self.logger.info(f"Rate limit resets at: {self.rate_limit_reset_time}")
                    except ValueError:
                        # Default to 1 hour if we can't parse reset time
                        self.rate_limit_reset_time = datetime.now() + timedelta(hours=1)
                else:
                    # Default to 1 hour reset time
                    self.rate_limit_reset_time = datetime.now() + timedelta(hours=1)

                return True

            elif response.status_code == 401:
                self.logger.error("NewsAPI authentication failed (401). Check API key.")
                self.rate_limited = True
                return True

            elif response.status_code == 403:
                self.logger.error("NewsAPI access forbidden (403). Account may be suspended.")
                self.rate_limited = True
                return True

        # Handle consecutive failures
        self.consecutive_failures += 1
        if self.consecutive_failures >= self.max_consecutive_failures:
            self.logger.error(f"NewsAPI: {self.consecutive_failures} consecutive failures. Temporarily disabling.")
            self.rate_limited = True
            self.rate_limit_detected_at = datetime.now()
            return True

        return False

    def _make_api_request(self, url: str, params: Dict) -> Optional[Dict]:
        """Make API request with comprehensive error handling"""
        if not self._is_api_available():
            return None

        try:
            response = self.session.get(url, params=params, timeout=30)

            # Check for rate limiting or other errors
            if self._handle_api_error(response=response):
                return None

            response.raise_for_status()
            self.daily_requests += 1

            # Reset consecutive failures on success
            self.consecutive_failures = 0

            data = response.json()
            if data.get('status') != 'ok':
                self.logger.error(f"NewsAPI error: {data.get('message', 'Unknown error')}")
                self.consecutive_failures += 1
                return None

            return data

        except requests.exceptions.HTTPError as e:
            self.logger.error(f"NewsAPI HTTP error: {e}")
            self._handle_api_error(response=getattr(e, 'response', None), error=e)
            return None
        except Exception as e:
            self.logger.error(f"NewsAPI request failed: {e}")
            self._handle_api_error(error=e)
            return None

    def collect_news(self, hours_back: int = 24, max_articles: int = 50) -> List[NewsArticle]:
        """Collect general financial news from NewsAPI"""
        return self._fetch_news_efficiently(hours_back, max_articles)

    def get_symbol_news(self, symbol: str, hours_back: int = 24, max_articles: int = 20) -> List[NewsArticle]:
        """Get news for a specific symbol - CRITICAL for decision engine"""
        try:
            self.logger.info(f"Fetching NewsAPI news for {symbol}")

            # Try direct symbol search first
            articles = self._search_symbol_direct(symbol, hours_back, max_articles)

            if not articles:
                # Fallback: filter from general financial news
                general_articles = self._fetch_news_efficiently(hours_back, 50)
                articles = [
                    article for article in general_articles
                    if (symbol.upper() in article.symbols_mentioned or
                        symbol.lower() in article.title.lower())
                ][:max_articles]

            self.logger.info(f"Found {len(articles)} NewsAPI articles for {symbol}")
            return articles

        except Exception as e:
            self.logger.error(f"Error getting NewsAPI news for {symbol}: {e}")
            return []

    def get_watchlist_news(self, symbols: List[str], hours_back: int = 24,
                          max_articles_per_symbol: int = 10) -> Dict[str, List[NewsArticle]]:
        """Get news for multiple symbols (watchlist) - CRITICAL for decision engine"""
        watchlist_news = {symbol: [] for symbol in symbols}

        try:
            self.logger.info(f"Collecting NewsAPI news for {len(symbols)} watchlist symbols")

            # Strategy: Get general financial news once and distribute to symbols
            general_articles = self._fetch_news_efficiently(hours_back, 100)
            self.logger.info(f"Fetched {len(general_articles)} general NewsAPI articles for filtering")

            # Distribute articles to relevant symbols
            for symbol in symbols:
                try:
                    symbol_articles = [
                        article for article in general_articles
                        if (symbol.upper() in article.symbols_mentioned or
                            symbol.lower() in article.title.lower())
                    ]

                    # Add symbol to symbols_mentioned if found in title
                    for article in symbol_articles:
                        if symbol.upper() not in article.symbols_mentioned:
                            article.symbols_mentioned.append(symbol.upper())

                    # Limit per symbol
                    watchlist_news[symbol] = symbol_articles[:max_articles_per_symbol]

                except Exception as e:
                    self.logger.error(f"Error processing NewsAPI watchlist for {symbol}: {e}")
                    watchlist_news[symbol] = []

            total_articles = sum(len(articles) for articles in watchlist_news.values())
            self.logger.info(f"NewsAPI watchlist: {total_articles} total articles for {len(symbols)} symbols")

        except Exception as e:
            self.logger.error(f"Error in NewsAPI watchlist collection: {e}")

        return watchlist_news

    def test_connection(self) -> bool:
        """Test NewsAPI connection"""
        try:
            url = f"{self.base_url}/top-headlines"
            params = {
                'category': 'business',
                'language': 'en',
                'country': 'us',
                'pageSize': 1,
                'apiKey': self.api_key
            }

            response = self.session.get(url, params=params, timeout=10)

            if response.status_code == 200:
                data = response.json()
                return data.get('status') == 'ok'
            return False

        except Exception as e:
            self.logger.error(f"NewsAPI connection test failed: {e}")
            return False

    def _fetch_news_efficiently(self, hours_back: int, max_articles: int) -> List[NewsArticle]:
        """Fetch financial news using optimized strategy"""
        if not self._is_api_available():
            self.logger.warning("NewsAPI not available due to rate limiting")
            return []

        # Single optimized strategy: Premium sources with financial keywords
        cutoff_time = datetime.now() - timedelta(hours=hours_back)

        url = f"{self.base_url}/everything"
        params = {
            'sources': self.premium_sources,
            'language': 'en',
            'sortBy': 'publishedAt',
            'pageSize': min(100, max_articles),
            'apiKey': self.api_key,
            'from': cutoff_time.isoformat()
        }

        data = self._make_api_request(url, params)
        if not data:
            return self._fallback_to_headlines(max_articles, cutoff_time)

        articles = []
        for article_data in data.get('articles', []):
            try:
                article = self._parse_article(article_data)
                if article:
                    articles.append(article)
            except Exception as e:
                self.logger.debug(f"Error parsing NewsAPI article: {e}")
                continue

        self.logger.info(f"NewsAPI premium sources: {len(articles)} articles")
        return articles[:max_articles]

    def _search_symbol_direct(self, symbol: str, hours_back: int, max_articles: int) -> List[NewsArticle]:
        """Direct search for symbol-specific news"""
        if not self._is_api_available():
            return []

        cutoff_time = datetime.now() - timedelta(hours=hours_back)

        url = f"{self.base_url}/everything"
        params = {
            'q': f'"{symbol}" OR "{symbol.lower()}"',
            'sources': self.premium_sources,
            'language': 'en',
            'sortBy': 'publishedAt',
            'pageSize': min(50, max_articles * 2),
            'apiKey': self.api_key,
            'from': cutoff_time.isoformat()
        }

        data = self._make_api_request(url, params)
        if not data:
            return []

        articles = []
        for article_data in data.get('articles', []):
            try:
                article = self._parse_article(article_data)
                if article:
                    # Ensure symbol is in symbols_mentioned
                    if symbol.upper() not in article.symbols_mentioned:
                        article.symbols_mentioned.append(symbol.upper())
                    articles.append(article)
            except Exception:
                continue

        self.logger.info(f"NewsAPI direct search for {symbol}: {len(articles)} articles")
        return articles

    def _fallback_to_headlines(self, max_articles: int, cutoff_time: datetime) -> List[NewsArticle]:
        """Fallback to business headlines if main strategy fails"""
        if not self._is_api_available():
            return []

        url = f"{self.base_url}/top-headlines"
        params = {
            'category': 'business',
            'language': 'en',
            'country': 'us',
            'pageSize': min(50, max_articles),
            'apiKey': self.api_key
        }

        data = self._make_api_request(url, params)
        if not data:
            return []

        articles = []
        for article_data in data.get('articles', []):
            try:
                article = self._parse_article(article_data)
                if article and article.published_date >= cutoff_time:
                    articles.append(article)
            except Exception:
                continue

        self.logger.info(f"NewsAPI headlines fallback: {len(articles)} articles")
        return articles

    def _parse_article(self, article_data: Dict) -> Optional[NewsArticle]:
        """Parse NewsAPI article data into NewsArticle format"""
        title = article_data.get('title', '').strip()

        if not title or title == '[Removed]' or len(title) < 10:
            return None

        # Parse publication date
        published_str = article_data.get('publishedAt', '')
        try:
            published_date = datetime.fromisoformat(published_str.replace('Z', '+00:00'))
            if published_date.tzinfo is not None:
                published_date = published_date.replace(tzinfo=None)
        except:
            published_date = datetime.now()

        # Get source info
        source_info = article_data.get('source', {})
        source_name = source_info.get('name', 'NewsAPI')

        # Get description/content
        description = article_data.get('description', '') or ''
        content = f"{title}. {description}".strip()

        # Extract symbols from title and description
        full_text = f"{title} {description}"
        symbols = self.symbol_extractor.extract_symbols(full_text)

        # Create article
        article = NewsArticle(
            title=title,
            content=content,
            source=f"NewsAPI-{source_name}",
            url=article_data.get('url', ''),
            published_date=published_date,
            symbols_mentioned=symbols,
            author=article_data.get('author')
        )

        # Note: NewsAPI-specific metadata would include:
        # source_id, source_name, url_to_image, category
        # But these are not stored in the base NewsArticle class

        return article

    def _remove_duplicates(self, articles: List[NewsArticle]) -> List[NewsArticle]:
        """Remove duplicate articles based on title similarity"""
        unique_articles = []
        seen_titles = set()

        for article in articles:
            # Create a normalized title key for comparison
            title_key = article.title[:50].lower().replace(' ', '').replace('-', '').replace('.', '')

            if title_key not in seen_titles:
                seen_titles.add(title_key)
                unique_articles.append(article)

        return unique_articles

    def get_api_usage(self) -> Dict[str, int]:
        """Get current API usage stats"""
        return {
            'requests_used': self.daily_requests,
            'requests_remaining': self.max_daily_requests - self.daily_requests,
            'daily_limit': self.max_daily_requests
        }

    def get_trending_sources(self) -> List[str]:
        """Get list of premium financial sources used"""
        return self.premium_sources.split(',')

