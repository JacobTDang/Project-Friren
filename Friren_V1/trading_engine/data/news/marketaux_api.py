"""
Marketaux News Data Collector

Real-time financial news from Marketaux API.
Focused on market analysis and trading-relevant news.
"""

import os
from pathlib import Path
from dotenv import load_dotenv
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


class MarketauxNews(NewsDataSource):
    """Marketaux API news collector for real-time financial news"""

    def __init__(self, api_key: Optional[str] = None):
        super().__init__("Marketaux")
        self.api_key = api_key or os.getenv('MARKETAUX_API_KEY')
        if not self.api_key:
            raise ValueError("Marketaux API key not found. Set MARKETAUX_API_KEY environment variable.")

        self.base_url = "https://api.marketaux.com/v1"
        self.session = requests.Session()
        self.daily_requests = 0
        self.max_daily_requests = 100  # Free tier limit
        self.logger = logging.getLogger(f"{__name__}.MarketauxNews")

        # Rate limiting and error tracking
        self.rate_limited = False
        self.rate_limit_detected_at = None
        self.consecutive_failures = 0
        self.max_consecutive_failures = 3

        self.symbol_extractor = SymbolExtractor()

    def _is_api_available(self) -> bool:
        """Check if API is available and not rate limited"""
        if self.rate_limited:
            # Reset rate limit status after 1 hour
            if self.rate_limit_detected_at and (datetime.now() - self.rate_limit_detected_at).total_seconds() > 3600:
                self.logger.info("Resetting Marketaux rate limit status after 1 hour")
                self.rate_limited = False
                self.consecutive_failures = 0
                return True
            return False

        if self.daily_requests >= self.max_daily_requests:
            self.logger.warning("Daily Marketaux limit reached")
            return False

        return True

    def _handle_api_error(self, response: Optional[requests.Response] = None, error: Optional[Exception] = None) -> bool:
        """Handle API errors and rate limiting. Returns True if should stop making requests"""
        if response is not None:
            if response.status_code == 429:
                self.logger.error("Marketaux rate limit exceeded (429). Disabling API calls.")
                self.rate_limited = True
                self.rate_limit_detected_at = datetime.now()
                return True

            elif response.status_code == 401:
                self.logger.error("Marketaux authentication failed (401). Check API key.")
                self.rate_limited = True
                return True

            elif response.status_code == 402:
                self.logger.error("Marketaux payment required (402). Account may need upgrade.")
                self.rate_limited = True
                return True

            elif response.status_code == 403:
                self.logger.error("Marketaux access forbidden (403). Account may be suspended.")
                self.rate_limited = True
                return True

        # Handle consecutive failures
        self.consecutive_failures += 1
        if self.consecutive_failures >= self.max_consecutive_failures:
            self.logger.error(f"Marketaux: {self.consecutive_failures} consecutive failures. Temporarily disabling.")
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
            return data

        except requests.exceptions.HTTPError as e:
            self.logger.error(f"Marketaux HTTP error: {e}")
            self._handle_api_error(response=getattr(e, 'response', None), error=e)
            return None
        except Exception as e:
            self.logger.error(f"Marketaux request failed: {e}")
            self._handle_api_error(error=e)
            return None

    def collect_news(self, hours_back: int = 24, max_articles: int = 50) -> List[NewsArticle]:
        """Collect general financial news from Marketaux"""
        try:
            self.logger.info("Collecting general news from Marketaux")

            # Calculate time filter
            published_after = datetime.now() - timedelta(hours=hours_back)

            # Marketaux news endpoint
            url = f"{self.base_url}/news/all"
            params = {
                'api_token': self.api_key,
                'language': 'en',
                'countries': 'us',
                'filter_entities': 'true',  # Only articles with stock entities
                'limit': min(50, max_articles),  # API limit is 100
                'published_after': published_after.strftime('%Y-%m-%dT%H:%M:%S'),
                'sort': 'published_desc'
            }

            data = self._make_api_request(url, params)
            if data:
                articles = []

                for article_data in data.get('data', []):
                    try:
                        article = self._parse_marketaux_article(article_data)
                        if article:
                            articles.append(article)
                    except Exception as e:
                        self.logger.debug(f"Error parsing Marketaux article: {e}")
                        continue

                self.logger.info(f"Collected {len(articles)} articles from Marketaux")
                return articles[:max_articles]

        except Exception as e:
            self.logger.error(f"Error collecting Marketaux news: {e}")

        return []

    def get_symbol_news(self, symbol: str, hours_back: int = 24, max_articles: int = 20) -> List[NewsArticle]:
        """Get news specifically for a symbol from Marketaux - KEY for decision engine"""
        try:
            self.logger.info(f"Fetching Marketaux news for {symbol}")

            # Calculate time filter
            published_after = datetime.now() - timedelta(hours=hours_back)

            # Symbol-specific news endpoint
            url = f"{self.base_url}/news/all"
            params = {
                'api_token': self.api_key,
                'symbols': symbol.upper(),  # Marketaux uses symbols parameter
                'language': 'en',
                'countries': 'us',
                'filter_entities': 'true',
                'limit': min(50, max_articles),
                'published_after': published_after.strftime('%Y-%m-%dT%H:%M:%S'),
                'sort': 'published_desc'
            }

            data = self._make_api_request(url, params)
            if data:
                articles = []

                for article_data in data.get('data', []):
                    try:
                        article = self._parse_marketaux_article(article_data)
                        if article:
                            # Ensure the target symbol is in symbols_mentioned
                            if symbol.upper() not in article.symbols_mentioned:
                                article.symbols_mentioned.append(symbol.upper())
                            articles.append(article)
                    except Exception as e:
                        self.logger.debug(f"Error parsing Marketaux article for {symbol}: {e}")
                        continue

                self.logger.info(f"Found {len(articles)} Marketaux articles for {symbol}")
                return articles[:max_articles]

        except Exception as e:
            self.logger.error(f"Error getting Marketaux news for {symbol}: {e}")
            return []

    def get_watchlist_news(self, symbols: List[str], hours_back: int = 24,
                          max_articles_per_symbol: int = 10) -> Dict[str, List[NewsArticle]]:
        """Get news for multiple symbols (watchlist) from Marketaux - KEY for decision engine"""
        watchlist_news = {}

        try:
            self.logger.info(f"Collecting Marketaux news for {len(symbols)} watchlist symbols")

            # Marketaux supports multiple symbols in one call (comma-separated)
            symbols_str = ','.join(symbol.upper() for symbol in symbols)

            # Calculate time filter
            published_after = datetime.now() - timedelta(hours=hours_back)

            # Batch request for all symbols
            url = f"{self.base_url}/news/all"
            params = {
                'api_token': self.api_key,
                'symbols': symbols_str,
                'language': 'en',
                'countries': 'us',
                'filter_entities': 'true',
                'limit': 100,  # Get more articles for distribution
                'published_after': published_after.strftime('%Y-%m-%dT%H:%M:%S'),
                'sort': 'published_desc'
            }

            data = self._make_api_request(url, params)
            if data:
                # Initialize results for all symbols
                for symbol in symbols:
                    watchlist_news[symbol] = []

                # Parse and distribute articles
                for article_data in data.get('data', []):
                    try:
                        article = self._parse_marketaux_article(article_data)
                        if article:
                            # Distribute article to relevant symbols
                            for symbol in symbols:
                                if (symbol.upper() in article.symbols_mentioned or
                                    symbol.lower() in article.title.lower()):

                                    # Ensure symbol is in symbols_mentioned
                                    if symbol.upper() not in article.symbols_mentioned:
                                        article.symbols_mentioned.append(symbol.upper())

                                    # Add to symbol's articles if under limit
                                    if len(watchlist_news[symbol]) < max_articles_per_symbol:
                                        watchlist_news[symbol].append(article)

                    except Exception as e:
                        self.logger.debug(f"Error parsing Marketaux watchlist article: {e}")
                        continue

                total_articles = sum(len(articles) for articles in watchlist_news.values())
                self.logger.info(f"Marketaux watchlist complete: {total_articles} total articles for {len(symbols)} symbols")

        except Exception as e:
            self.logger.error(f"Error in Marketaux watchlist collection: {e}")
            watchlist_news = {symbol: [] for symbol in symbols}

        return watchlist_news

    def test_connection(self) -> bool:
        """Test Marketaux API connection"""
        try:
            url = f"{self.base_url}/news/all"
            params = {
                'api_token': self.api_key,
                'limit': 1  # Minimal request
            }

            response = self.session.get(url, params=params, timeout=10)

            if response.status_code == 200:
                data = response.json()
                return 'data' in data  # Marketaux returns data array
            return False

        except Exception as e:
            self.logger.error(f"Marketaux connection test failed: {e}")
            return False

    def _parse_marketaux_article(self, article_data: Dict) -> Optional[NewsArticle]:
        """Parse Marketaux article data into NewsArticle format"""
        try:
            title = article_data.get('title', '').strip()

            if not title or len(title) < 10:
                return None

            # Parse publication date
            published_str = article_data.get('published_at', '')
            try:
                # Marketaux format: "2023-12-07T14:30:00.000000Z"
                published_date = datetime.fromisoformat(published_str.replace('Z', '+00:00'))
                if published_date.tzinfo is not None:
                    published_date = published_date.replace(tzinfo=None)
            except:
                published_date = datetime.now()

            # Get content/description
            description = article_data.get('description', '') or ''
            snippet = article_data.get('snippet', '') or ''
            content = description or snippet or title

            # Extract symbols from Marketaux entities (they provide this!)
            symbols = []
            entities = article_data.get('entities', [])

            for entity in entities:
                if entity.get('type') == 'equity':  # Stock entities
                    symbol = entity.get('symbol', '').upper()
                    if symbol and len(symbol) <= 5:  # Valid symbol length
                        symbols.append(symbol)

            # Also extract symbols from text as backup
            text_symbols = self.symbol_extractor.extract_symbols(f"{title} {content}")
            symbols.extend(text_symbols)

            # Remove duplicates while preserving order
            unique_symbols = []
            seen = set()
            for symbol in symbols:
                if symbol not in seen:
                    unique_symbols.append(symbol)
                    seen.add(symbol)

            # Create article
            article = NewsArticle(
                title=title,
                content=content,
                source="Marketaux",
                url=article_data.get('url', ''),
                published_date=published_date,
                symbols_mentioned=unique_symbols,
                author=article_data.get('source', '')  # Marketaux provides source/publisher
            )

            # Add Marketaux-specific metadata
            article.additional_metadata = {
                'source_name': article_data.get('source', ''),
                'source_domain': article_data.get('source_url', ''),
                'entities': entities,
                'language': article_data.get('language', 'en'),
                'countries': article_data.get('countries', []),
                'similar_articles': len(article_data.get('similar', []))
            }

            return article

        except Exception as e:
            self.logger.debug(f"Error parsing Marketaux article: {e}")
            return None

    def get_api_usage(self) -> Dict[str, int]:
        """Get current API usage stats"""
        return {
            'requests_used': self.daily_requests,
            'requests_remaining': self.max_daily_requests - self.daily_requests,
            'daily_limit': self.max_daily_requests
        }

    def get_trending_symbols(self, hours_back: int = 24, limit: int = 20) -> List[Dict[str, any]]:
        """Get trending symbols based on news volume (Marketaux specific feature)"""
        try:
            self.logger.info("Getting trending symbols from Marketaux")

            published_after = datetime.now() - timedelta(hours=hours_back)

            url = f"{self.base_url}/news/all"
            params = {
                'api_token': self.api_key,
                'language': 'en',
                'countries': 'us',
                'filter_entities': 'true',
                'limit': 100,  # Get lots of articles to analyze trends
                'published_after': published_after.strftime('%Y-%m-%dT%H:%M:%S'),
                'sort': 'published_desc'
            }

            data = self._make_api_request(url, params)
            if data:
                # Count symbol mentions
                symbol_counts = {}

                for article_data in data.get('data', []):
                    entities = article_data.get('entities', [])
                    for entity in entities:
                        if entity.get('type') == 'equity':
                            symbol = entity.get('symbol', '').upper()
                            if symbol:
                                symbol_counts[symbol] = symbol_counts.get(symbol, 0) + 1

                # Sort by mention count
                trending = [
                    {'symbol': symbol, 'mention_count': count}
                    for symbol, count in sorted(symbol_counts.items(), key=lambda x: x[1], reverse=True)
                ]

                return trending[:limit]

        except Exception as e:
            self.logger.error(f"Error getting trending symbols: {e}")
            return []

