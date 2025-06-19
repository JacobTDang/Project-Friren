"""
Financial Modeling Prep (FMP) News Data Collector - FREE TIER VERSION

Professional financial news from FMP API using only FREE tier endpoints.
Free tier limitations: 250 requests/day, US stocks only, limited endpoints.
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


class FMPNews(NewsDataSource):
    """Financial Modeling Prep API news collector - FREE TIER ONLY"""

    def __init__(self, api_key: Optional[str] = None):
        super().__init__("FMP")

        # Try to get API key from parameter first, then environment
        self.api_key = api_key or os.getenv('FMP_API_KEY')

        if not self.api_key:
            raise ValueError(
                "FMP API key not found. Set FMP_API_KEY environment variable or pass api_key parameter."
            )

        self.base_url = "https://financialmodelingprep.com/api/v3"
        self.base_url_v4 = "https://financialmodelingprep.com/api/v4"
        self.session = requests.Session()
        self.daily_requests = 0
        self.max_daily_requests = 250  # Free tier limit
        self.logger = logging.getLogger(f"{__name__}.FMPNews")

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
                self.logger.info("Resetting FMP rate limit status after 1 hour")
                self.rate_limited = False
                self.consecutive_failures = 0
                return True
            return False

        if self.daily_requests >= self.max_daily_requests:
            self.logger.warning("Daily FMP limit reached")
            return False

        return True

    def _handle_api_error(self, response: Optional[requests.Response] = None, error: Optional[Exception] = None) -> bool:
        """Handle API errors and rate limiting. Returns True if should stop making requests"""
        if response is not None:
            if response.status_code == 429:
                self.logger.error("FMP rate limit exceeded (429). Disabling API calls.")
                self.rate_limited = True
                self.rate_limit_detected_at = datetime.now()
                return True

            elif response.status_code == 401:
                self.logger.error("FMP authentication failed (401). Check API key.")
                self.rate_limited = True
                return True

            elif response.status_code == 403:
                self.logger.error("FMP access forbidden (403). Account may be suspended.")
                self.rate_limited = True
                return True

        # Handle consecutive failures
        self.consecutive_failures += 1
        if self.consecutive_failures >= self.max_consecutive_failures:
            self.logger.error(f"FMP: {self.consecutive_failures} consecutive failures. Temporarily disabling.")
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

            # FMP returns array directly or error object
            if isinstance(data, dict) and ('Error Message' in data or 'message' in data):
                error_msg = data.get('Error Message', data.get('message', 'Unknown error'))
                self.logger.error(f"FMP API error: {error_msg}")
                self.consecutive_failures += 1
                return None

            return data

        except requests.exceptions.HTTPError as e:
            self.logger.error(f"FMP HTTP error: {e}")
            self._handle_api_error(response=getattr(e, 'response', None), error=e)
            return None
        except Exception as e:
            self.logger.error(f"FMP request failed: {e}")
            self._handle_api_error(error=e)
            return None

    def collect_news(self, hours_back: int = 24, max_articles: int = 50) -> List[NewsArticle]:
        """Collect general financial news from FMP - FREE TIER"""
        try:
            self.logger.info("Collecting general news from FMP (FREE TIER)")

            # FMP general articles endpoint (ORIGINAL WORKING VERSION)
            url = f"{self.base_url}/fmp/articles"
            params = {
                'apikey': self.api_key,
                'page': 0,
                'size': min(20, max_articles)  # Free tier limit
            }

            data = self._make_api_request(url, params)

            if data is None:
                return []

            articles = []
            cutoff_time = datetime.now() - timedelta(hours=hours_back)

            for article_data in data:
                try:
                    article = self._parse_fmp_article(article_data)
                    if article and article.published_date >= cutoff_time:
                        articles.append(article)
                except Exception as e:
                    self.logger.debug(f"Error parsing FMP article: {e}")
                    continue

            self.logger.info(f"Collected {len(articles)} articles from FMP")
            return articles[:max_articles]

        except Exception as e:
            self.logger.error(f"Error collecting FMP news: {e}")
            return []

    def get_symbol_news(self, symbol: str, hours_back: int = 24, max_articles: int = 20) -> List[NewsArticle]:
        """Get news for a symbol - FREE TIER (limited by filtering general news)"""
        try:
            self.logger.info(f"Fetching FMP news for {symbol} (filtering from general news)")

            symbol_articles = []

            # Only get press releases to save API calls (1 request instead of 2)
            if self.daily_requests < self.max_daily_requests - 5:  # Save some requests
                try:
                    press_releases = self.get_press_releases(symbol, max_articles=max_articles)
                    symbol_articles.extend(press_releases)
                except Exception as e:
                    self.logger.debug(f"Press releases not available: {e}")

            self.logger.info(f"Found {len(symbol_articles)} FMP articles for {symbol}")
            return symbol_articles[:max_articles]

        except Exception as e:
            self.logger.error(f"Error getting FMP news for {symbol}: {e}")
            return []

    def get_watchlist_news(self, symbols: List[str], hours_back: int = 24,
                          max_articles_per_symbol: int = 10) -> Dict[str, List[NewsArticle]]:
        """Get news for multiple symbols (watchlist) - FREE TIER"""
        watchlist_news = {}

        try:
            self.logger.info(f"Collecting FMP news for {len(symbols)} watchlist symbols")

            # Only get one general articles call to save API requests
            general_articles = self.collect_news(hours_back=hours_back, max_articles=30)

            # Initialize results for all symbols
            for symbol in symbols:
                watchlist_news[symbol] = []

            # Filter articles for each symbol
            for symbol in symbols:
                try:
                    symbol_articles = []

                    # Filter from general articles
                    for article in general_articles:
                        if (symbol.upper() in article.symbols_mentioned or
                            symbol.lower() in article.title.lower() or
                            symbol.upper() in article.title.upper() or
                            symbol.lower() in article.content.lower()):

                            # Ensure symbol is in symbols_mentioned
                            if symbol.upper() not in article.symbols_mentioned:
                                article.symbols_mentioned.append(symbol.upper())

                            symbol_articles.append(article)

                    # Remove duplicates and limit
                    unique_articles = self._remove_duplicates(symbol_articles)
                    watchlist_news[symbol] = unique_articles[:max_articles_per_symbol]

                except Exception as e:
                    self.logger.error(f"Error processing watchlist for {symbol}: {e}")
                    watchlist_news[symbol] = []

            total_articles = sum(len(articles) for articles in watchlist_news.values())
            self.logger.info(f"FMP watchlist complete: {total_articles} total articles for {len(symbols)} symbols")

        except Exception as e:
            self.logger.error(f"Error in FMP watchlist collection: {e}")
            watchlist_news = {symbol: [] for symbol in symbols}

        return watchlist_news

    def get_press_releases(self, symbol: str, max_articles: int = 10) -> List[NewsArticle]:
        """Get press releases for a symbol - FREE TIER AVAILABLE"""
        try:
            self.logger.info(f"Fetching press releases for {symbol}")

            url = f"{self.base_url}/press-releases/{symbol.upper()}"
            params = {
                'apikey': self.api_key,
                'limit': max_articles
            }

            data = self._make_api_request(url, params)

            if data is None:
                return []

            articles = []
            for release_data in data:
                try:
                    article = self._parse_fmp_press_release(release_data, symbol)
                    if article:
                        articles.append(article)
                except Exception as e:
                    self.logger.debug(f"Error parsing press release: {e}")
                    continue

            self.logger.info(f"Found {len(articles)} press releases for {symbol}")
            return articles

        except Exception as e:
            self.logger.debug(f"Error getting press releases for {symbol}: {e}")
            return []

    def get_market_gainers_losers(self) -> Dict[str, List[str]]:
        """Get market gainers/losers - FREE TIER AVAILABLE"""
        try:
            self.logger.info("Getting market gainers and losers")

            gainers = []
            losers = []

            # Get gainers
            try:
                gainers_url = f"{self.base_url}/stock_market/gainers"
                params = {'apikey': self.api_key}

                response = self.session.get(gainers_url, params=params, timeout=30)
                response.raise_for_status()
                self.daily_requests += 1

                gainers_data = response.json()

                if isinstance(gainers_data, list):
                    gainers = [stock.get('symbol', '') for stock in gainers_data[:10]]

            except Exception as e:
                self.logger.debug(f"Error getting gainers: {e}")

            # Get losers
            try:
                losers_url = f"{self.base_url}/stock_market/losers"
                params = {'apikey': self.api_key}

                response = self.session.get(losers_url, params=params, timeout=30)
                response.raise_for_status()
                self.daily_requests += 1

                losers_data = response.json()

                if isinstance(losers_data, list):
                    losers = [stock.get('symbol', '') for stock in losers_data[:10]]

            except Exception as e:
                self.logger.debug(f"Error getting losers: {e}")

            return {
                'gainers': [s for s in gainers if s],
                'losers': [s for s in losers if s]
            }

        except Exception as e:
            self.logger.error(f"Error getting market movers: {e}")
            return {'gainers': [], 'losers': []}

    def test_connection(self) -> bool:
        """Test FMP API connection - ORIGINAL WORKING ENDPOINT"""
        try:
            url = f"{self.base_url}/fmp/articles"
            params = {
                'apikey': self.api_key,
                'page': 0,
                'size': 1  # Minimal request
            }

            response = self.session.get(url, params=params, timeout=10)

            if response.status_code == 200:
                data = response.json()
                # Check for valid response (array or valid data)
                return isinstance(data, list) or (isinstance(data, dict) and 'Error Message' not in data and 'message' not in data)
            return False

        except Exception as e:
            self.logger.error(f"FMP connection test failed: {e}")
            return False

    def _parse_fmp_article(self, article_data: Dict) -> Optional[NewsArticle]:
        """Parse FMP general article data into NewsArticle format"""
        try:
            title = article_data.get('title', '').strip()

            if not title or len(title) < 10:
                return None

            # Parse publication date
            published_str = article_data.get('date', '') or article_data.get('publishedDate', '')
            try:
                # FMP format variations
                if 'T' in published_str:
                    published_date = datetime.fromisoformat(published_str.replace('Z', ''))
                else:
                    published_date = datetime.strptime(published_str, '%Y-%m-%d %H:%M:%S')
            except:
                published_date = datetime.now()

            # Get content
            content = (article_data.get('content', '') or
                      article_data.get('text', '') or
                      article_data.get('snippet', '') or title)

            # Extract symbols from title and content
            symbols = self.symbol_extractor.extract_symbols(f"{title} {content}")

            # Create NewsArticle
            article = NewsArticle(
                title=title,
                content=content,
                source="FMP",
                url=article_data.get('url', ''),
                published_date=published_date,
                symbols_mentioned=symbols,
                author=article_data.get('author', '')
            )

            # Note: FMP-specific metadata would include image, site, text
            # But these are not stored in the base NewsArticle class

            return article

        except Exception as e:
            self.logger.debug(f"Error parsing FMP article: {e}")
            return None

    def _parse_fmp_press_release(self, release_data: Dict, symbol: str) -> Optional[NewsArticle]:
        """Parse FMP press release data into NewsArticle format"""
        try:
            title = release_data.get('title', '').strip()

            if not title or len(title) < 10:
                return None

            # Parse publication date
            published_str = release_data.get('date', '')
            try:
                published_date = datetime.strptime(published_str, '%Y-%m-%d %H:%M:%S')
            except:
                published_date = datetime.now()

            # Get content
            text = release_data.get('text', '') or ''

            # Symbols - include the requested symbol
            symbols = [symbol.upper()]
            text_symbols = self.symbol_extractor.extract_symbols(f"{title} {text}")
            symbols.extend(text_symbols)

            # Remove duplicates
            unique_symbols = list(dict.fromkeys(symbols))

            # Create article
            article = NewsArticle(
                title=title,
                content=text,
                source="FMP",
                url=release_data.get('url', ''),
                published_date=published_date,
                symbols_mentioned=unique_symbols,
                author="Press Release"
            )

            # Note: FMP-specific metadata would include symbol, category
            # But these are not stored in the base NewsArticle class

            return article

        except Exception as e:
            self.logger.debug(f"Error parsing FMP press release: {e}")
            return None

    def _remove_duplicates(self, articles: List[NewsArticle]) -> List[NewsArticle]:
        """Remove duplicate articles based on title"""
        seen_titles = set()
        unique_articles = []

        for article in articles:
            if article.title not in seen_titles:
                seen_titles.add(article.title)
                unique_articles.append(article)

        return unique_articles

    def get_api_usage(self) -> Dict[str, int]:
        """Get current API usage stats"""
        return {
            'requests_used': self.daily_requests,
            'requests_remaining': self.max_daily_requests - self.daily_requests,
            'daily_limit': self.max_daily_requests
        }

