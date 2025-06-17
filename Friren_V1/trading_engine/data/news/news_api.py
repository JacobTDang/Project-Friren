"""
NewsAPI Data Collector - Simplified

Clean NewsAPI integration for professional financial news.
Sources: CNBC, Reuters, Bloomberg, Financial Times, Business Insider.
"""

import os
import requests
import time
from datetime import datetime, timedelta
from typing import List, Optional, Dict
import logging

from base import NewsDataSource, NewsArticle
from yahoo_news import SymbolExtractor


class NewsAPIData(NewsDataSource):
    """Simple NewsAPI collector optimized for decision engine"""

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

        self.symbol_extractor = SymbolExtractor()

    def collect_news(self, hours_back: int = 24, max_articles: int = 50) -> List[NewsArticle]:
        """Collect general financial news from NewsAPI"""
        return self._fetch_news_with_strategies(hours_back, max_articles)

    def get_symbol_news(self, symbol: str, hours_back: int = 24, max_articles: int = 20) -> List[NewsArticle]:
        """Get news specifically for a symbol - CRITICAL for decision engine"""
        try:
            self.logger.info(f"Fetching NewsAPI articles for {symbol}")

            # Search for symbol-specific news
            symbol_articles = self._search_symbol_news(symbol, hours_back, max_articles)

            # Also get general news and filter for symbol mentions
            general_articles = self._fetch_news_with_strategies(hours_back, max_articles * 2)

            # Filter general articles for symbol mentions
            symbol_filtered = [
                article for article in general_articles
                if (symbol.upper() in article.symbols_mentioned or
                    symbol.lower() in article.title.lower())
            ]

            # Combine and deduplicate
            all_articles = symbol_articles + symbol_filtered
            unique_articles = self._remove_duplicates(all_articles)

            self.logger.info(f"Found {len(unique_articles)} NewsAPI articles for {symbol}")
            return unique_articles[:max_articles]

        except Exception as e:
            self.logger.error(f"Error getting NewsAPI news for {symbol}: {e}")
            return []

    def get_watchlist_news(self, symbols: List[str], hours_back: int = 24, max_articles_per_symbol: int = 10) -> Dict[str, List[NewsArticle]]:
        """Get news for multiple symbols (watchlist) - CRITICAL for decision engine"""
        watchlist_news = {}

        # Get general news once and filter for all symbols
        try:
            general_articles = self._fetch_news_with_strategies(hours_back, 100)
            self.logger.info(f"Fetched {len(general_articles)} general NewsAPI articles for watchlist filtering")

            for symbol in symbols:
                try:
                    # Filter general articles for this symbol
                    symbol_articles = [
                        article for article in general_articles
                        if (symbol.upper() in article.symbols_mentioned or
                            symbol.lower() in article.title.lower())
                    ]

                    # Also try symbol-specific search if we have API calls left
                    if self.daily_requests < self.max_daily_requests - 10:  # Leave some buffer
                        specific_articles = self._search_symbol_news(symbol, hours_back, max_articles_per_symbol)
                        symbol_articles.extend(specific_articles)

                    # Deduplicate and limit
                    unique_articles = self._remove_duplicates(symbol_articles)
                    watchlist_news[symbol] = unique_articles[:max_articles_per_symbol]

                    # Rate limiting between symbols
                    time.sleep(0.5)

                except Exception as e:
                    self.logger.error(f"Error getting NewsAPI watchlist news for {symbol}: {e}")
                    watchlist_news[symbol] = []

            total_articles = sum(len(articles) for articles in watchlist_news.values())
            self.logger.info(f"NewsAPI watchlist: {total_articles} total articles for {len(symbols)} symbols")

        except Exception as e:
            self.logger.error(f"Error in NewsAPI watchlist collection: {e}")
            # Return empty results for all symbols
            watchlist_news = {symbol: [] for symbol in symbols}

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

        except Exception:
            return False

    def _fetch_news_with_strategies(self, hours_back: int, max_articles: int) -> List[NewsArticle]:
        """Fetch news using multiple search strategies"""
        if self.daily_requests >= self.max_daily_requests:
            self.logger.warning("Daily NewsAPI limit reached")
            return []

        # Try different search strategies
        strategies = [
            {"q": "stock market", "sources": None},
            {"q": "earnings", "sources": None},
            {"q": "*", "sources": "cnbc,reuters,bloomberg,financial-times,business-insider"},
            {"q": "finance", "sources": None}
        ]

        all_articles = []
        cutoff_time = datetime.now() - timedelta(hours=hours_back)

        for i, strategy in enumerate(strategies):
            if self.daily_requests >= self.max_daily_requests:
                break

            try:
                articles = self._execute_search_strategy(strategy, max_articles, cutoff_time)
                if articles:
                    all_articles.extend(articles)
                    self.logger.info(f"NewsAPI strategy {i+1}: {len(articles)} articles")

                    if len(all_articles) >= max_articles:
                        break

            except Exception as e:
                self.logger.error(f"NewsAPI strategy {i+1} failed: {e}")
                continue

        # Fallback to headlines if no articles found
        if not all_articles and self.daily_requests < self.max_daily_requests:
            try:
                headlines = self._get_business_headlines(max_articles, cutoff_time)
                all_articles.extend(headlines)
                self.logger.info(f"NewsAPI fallback headlines: {len(headlines)} articles")
            except Exception as e:
                self.logger.error(f"NewsAPI headlines fallback failed: {e}")

        unique_articles = self._remove_duplicates(all_articles)
        return unique_articles[:max_articles]

    def _execute_search_strategy(self, strategy: Dict, max_articles: int, cutoff_time: datetime) -> List[NewsArticle]:
        """Execute a single search strategy"""
        url = f"{self.base_url}/everything"
        params = {
            'language': 'en',
            'sortBy': 'publishedAt',
            'pageSize': min(50, max_articles),
            'apiKey': self.api_key,
            'from': cutoff_time.isoformat()
        }

        if strategy["sources"]:
            params['sources'] = strategy["sources"]
        else:
            params['q'] = strategy["q"]

        response = self.session.get(url, params=params, timeout=30)
        response.raise_for_status()
        self.daily_requests += 1

        data = response.json()
        if data['status'] != 'ok':
            self.logger.error(f"NewsAPI error: {data.get('message', 'Unknown error')}")
            return []

        articles = []
        for article_data in data.get('articles', []):
            try:
                article = self._parse_article(article_data)
                if article:
                    articles.append(article)
            except Exception as e:
                self.logger.debug(f"Error parsing NewsAPI article: {e}")
                continue

        return articles

    def _search_symbol_news(self, symbol: str, hours_back: int, max_articles: int) -> List[NewsArticle]:
        """Search for news specific to a symbol"""
        if self.daily_requests >= self.max_daily_requests:
            return []

        try:
            cutoff_time = datetime.now() - timedelta(hours=hours_back)

            url = f"{self.base_url}/everything"
            params = {
                'q': f'"{symbol}" OR "{symbol.lower()}"',
                'language': 'en',
                'sortBy': 'publishedAt',
                'pageSize': min(30, max_articles),
                'apiKey': self.api_key,
                'from': cutoff_time.isoformat()
            }

            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            self.daily_requests += 1

            data = response.json()
            if data['status'] != 'ok':
                return []

            articles = []
            for article_data in data.get('articles', []):
                try:
                    article = self._parse_article(article_data)
                    if article and symbol.upper() in article.symbols_mentioned:
                        articles.append(article)
                except Exception:
                    continue

            return articles

        except Exception as e:
            self.logger.error(f"Error searching NewsAPI for {symbol}: {e}")
            return []

    def _get_business_headlines(self, max_articles: int, cutoff_time: datetime) -> List[NewsArticle]:
        """Get top business headlines as fallback"""
        url = f"{self.base_url}/top-headlines"
        params = {
            'category': 'business',
            'language': 'en',
            'country': 'us',
            'pageSize': min(50, max_articles),
            'apiKey': self.api_key
        }

        response = self.session.get(url, params=params, timeout=30)
        response.raise_for_status()
        self.daily_requests += 1

        data = response.json()
        if data['status'] != 'ok':
            return []

        articles = []
        for article_data in data.get('articles', []):
            try:
                article = self._parse_article(article_data)
                if article and article.published_date >= cutoff_time:
                    articles.append(article)
            except Exception:
                continue

        return articles

    def _parse_article(self, article_data: Dict) -> Optional[NewsArticle]:
        """Parse NewsAPI article data into NewsArticle"""
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
        source_name = source_info.get('name', 'Unknown')

        # Get description/content
        description = article_data.get('description', '') or ''
        content = f"{title}. {description}".strip()

        # Extract symbols from title and description
        full_text = f"{title} {description}"
        symbols = self.symbol_extractor.extract_symbols(full_text)

        return NewsArticle(
            title=title,
            content=content,
            source=f"NewsAPI-{source_name}",
            url=article_data.get('url', ''),
            published_date=published_date,
            symbols_mentioned=symbols,
            author=article_data.get('author')
        )

    def _remove_duplicates(self, articles: List[NewsArticle]) -> List[NewsArticle]:
        """Remove duplicate articles"""
        unique_articles = []
        seen_titles = set()

        for article in articles:
            title_key = article.title[:30].lower().replace(' ', '')
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
