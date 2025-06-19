"""
Alpha Vantage News Data Collector

Professional financial news and market analysis from Alpha Vantage API.
Focused on earnings, analyst upgrades/downgrades, and market-moving news.
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


class AlphaVantageNews(NewsDataSource):
    """Alpha Vantage API news collector for professional financial news"""

    def __init__(self, api_key: Optional[str] = None):
        super().__init__("AlphaVantage")

        # Try to get API key from parameter first, then environment
        self.api_key = api_key or os.getenv('ALPHA_VANTAGE_API_KEY')

        # Debug: Show what we found
        if self.api_key:
            print(f"Alpha Vantage API key found: {self.api_key[:8]}...")
        else:
            print("Alpha Vantage API key not found")
            print("Available environment variables:")
            for key in sorted(os.environ.keys()):
                if 'ALPHA' in key or 'VANTAGE' in key or 'API' in key:
                    value = os.environ.get(key, '')
                    print(f"  {key}={value[:8]}..." if value else f"  {key}=(empty)")

        if not self.api_key:
            raise ValueError(
                "Alpha Vantage API key not found. Set ALPHA_VANTAGE_API_KEY environment variable or pass api_key parameter."
            )

        self.base_url = "https://www.alphavantage.co/query"
        self.session = requests.Session()
        self.daily_requests = 0
        self.max_daily_requests = 25  # Free tier limit
        self.logger = logging.getLogger(f"{__name__}.AlphaVantageNews")

        self.symbol_extractor = SymbolExtractor()

    def collect_news(self, hours_back: int = 24, max_articles: int = 50) -> List[NewsArticle]:
        """Collect general financial news from Alpha Vantage"""
        try:
            self.logger.info("Collecting general news from Alpha Vantage")

            # Alpha Vantage NEWS_SENTIMENT endpoint for general news
            params = {
                'function': 'NEWS_SENTIMENT',
                'apikey': self.api_key,
                'limit': min(50, max_articles),  # API allows up to 50
                'sort': 'LATEST'
            }

            # Add time filter if needed
            if hours_back < 168:  # If less than a week, add time filter
                time_from = (datetime.now() - timedelta(hours=hours_back)).strftime('%Y%m%dT%H%M')
                params['time_from'] = time_from

            response = self.session.get(self.base_url, params=params, timeout=30)
            response.raise_for_status()
            self.daily_requests += 1

            data = response.json()

            # Check for API errors
            if 'Error Message' in data:
                self.logger.error(f"Alpha Vantage API error: {data['Error Message']}")
                return []

            if 'Note' in data:
                self.logger.warning(f"Alpha Vantage API limit: {data['Note']}")
                return []

            articles = []
            feed_data = data.get('feed', [])

            for article_data in feed_data:
                try:
                    article = self._parse_alphavantage_article(article_data)
                    if article:
                        articles.append(article)
                except Exception as e:
                    self.logger.debug(f"Error parsing Alpha Vantage article: {e}")
                    continue

            self.logger.info(f"Collected {len(articles)} articles from Alpha Vantage")
            return articles[:max_articles]

        except Exception as e:
            self.logger.error(f"Error collecting Alpha Vantage news: {e}")
            return []

    def get_symbol_news(self, symbol: str, hours_back: int = 24, max_articles: int = 20) -> List[NewsArticle]:
        """Get news specifically for a symbol from Alpha Vantage - KEY for decision engine"""
        try:
            self.logger.info(f"Fetching Alpha Vantage news for {symbol}")

            # Alpha Vantage supports symbol-specific news
            params = {
                'function': 'NEWS_SENTIMENT',
                'tickers': symbol.upper(),  # Specific symbol
                'apikey': self.api_key,
                'limit': min(50, max_articles * 2),  # Get more to filter
                'sort': 'LATEST'
            }

            # Add time filter
            if hours_back < 168:
                time_from = (datetime.now() - timedelta(hours=hours_back)).strftime('%Y%m%dT%H%M')
                params['time_from'] = time_from

            response = self.session.get(self.base_url, params=params, timeout=30)
            response.raise_for_status()
            self.daily_requests += 1

            data = response.json()

            # Check for API errors
            if 'Error Message' in data:
                self.logger.error(f"Alpha Vantage API error: {data['Error Message']}")
                return []

            if 'Note' in data:
                self.logger.warning(f"Alpha Vantage API limit: {data['Note']}")
                return []

            articles = []
            feed_data = data.get('feed', [])

            for article_data in feed_data:
                try:
                    article = self._parse_alphavantage_article(article_data)
                    if article:
                        # Ensure the target symbol is in symbols_mentioned
                        if symbol.upper() not in article.symbols_mentioned:
                            article.symbols_mentioned.append(symbol.upper())
                        articles.append(article)
                except Exception as e:
                    self.logger.debug(f"Error parsing Alpha Vantage article for {symbol}: {e}")
                    continue

            self.logger.info(f"Found {len(articles)} Alpha Vantage articles for {symbol}")
            return articles[:max_articles]

        except Exception as e:
            self.logger.error(f"Error getting Alpha Vantage news for {symbol}: {e}")
            return []

    def get_watchlist_news(self, symbols: List[str], hours_back: int = 24,
                          max_articles_per_symbol: int = 10) -> Dict[str, List[NewsArticle]]:
        """Get news for multiple symbols (watchlist) from Alpha Vantage - KEY for decision engine"""
        watchlist_news = {}

        try:
            self.logger.info(f"Collecting Alpha Vantage news for {len(symbols)} watchlist symbols")

            # Alpha Vantage supports multiple tickers (comma-separated)
            symbols_str = ','.join(symbol.upper() for symbol in symbols)

            params = {
                'function': 'NEWS_SENTIMENT',
                'tickers': symbols_str,
                'apikey': self.api_key,
                'limit': 50,  # Maximum allowed
                'sort': 'LATEST'
            }

            # Add time filter
            if hours_back < 168:
                time_from = (datetime.now() - timedelta(hours=hours_back)).strftime('%Y%m%dT%H%M')
                params['time_from'] = time_from

            response = self.session.get(self.base_url, params=params, timeout=30)
            response.raise_for_status()
            self.daily_requests += 1

            data = response.json()

            # Check for API errors
            if 'Error Message' in data:
                self.logger.error(f"Alpha Vantage API error: {data['Error Message']}")
                return {symbol: [] for symbol in symbols}

            if 'Note' in data:
                self.logger.warning(f"Alpha Vantage API limit: {data['Note']}")
                return {symbol: [] for symbol in symbols}

            # Initialize results for all symbols
            for symbol in symbols:
                watchlist_news[symbol] = []

            # Parse and distribute articles
            feed_data = data.get('feed', [])

            for article_data in feed_data:
                try:
                    article = self._parse_alphavantage_article(article_data)
                    if article:
                        # Alpha Vantage provides ticker_sentiment array
                        article_tickers = set()

                        # Extract tickers from Alpha Vantage ticker_sentiment
                        ticker_sentiment = article_data.get('ticker_sentiment', [])
                        for ticker_data in ticker_sentiment:
                            ticker = ticker_data.get('ticker', '').upper()
                            if ticker:
                                article_tickers.add(ticker)

                        # Also check symbols mentioned in title/content
                        extracted_symbols = set(article.symbols_mentioned)
                        article_tickers.update(extracted_symbols)

                        # Distribute article to relevant symbols
                        for symbol in symbols:
                            if (symbol.upper() in article_tickers or
                                symbol.lower() in article.title.lower()):

                                # Ensure symbol is in symbols_mentioned
                                if symbol.upper() not in article.symbols_mentioned:
                                    article.symbols_mentioned.append(symbol.upper())

                                # Add to symbol's articles if under limit
                                if len(watchlist_news[symbol]) < max_articles_per_symbol:
                                    watchlist_news[symbol].append(article)

                except Exception as e:
                    self.logger.debug(f"Error parsing Alpha Vantage watchlist article: {e}")
                    continue

            total_articles = sum(len(articles) for articles in watchlist_news.values())
            self.logger.info(f"Alpha Vantage watchlist complete: {total_articles} total articles for {len(symbols)} symbols")

        except Exception as e:
            self.logger.error(f"Error in Alpha Vantage watchlist collection: {e}")
            watchlist_news = {symbol: [] for symbol in symbols}

        return watchlist_news

    def test_connection(self) -> bool:
        """Test Alpha Vantage API connection"""
        try:
            params = {
                'function': 'NEWS_SENTIMENT',
                'apikey': self.api_key,
                'limit': 1  # Minimal request
            }

            response = self.session.get(self.base_url, params=params, timeout=10)

            if response.status_code == 200:
                data = response.json()
                # Check for valid response (not error or limit message)
                return 'feed' in data and 'Error Message' not in data and 'Note' not in data
            return False

        except Exception as e:
            self.logger.error(f"Alpha Vantage connection test failed: {e}")
            return False

    def _parse_alphavantage_article(self, article_data: Dict) -> Optional[NewsArticle]:
        """Parse Alpha Vantage article data into NewsArticle format"""
        try:
            title = article_data.get('title', '').strip()

            if not title or len(title) < 10:
                return None

            # Parse publication date
            published_str = article_data.get('time_published', '')
            try:
                # Alpha Vantage format: "20231207T143000"
                published_date = datetime.strptime(published_str, '%Y%m%dT%H%M%S')
            except:
                published_date = datetime.now()

            # Get content/summary
            summary = article_data.get('summary', '') or ''
            content = summary or title

            # Extract symbols from Alpha Vantage ticker_sentiment (they provide this!)
            symbols = []
            ticker_sentiment = article_data.get('ticker_sentiment', [])

            for ticker_data in ticker_sentiment:
                ticker = ticker_data.get('ticker', '').upper()
                if ticker and len(ticker) <= 5:  # Valid symbol length
                    symbols.append(ticker)

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
                source="AlphaVantage",
                url=article_data.get('url', ''),
                published_date=published_date,
                symbols_mentioned=unique_symbols,
                author=article_data.get('source', '')  # Alpha Vantage provides source
            )

            # Add Alpha Vantage-specific metadata including sentiment scores
            article.additional_metadata = {
                'source_name': article_data.get('source', ''),
                'source_domain': article_data.get('source_domain', ''),
                'overall_sentiment_score': article_data.get('overall_sentiment_score', 0.0),
                'overall_sentiment_label': article_data.get('overall_sentiment_label', 'Neutral'),
                'ticker_sentiment': ticker_sentiment,  # Individual ticker sentiments
                'authors': article_data.get('authors', []),
                'banner_image': article_data.get('banner_image', ''),
                'category_within_source': article_data.get('category_within_source', '')
            }

            # Set article-level sentiment from Alpha Vantage data
            article.sentiment_score = float(article_data.get('overall_sentiment_score', 0.0))
            article.sentiment_label = article_data.get('overall_sentiment_label', 'Neutral')
            article.sentiment_confidence = 0.8  # Alpha Vantage is generally reliable

            return article

        except Exception as e:
            self.logger.debug(f"Error parsing Alpha Vantage article: {e}")
            return None

    def get_api_usage(self) -> Dict[str, int]:
        """Get current API usage stats"""
        return {
            'requests_used': self.daily_requests,
            'requests_remaining': self.max_daily_requests - self.daily_requests,
            'daily_limit': self.max_daily_requests
        }

    def get_market_sentiment(self, symbols: Optional[List[str]] = None, hours_back: int = 24) -> Dict[str, Dict]:
        """Get aggregated market sentiment scores for symbols (Alpha Vantage specialty)"""
        try:
            self.logger.info("Getting market sentiment from Alpha Vantage")

            params = {
                'function': 'NEWS_SENTIMENT',
                'apikey': self.api_key,
                'limit': 50,
                'sort': 'LATEST'
            }

            if symbols:
                params['tickers'] = ','.join(symbol.upper() for symbol in symbols)

            if hours_back < 168:
                time_from = (datetime.now() - timedelta(hours=hours_back)).strftime('%Y%m%dT%H%M')
                params['time_from'] = time_from

            response = self.session.get(self.base_url, params=params, timeout=30)
            response.raise_for_status()
            self.daily_requests += 1

            data = response.json()

            if 'Error Message' in data or 'Note' in data:
                return {}

            # Aggregate sentiment by symbol
            sentiment_data = {}

            for article_data in data.get('feed', []):
                ticker_sentiment = article_data.get('ticker_sentiment', [])

                for ticker_data in ticker_sentiment:
                    ticker = ticker_data.get('ticker', '').upper()
                    if ticker:
                        if ticker not in sentiment_data:
                            sentiment_data[ticker] = {
                                'scores': [],
                                'labels': [],
                                'article_count': 0
                            }

                        score = float(ticker_data.get('ticker_sentiment_score', 0.0))
                        label = ticker_data.get('ticker_sentiment_label', 'Neutral')

                        sentiment_data[ticker]['scores'].append(score)
                        sentiment_data[ticker]['labels'].append(label)
                        sentiment_data[ticker]['article_count'] += 1

            # Calculate averages
            aggregated_sentiment = {}
            for ticker, data in sentiment_data.items():
                if data['scores']:
                    avg_score = sum(data['scores']) / len(data['scores'])
                    most_common_label = max(set(data['labels']), key=data['labels'].count)

                    aggregated_sentiment[ticker] = {
                        'average_sentiment_score': avg_score,
                        'dominant_sentiment_label': most_common_label,
                        'article_count': data['article_count'],
                        'confidence': min(data['article_count'] / 10.0, 1.0)  # More articles = higher confidence
                    }

            return aggregated_sentiment

        except Exception as e:
            self.logger.error(f"Error getting market sentiment: {e}")
            return {}
