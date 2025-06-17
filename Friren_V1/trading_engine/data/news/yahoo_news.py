"""
Yahoo Finance News Data Collector - Simplified

Pure news scraping from Yahoo Finance. Clean and simple.
The enhanced collector will handle FinBERT prep later.
"""

import requests
import time
import re
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from typing import List, Optional
import logging

from .base import NewsDataSource, NewsArticle


class SymbolExtractor:
    """Simple symbol extraction for Yahoo Finance"""

    def __init__(self):
        # Key symbols we care about
        self.known_symbols = {
            # Tech
            'AAPL', 'MSFT', 'GOOGL', 'GOOG', 'AMZN', 'TSLA', 'META', 'NVDA', 'NFLX', 'ADBE',
            'CRM', 'ORCL', 'IBM', 'INTC', 'AMD', 'QCOM', 'TXN', 'AVGO', 'CSCO', 'UBER', 'LYFT',

            # Finance
            'JPM', 'BAC', 'WFC', 'GS', 'MS', 'C', 'BLK', 'SCHW', 'SPGI', 'AXP', 'V', 'MA', 'PYPL', 'COIN',

            # Healthcare
            'JNJ', 'PFE', 'UNH', 'ABBV', 'MRK', 'TMO', 'ABT', 'DHR', 'LLY', 'AMGN', 'GILD', 'VRTX', 'CVS',

            # Consumer
            'PG', 'KO', 'PEP', 'WMT', 'COST', 'HD', 'LOW', 'TGT', 'NKE', 'SBUX', 'MCD', 'DIS', 'VZ', 'T',

            # Energy & Industrials
            'XOM', 'CVX', 'COP', 'EOG', 'SLB', 'OXY', 'HON', 'BA', 'CAT', 'DE', 'GE', 'MMM', 'RTX', 'UPS', 'FDX',

            # ETFs
            'SPY', 'QQQ', 'IWM', 'DIA', 'VTI', 'VOO', 'GLD', 'SLV',

            # Popular stocks
            'GME', 'AMC', 'BB', 'NOK', 'PLTR', 'RBLX', 'HOOD', 'SOFI', 'RIVN', 'LCID', 'SNAP'
        }

        # Company name mappings
        self.company_mappings = {
            'apple': 'AAPL', 'microsoft': 'MSFT', 'google': 'GOOGL', 'alphabet': 'GOOGL',
            'amazon': 'AMZN', 'tesla': 'TSLA', 'facebook': 'META', 'meta': 'META',
            'nvidia': 'NVDA', 'netflix': 'NFLX', 'adobe': 'ADBE', 'salesforce': 'CRM',
            'oracle': 'ORCL', 'intel': 'INTC', 'qualcomm': 'QCOM', 'jpmorgan': 'JPM',
            'johnson & johnson': 'JNJ', 'pfizer': 'PFE', 'walmart': 'WMT', 'costco': 'COST',
            'boeing': 'BA', 'disney': 'DIS', 'nike': 'NKE', 'visa': 'V', 'mastercard': 'MA'
        }

    def extract_symbols(self, text: str) -> List[str]:
        """Extract stock symbols from text"""
        if not text:
            return []

        symbols = set()
        text_lower = text.lower()

        # 1. $SYMBOL format
        dollar_symbols = re.findall(r'\$([A-Z]{1,5})\b', text)
        symbols.update(dollar_symbols)

        # 2. (SYMBOL) format
        paren_symbols = re.findall(r'\(([A-Z]{1,5})\)', text)
        symbols.update(paren_symbols)

        # 3. Known symbols as words
        words = re.findall(r'\b[A-Za-z]{1,5}\b', text)
        for word in words:
            if word.upper() in self.known_symbols:
                symbols.add(word.upper())

        # 4. Company names
        for company_name, symbol in self.company_mappings.items():
            if company_name in text_lower:
                symbols.add(symbol)

        # Filter out false positives
        false_positives = {'US', 'USA', 'UK', 'EU', 'CEO', 'CFO', 'IPO', 'SEC', 'FDA', 'AI', 'IT'}
        symbols = symbols - false_positives

        return sorted(list(symbols))


class YahooFinanceNews(NewsDataSource):
    """Simple Yahoo Finance news collector - optimized for individual stock news"""

    def __init__(self):
        super().__init__("Yahoo Finance")
        self.logger = logging.getLogger(f"{__name__}.YahooFinanceNews")

        # Simple session
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })

        self.symbol_extractor = SymbolExtractor()

    def collect_news(self, hours_back: int = 24, max_articles: int = 50) -> List[NewsArticle]:
        """Collect general news from Yahoo Finance"""
        sections = [
            'https://finance.yahoo.com/news/',
            'https://finance.yahoo.com/topic/stock-market-news/',
            'https://finance.yahoo.com/topic/earnings/'
        ]

        all_articles = []
        cutoff_time = datetime.now() - timedelta(hours=hours_back)

        for section_url in sections:
            try:
                articles = self._scrape_section(section_url)

                # Filter by time
                recent_articles = [
                    article for article in articles
                    if article.published_date >= cutoff_time
                ]

                all_articles.extend(recent_articles)
                time.sleep(2)  # Rate limiting

                if len(all_articles) >= max_articles:
                    break

            except Exception as e:
                self.logger.error(f"Error scraping {section_url}: {e}")
                continue

        # Remove duplicates
        unique_articles = self._remove_duplicates(all_articles)
        return unique_articles[:max_articles]

    def get_symbol_news(self, symbol: str, hours_back: int = 24, max_articles: int = 20) -> List[NewsArticle]:
        """
        Get news specifically for a symbol - CRITICAL for decision engine

        This is the key method for watchlist-based trading decisions
        """
        try:
            self.logger.info(f"Fetching news for {symbol}")

            # Yahoo Finance symbol-specific news page
            symbol_url = f"https://finance.yahoo.com/quote/{symbol}/news"
            articles = self._scrape_section(symbol_url)

            # Filter by time
            cutoff_time = datetime.now() - timedelta(hours=hours_back)
            recent_articles = [
                article for article in articles
                if article.published_date >= cutoff_time
            ]

            # Filter to articles that actually mention the symbol
            symbol_articles = []
            for article in recent_articles:
                if (symbol.upper() in article.symbols_mentioned or
                    symbol.lower() in article.title.lower() or
                    symbol.upper() in article.title):
                    symbol_articles.append(article)

            self.logger.info(f"Found {len(symbol_articles)} recent articles for {symbol}")
            return symbol_articles[:max_articles]

        except Exception as e:
            self.logger.error(f"Error getting news for {symbol}: {e}")
            return []

    def get_watchlist_news(self, symbols: List[str], hours_back: int = 24, max_articles_per_symbol: int = 10) -> dict[str, List[NewsArticle]]:
        """
        Get news for multiple symbols (watchlist) - CRITICAL for decision engine

        Returns: {symbol: [articles]} mapping for easy decision engine processing
        """
        watchlist_news = {}

        for symbol in symbols:
            try:
                self.logger.info(f"Getting watchlist news for {symbol}")
                symbol_articles = self.get_symbol_news(symbol, hours_back, max_articles_per_symbol)
                watchlist_news[symbol] = symbol_articles

                # Rate limiting between symbols
                time.sleep(1)

            except Exception as e:
                self.logger.error(f"Error getting watchlist news for {symbol}: {e}")
                watchlist_news[symbol] = []

        total_articles = sum(len(articles) for articles in watchlist_news.values())
        self.logger.info(f"Collected {total_articles} total articles for {len(symbols)} watchlist symbols")

        return watchlist_news

    def test_connection(self) -> bool:
        """Test connection to Yahoo Finance"""
        try:
            response = self.session.get("https://finance.yahoo.com/news/", timeout=10)
            return response.status_code == 200
        except Exception:
            return False

    def _scrape_section(self, url: str) -> List[NewsArticle]:
        """Scrape a Yahoo Finance news section"""
        articles = []

        try:
            response = self.session.get(url, timeout=15)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')

            # Find article links
            selectors = [
                'h3 a[href*="/news/"]',
                'a[data-testid]',
                '.js-content-viewer a'
            ]

            links = []
            for selector in selectors:
                links.extend(soup.select(selector))

            # Process links
            seen_urls = set()
            for link in links[:30]:  # Limit per section
                try:
                    title = link.get_text(strip=True)
                    href = link.get('href', '')

                    if not title or len(title) < 10 or href in seen_urls:
                        continue

                    seen_urls.add(href)

                    # Build full URL
                    if href.startswith('/'):
                        full_url = f"https://finance.yahoo.com{href}"
                    elif href.startswith('http'):
                        full_url = href
                    else:
                        continue

                    # Clean title
                    title = ' '.join(title.split()).replace('...', '').strip()

                    # Extract symbols
                    symbols = self.symbol_extractor.extract_symbols(title)

                    # Create article
                    article = NewsArticle(
                        title=title,
                        content=title,  # Use title as content for now
                        source=self.source_name,
                        url=full_url,
                        published_date=datetime.now(),
                        symbols_mentioned=symbols
                    )

                    articles.append(article)

                except Exception as e:
                    self.logger.debug(f"Error processing link: {e}")
                    continue

        except Exception as e:
            self.logger.error(f"Error scraping {url}: {e}")

        return articles

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

    def get_symbol_news(self, symbol: str, max_articles: int = 20) -> List[NewsArticle]:
        """Get news for a specific symbol"""
        try:
            url = f"https://finance.yahoo.com/quote/{symbol}/news"
            articles = self._scrape_section(url)

            # Filter to articles mentioning the symbol
            symbol_articles = [
                article for article in articles
                if symbol.upper() in article.symbols_mentioned
            ]

            return symbol_articles[:max_articles]

        except Exception as e:
            self.logger.error(f"Error getting news for {symbol}: {e}")
            return []


# Example usage
if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)

    collector = YahooFinanceNews()

    if collector.test_connection():
        print("Testing Yahoo Finance news collection...")

        # Test general news
        articles = collector.collect_news(max_articles=5, hours_back=48)
        print(f"General news: {len(articles)} articles")

        # Test individual stock news (key for decision engine)
        print(f"\nTesting individual stock news...")
        aapl_news = collector.get_symbol_news('AAPL', hours_back=48)
        print(f"AAPL news: {len(aapl_news)} articles")

        for article in aapl_news[:2]:
            print(f"  - {article.title}")
            print(f"    Symbols: {article.symbols_mentioned}")

        # Test watchlist news (key for decision engine)
        print(f"\nTesting watchlist news...")
        watchlist = ['AAPL', 'MSFT', 'GOOGL']
        watchlist_news = collector.get_watchlist_news(watchlist, hours_back=48, max_articles_per_symbol=3)

        for symbol, articles in watchlist_news.items():
            print(f"  {symbol}: {len(articles)} articles")
            for article in articles[:1]:  # Show first article
                print(f"    - {article.title[:60]}...")

    else:
        print("Connection failed!")
