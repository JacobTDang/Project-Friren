"""
Yahoo Finance News Data Collector - Fixed Version

Pure news scraping from Yahoo Finance with corrected URLs and enhanced symbol extraction.
"""

import requests
import time
import re
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from typing import List, Optional, Dict
import logging

try:
    from .base import NewsDataSource, NewsArticle
except ImportError:
    from base import NewsDataSource, NewsArticle


class SymbolExtractor:
    """Enhanced symbol extraction for Yahoo Finance"""

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

        # 3. Yahoo Finance specific patterns
        yahoo_patterns = [
            r'([A-Z]{2,5})[+-]?\s*\d+\.\d+%?',  # AAPL+1.23% or AAPL 150.23
            r'([A-Z]{2,5})\s*(?:stock|shares?|equity)',  # AAPL stock
            r'([A-Z]{2,5})\s*(?:Corporation|Corp|Inc\.?)',  # AAPL Inc
        ]

        for pattern in yahoo_patterns:
            matches = re.findall(pattern, text)
            symbols.update(matches)

        # 4. Known symbols as words
        words = re.findall(r'\b[A-Za-z]{1,5}\b', text)
        for word in words:
            if word.upper() in self.known_symbols:
                symbols.add(word.upper())

        # 5. Company names
        for company_name, symbol in self.company_mappings.items():
            if company_name in text_lower:
                symbols.add(symbol)

        # 6. Financial context patterns
        financial_patterns = [
            r'\b(' + '|'.join(self.known_symbols) + r')\s+(?:earnings|revenue|beat|miss)\b',
            r'\b(' + '|'.join(self.known_symbols) + r')\s+(?:up|down|gained?|lost)\s+\d+',
            r'(?:upgraded?|downgraded?)\s+(' + '|'.join(self.known_symbols) + r')\b',
        ]

        for pattern in financial_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            symbols.update(m.upper() for m in matches)

        # Filter out false positives
        false_positives = {'US', 'USA', 'UK', 'EU', 'CEO', 'CFO', 'IPO', 'SEC', 'FDA', 'AI', 'IT'}
        symbols = symbols - false_positives

        return sorted(list(symbols))


class YahooFinanceNews(NewsDataSource):
    """Fixed Yahoo Finance news collector with working URLs"""

    def __init__(self):
        super().__init__("Yahoo Finance")
        self.logger = logging.getLogger(f"{__name__}.YahooFinanceNews")

        # Session with proper headers
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
        })

        self.symbol_extractor = SymbolExtractor()

    def collect_news(self, hours_back: int = 24, max_articles: int = 50) -> List[NewsArticle]:
        """Collect general financial news from Yahoo Finance"""
        sections = [
            'https://finance.yahoo.com/news/',
            'https://finance.yahoo.com/topic/stock-market-news/',
            'https://finance.yahoo.com/topic/earnings/',
            'https://finance.yahoo.com/markets/'
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

    def get_symbol_news(self, symbol: str, max_articles: int = 20) -> List[NewsArticle]:
        """Get news specifically for a symbol - FIXED URLs"""
        try:
            self.logger.info(f"Fetching news for {symbol}")

            # Try multiple Yahoo Finance URLs for symbol news
            urls_to_try = [
                f"https://finance.yahoo.com/quote/{symbol}/",  # Main quote page
                f"https://finance.yahoo.com/quote/{symbol}/news/",  # Direct news page
                f"https://finance.yahoo.com/quote/{symbol}",  # Without trailing slash
                f"https://finance.yahoo.com/lookup?s={symbol}",  # Search results
            ]

            all_articles = []

            for url in urls_to_try:
                try:
                    articles = self._scrape_section(url)
                    all_articles.extend(articles)

                    if articles:  # If we got articles, no need to try other URLs
                        self.logger.info(f"Successfully got articles from {url}")
                        break

                except Exception as e:
                    self.logger.debug(f"URL {url} failed: {e}")
                    continue

            # Also search in general news for symbol mentions
            general_articles = self.collect_news(hours_back=24, max_articles=50)

            # Filter general articles for symbol mentions
            symbol_filtered = []
            for article in general_articles:
                if (symbol.upper() in article.symbols_mentioned or
                    symbol.lower() in article.title.lower() or
                    symbol.upper() in article.title.upper() or
                    any(company in article.title.lower()
                        for company, sym in self.symbol_extractor.company_mappings.items()
                        if sym == symbol.upper())):

                    # Ensure symbol is in the symbols_mentioned list
                    if symbol.upper() not in article.symbols_mentioned:
                        article.symbols_mentioned.append(symbol.upper())

                    symbol_filtered.append(article)

            # Combine all articles
            all_symbol_articles = all_articles + symbol_filtered
            unique_articles = self._remove_duplicates(all_symbol_articles)

            self.logger.info(f"Found {len(unique_articles)} articles for {symbol}")
            return unique_articles[:max_articles]

        except Exception as e:
            self.logger.error(f"Error getting news for {symbol}: {e}")
            return []

    def get_watchlist_news(self, symbols: List[str], max_articles_per_symbol: int = 10) -> Dict[str, List[NewsArticle]]:
        """Get news for multiple symbols (watchlist) - FIXED for decision engine"""
        watchlist_news = {}

        # Get general news once to avoid repeated scraping
        try:
            self.logger.info(f"Collecting general news for watchlist filtering...")
            general_articles = self.collect_news(hours_back=24, max_articles=100)

            for symbol in symbols:
                try:
                    # Filter general articles for this symbol
                    symbol_articles = []

                    for article in general_articles:
                        if (symbol.upper() in article.symbols_mentioned or
                            symbol.lower() in article.title.lower() or
                            symbol.upper() in article.title.upper() or
                            any(company in article.title.lower()
                                for company, sym in self.symbol_extractor.company_mappings.items()
                                if sym == symbol.upper())):

                            # Ensure symbol is in the symbols_mentioned list
                            if symbol.upper() not in article.symbols_mentioned:
                                article.symbols_mentioned.append(symbol.upper())

                            symbol_articles.append(article)

                    # Also try to get symbol-specific articles (with rate limiting)
                    try:
                        specific_articles = self.get_symbol_news(symbol, max_articles=max_articles_per_symbol)
                        symbol_articles.extend(specific_articles)
                        time.sleep(1)  # Rate limiting
                    except Exception as e:
                        self.logger.debug(f"Symbol-specific search failed for {symbol}: {e}")

                    # Remove duplicates and limit
                    unique_articles = self._remove_duplicates(symbol_articles)
                    watchlist_news[symbol] = unique_articles[:max_articles_per_symbol]

                except Exception as e:
                    self.logger.error(f"Error processing {symbol} for watchlist: {e}")
                    watchlist_news[symbol] = []

            total_articles = sum(len(articles) for articles in watchlist_news.values())
            self.logger.info(f"Watchlist processing complete: {total_articles} total articles for {len(symbols)} symbols")

        except Exception as e:
            self.logger.error(f"Error in watchlist collection: {e}")
            watchlist_news = {symbol: [] for symbol in symbols}

        return watchlist_news

    def test_connection(self) -> bool:
        """Test connection to Yahoo Finance"""
        try:
            response = self.session.get("https://finance.yahoo.com/news/", timeout=10)
            return response.status_code == 200
        except Exception:
            return False

    def _scrape_section(self, url: str) -> List[NewsArticle]:
        """Scrape a Yahoo Finance section with improved selectors"""
        articles = []

        try:
            self.logger.debug(f"Scraping {url}")
            response = self.session.get(url, timeout=15)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, 'html.parser')

            # Enhanced selectors for Yahoo Finance
            selectors = [
                # News page selectors
                'h3 a[href*="/news/"]',
                'a[data-testid]',
                'h3 a',
                '.js-content-viewer a',

                # Quote page selectors (for symbol-specific news)
                '.stream-item h3 a',
                '.stream-item a',
                '.news-list a',

                # General content selectors
                'a[href*="/finance/news/"]',
                'a[href*="/news/"]',
                '[data-module] a',

                # Market page selectors
                '.market-news a',
                '.trending-list a'
            ]

            links = []
            for selector in selectors:
                found_links = soup.select(selector)
                if found_links:
                    self.logger.debug(f"Found {len(found_links)} links with selector: {selector}")
                    links.extend(found_links)

            # Process links
            seen_urls = set()
            processed_count = 0

            for link in links:
                if processed_count >= 50:  # Limit per section
                    break

                try:
                    title = link.get_text(strip=True)
                    href = link.get('href', '')

                    if not title or len(title) < 10 or href in seen_urls:
                        continue

                    # Skip navigation and non-news items
                    skip_keywords = ['sign in', 'newsletters', 'premium', 'more...', 'subscribe']
                    if any(keyword in title.lower() for keyword in skip_keywords):
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

                    # Extract symbols with enhanced detection
                    symbols = self.symbol_extractor.extract_symbols(title)

                    # Create article
                    article = NewsArticle(
                        title=title,
                        content=title,  # Use title as content for now
                        source=self.source_name,
                        url=full_url,
                        published_date=datetime.now(),  # Could be enhanced to extract real date
                        symbols_mentioned=symbols
                    )

                    articles.append(article)
                    processed_count += 1

                    self.logger.debug(f" {title[:50]}... | Symbols: {symbols}")

                except Exception as e:
                    self.logger.debug(f"Error processing link: {e}")
                    continue

            self.logger.info(f"Scraped {len(articles)} articles from {url}")

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


# Example usage
if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)

    collector = YahooFinanceNews()

    if collector.test_connection():
        print("Yahoo Finance connection working!")

        # Test general news
        print("\nTesting general news...")
        articles = collector.collect_news(max_articles=3)
        print(f"Found {len(articles)} general articles")

        for article in articles:
            print(f"  - {article.title[:60]}...")
            print(f"    Symbols: {article.symbols_mentioned}")

        # Test symbol-specific news (FIXED)
        print(f"\nTesting AAPL news...")
        aapl_news = collector.get_symbol_news('AAPL', max_articles=5)
        print(f"Found {len(aapl_news)} AAPL articles")

        for article in aapl_news:
            print(f"  - {article.title[:60]}...")
            print(f"    Symbols: {article.symbols_mentioned}")

        # Test watchlist (FIXED)
        print(f"\nTesting watchlist...")
        watchlist = ['AAPL', 'MSFT']
        watchlist_news = collector.get_watchlist_news(watchlist, max_articles_per_symbol=3)

        for symbol, articles in watchlist_news.items():
            print(f"  {symbol}: {len(articles)} articles")
            for article in articles[:1]:
                print(f"    - {article.title[:50]}...")

    else:
        print("Connection failed!")
