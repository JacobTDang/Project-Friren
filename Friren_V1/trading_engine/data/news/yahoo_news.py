"""
Yahoo Finance News Data Collector

Pure news scraping from Yahoo Finance financial news sections.
Separated from price data and sentiment analysis for clean architecture.

Features:
- Scrapes financial news from Yahoo Finance news sections
- Standardized NewsArticle output format
- Smart symbol extraction from headlines
- Rate limiting and error handling
- No sentiment analysis (pure data collection)
"""

import requests
import time
import re
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from datetime import datetime, timedelta
from typing import List, Optional, Set
from dataclasses import dataclass, field
import logging


@dataclass
class NewsArticle:
    """Standardized news article structure for all news sources"""
    title: str
    source: str
    url: str
    published_date: datetime
    author: Optional[str] = None
    symbols_mentioned: List[str] = field(default_factory=list)
    content: Optional[str] = None
    # Note: No sentiment fields - this is pure data collection


class SymbolExtractor:
    """Enhanced symbol extraction specifically for Yahoo Finance news"""

    def __init__(self):
        # Comprehensive known symbols list
        self.known_symbols = {
            # Tech giants
            'AAPL', 'MSFT', 'GOOGL', 'GOOG', 'AMZN', 'TSLA', 'META', 'NVDA', 'NFLX', 'ADBE',
            'CRM', 'ORCL', 'IBM', 'INTC', 'AMD', 'QCOM', 'TXN', 'AVGO', 'CSCO', 'UBER', 'LYFT',

            # Finance
            'JPM', 'BAC', 'WFC', 'GS', 'MS', 'C', 'BLK', 'SCHW', 'SPGI', 'AXP', 'V', 'MA', 'PYPL', 'COIN',

            # Healthcare
            'JNJ', 'PFE', 'UNH', 'ABBV', 'MRK', 'TMO', 'ABT', 'DHR', 'LLY', 'AMGN', 'GILD', 'VRTX', 'CVS',

            # Consumer
            'PG', 'KO', 'PEP', 'WMT', 'COST', 'HD', 'LOW', 'TGT', 'NKE', 'SBUX', 'MCD', 'DIS', 'VZ', 'T',

            # Energy
            'XOM', 'CVX', 'COP', 'EOG', 'SLB', 'OXY', 'HON',

            # Industrials
            'BA', 'CAT', 'DE', 'GE', 'MMM', 'RTX', 'UPS', 'FDX', 'LMT',

            # ETFs and Indices
            'SPY', 'QQQ', 'IWM', 'DIA', 'VTI', 'VOO', 'VEA', 'VWO', 'AGG', 'BND', 'GLD', 'SLV',

            # Popular/Meme stocks
            'GME', 'AMC', 'BB', 'NOK', 'PLTR', 'RBLX', 'HOOD', 'SOFI', 'RIVN', 'LCID', 'SNAP',

            # Additional high-volume tickers
            'OKLO', 'QUBT', 'SMR', 'RGTI', 'FLR', 'NEGG', 'LYELL', 'SMMT', 'CHWY', 'GTLB', 'VOYG',
            'COMP', 'CLF', 'RYCEY', 'ANVS', 'HBCP', 'SIRI', 'DVA', 'SBLK', 'ESEA', 'BCS'
        }

        # Company name to symbol mappings for Yahoo Finance articles
        self.company_mappings = {
            'apple': 'AAPL', 'microsoft': 'MSFT', 'google': 'GOOGL', 'alphabet': 'GOOGL',
            'amazon': 'AMZN', 'tesla': 'TSLA', 'facebook': 'META', 'meta platforms': 'META',
            'nvidia': 'NVDA', 'netflix': 'NFLX', 'adobe': 'ADBE', 'salesforce': 'CRM',
            'oracle': 'ORCL', 'intel': 'INTC', 'qualcomm': 'QCOM', 'jpmorgan': 'JPM',
            'johnson & johnson': 'JNJ', 'pfizer': 'PFE', 'walmart': 'WMT', 'costco': 'COST',
            'boeing': 'BA', 'caterpillar': 'CAT', 'disney': 'DIS', 'nike': 'NKE',
            'starbucks': 'SBUX', 'mcdonalds': 'MCD', 'visa': 'V', 'mastercard': 'MA',
            'berkshire hathaway': 'BRK-B', 'unitedhealth': 'UNH', 'exxon': 'XOM', 'chevron': 'CVX'
        }

    def extract_symbols(self, text: str) -> List[str]:
        """
        Extract stock symbols from Yahoo Finance news text

        Args:
            text: Article title or content

        Returns:
            List[str]: Extracted stock symbols
        """
        if not text:
            return []

        symbols = set()
        text_lower = text.lower()

        # 1. Look for $SYMBOL format (common in financial news)
        dollar_symbols = re.findall(r'\$([A-Z]{1,5})\b', text)
        symbols.update(dollar_symbols)

        # 2. Look for (SYMBOL) format
        paren_symbols = re.findall(r'\(([A-Z]{1,5})\)', text)
        symbols.update(paren_symbols)

        # 3. Yahoo Finance specific patterns
        yahoo_patterns = [
            r'([A-Z]{2,5})[+-]\d+\.\d+%?',  # AAPL+1.23% or AAPL-1.23%
            r'([A-Z]{2,5})\s*\$?\d+\.\d+[+-]',  # AAPL $150.23+1.45
            r'([A-Z]{2,5})\s*(?:Corporation|Corp|Inc\.?|Ltd\.?)',  # AAPL Corporation
            r'([A-Z]{2,5})\s*(?:stock|shares?|equity)',  # AAPL stock
        ]

        for pattern in yahoo_patterns:
            matches = re.findall(pattern, text)
            symbols.update(matches)

        # 4. Look for known symbols as standalone words
        words = re.findall(r'\b[A-Za-z]{1,5}\b', text)
        for word in words:
            if word.upper() in self.known_symbols:
                symbols.add(word.upper())

        # 5. Look for company names and map to symbols
        for company_name, symbol in self.company_mappings.items():
            if company_name in text_lower:
                symbols.add(symbol)

        # 6. Financial context patterns specific to Yahoo Finance
        financial_patterns = [
            r'\b(' + '|'.join(self.known_symbols) + r')\s+(?:earnings|revenue|profit|loss|beat|miss)\b',
            r'\b(' + '|'.join(self.known_symbols) + r')\s+(?:up|down|gained?|lost|surged?|fell)\s+\d+',
            r'\b(' + '|'.join(self.known_symbols) + r')\s+(?:shares?|stock)\s+(?:up|down|rise|fall)',
            r'(?:upgraded?|downgraded?|initiated?)\s+(' + '|'.join(self.known_symbols) + r')\b',
        ]

        for pattern in financial_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            symbols.update([m.upper() for m in matches])

        # Filter out common false positives
        false_positives = {'US', 'USA', 'UK', 'EU', 'CEO', 'CFO', 'IPO', 'ETF', 'SEC', 'FDA', 'AI', 'IT'}
        symbols = symbols - false_positives

        return sorted(list(symbols))


class YahooFinanceNewsData:
    """
    Pure Yahoo Finance news data collector

    Handles scraping financial news from Yahoo Finance news sections.
    Focus: Clean data extraction with no sentiment processing.
    """

    def __init__(self, rate_limit_delay: float = 2.0, max_retries: int = 3):
        self.rate_limit_delay = rate_limit_delay
        self.max_retries = max_retries
        self.logger = logging.getLogger(f"{__name__}.YahooFinanceNewsData")

        # Session with proper headers to avoid blocking
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none'
        })

        self.symbol_extractor = SymbolExtractor()

    def get_financial_news(self, sections: Optional[List[str]] = None,
                          max_articles: int = 100, hours_back: int = 24) -> List[NewsArticle]:
        """
        Scrape financial news from Yahoo Finance news sections

        Args:
            sections: List of URLs to scrape (default: main financial news sections)
            max_articles: Maximum number of articles to collect
            hours_back: Only collect articles from last N hours (for filtering)

        Returns:
            List[NewsArticle]: Collected news articles
        """
        if sections is None:
            sections = [
                'https://finance.yahoo.com/news/',
                'https://finance.yahoo.com/topic/stock-market-news/',
                'https://finance.yahoo.com/topic/earnings/',
                'https://finance.yahoo.com/topic/financial-news/',
                'https://finance.yahoo.com/sector/ms_basic_materials',
                'https://finance.yahoo.com/sector/ms_financial_services',
                'https://finance.yahoo.com/sector/ms_technology'
            ]

        all_articles = []
        cutoff_time = datetime.now() - timedelta(hours=hours_back)

        for section_url in sections:
            try:
                self.logger.info(f"Scraping Yahoo Finance section: {section_url}")
                articles = self._scrape_section(section_url)

                # Filter by time if needed
                filtered_articles = []
                for article in articles:
                    if article.published_date >= cutoff_time:
                        filtered_articles.append(article)

                all_articles.extend(filtered_articles)
                self.logger.info(f"Collected {len(filtered_articles)} recent articles from section")

                # Rate limiting between sections
                time.sleep(self.rate_limit_delay)

                if len(all_articles) >= max_articles:
                    break

            except Exception as e:
                self.logger.error(f"Error scraping Yahoo Finance section {section_url}: {e}")
                continue

        # Remove duplicates and limit results
        unique_articles = self._remove_duplicates(all_articles)
        final_articles = unique_articles[:max_articles]

        self.logger.info(f"Collected {len(final_articles)} unique articles from Yahoo Finance")
        return final_articles

    def _scrape_section(self, url: str) -> List[NewsArticle]:
        """
        Scrape articles from a specific Yahoo Finance news section

        Args:
            url: Section URL to scrape

        Returns:
            List[NewsArticle]: Articles found in the section
        """
        articles = []

        for attempt in range(self.max_retries):
            try:
                self.logger.debug(f"Fetching {url} (attempt {attempt + 1})")
                response = self.session.get(url, timeout=15)
                response.raise_for_status()

                soup = BeautifulSoup(response.content, 'html.parser')

                # Multiple selectors to catch different article formats
                selectors = [
                    'h3 a[href*="/news/"]',  # Main news headlines
                    'a[data-testid]',        # Data-testid links
                    '.js-content-viewer a',  # Content viewer links
                    '[data-module] a',       # Data module links
                    '.simpTicker a',         # Simple ticker links
                    'a[href*="/finance/news/"]',  # Finance news links
                ]

                all_links = []
                for selector in selectors:
                    links = soup.select(selector)
                    if links:
                        self.logger.debug(f"Found {len(links)} links with selector: {selector}")
                        all_links.extend(links)

                # Process links
                processed_links = self._filter_and_clean_links(all_links)
                self.logger.debug(f"Processing {len(processed_links)} filtered links from {url}")

                for link in processed_links[:50]:  # Limit per section
                    try:
                        article = self._parse_link_to_article(link)
                        if article:
                            articles.append(article)
                            self.logger.debug(f"✓ Extracted: {article.title[:60]}... | Symbols: {article.symbols_mentioned}")
                    except Exception as e:
                        self.logger.debug(f"Error processing link: {e}")
                        continue

                self.logger.info(f"Successfully scraped {len(articles)} articles from {url}")
                return articles

            except requests.exceptions.RequestException as e:
                self.logger.warning(f"Request failed for {url} (attempt {attempt + 1}): {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    self.logger.error(f"Failed to scrape {url} after {self.max_retries} attempts")
            except Exception as e:
                self.logger.error(f"Unexpected error scraping {url}: {e}")
                break

        return []

    def _filter_and_clean_links(self, links: List) -> List:
        """
        Filter and clean scraped links to get relevant news articles

        Args:
            links: Raw BeautifulSoup link elements

        Returns:
            List: Filtered and cleaned links
        """
        processed_links = []
        seen_hrefs = set()

        # Keywords to skip (navigation, non-news items)
        skip_keywords = [
            'sign in', 'more...', 'newsletters', 'subscribe', 'premium', 'markets', 'watchlist',
            'portfolio', 'screeners', 'personal finance', 'my portfolios', 'currency converter'
        ]

        for link in links:
            href = link.get('href', '')
            title_text = link.get_text(strip=True)

            if not href or not title_text:
                continue

            # Clean and validate
            if (href not in seen_hrefs and
                len(title_text) >= 10 and  # Minimum meaningful length
                title_text.lower() not in skip_keywords and
                not any(skip in title_text.lower() for skip in skip_keywords)):

                seen_hrefs.add(href)
                processed_links.append(link)

        return processed_links

    def _parse_link_to_article(self, link) -> Optional[NewsArticle]:
        """
        Parse a BeautifulSoup link element into a NewsArticle

        Args:
            link: BeautifulSoup link element

        Returns:
            Optional[NewsArticle]: Parsed article or None if invalid
        """
        title = link.get_text(strip=True)
        href = link.get('href', '')

        if not title or len(title) < 10:
            return None

        # Simple title cleaning
        title = ' '.join(title.split())
        title = title.replace('...', '').strip()

        # Build full URL
        if href.startswith('/'):
            full_url = f"https://finance.yahoo.com{href}"
        elif href.startswith('http'):
            full_url = href
        else:
            return None

        # Extract symbols from title
        symbols = self.symbol_extractor.extract_symbols(title)

        # Create article with estimated publish time (Yahoo doesn't always provide timestamps)
        article = NewsArticle(
            title=title,
            source="Yahoo Finance",
            url=full_url,
            published_date=datetime.now(),  # Will be updated if we can extract real date
            symbols_mentioned=symbols
        )

        # Try to get more article details if possible
        try:
            article = self._enhance_article_details(article)
        except Exception as e:
            self.logger.debug(f"Could not enhance article details: {e}")

        return article

    def _enhance_article_details(self, article: NewsArticle) -> NewsArticle:
        """
        Try to enhance article with additional details (author, content, real publish date)

        Args:
            article: Basic NewsArticle to enhance

        Returns:
            NewsArticle: Enhanced article (or original if enhancement fails)
        """
        try:
            # For now, just return the basic article
            # In the future, this could fetch the full article page to get:
            # - Real publication date
            # - Author information
            # - Article content/summary
            # - More accurate symbol extraction from full content

            # This would require additional HTTP requests, so implement carefully
            # to avoid overwhelming Yahoo Finance with requests

            return article

        except Exception:
            return article

    def _remove_duplicates(self, articles: List[NewsArticle]) -> List[NewsArticle]:
        """
        Remove duplicate articles based on title similarity

        Args:
            articles: List of articles to deduplicate

        Returns:
            List[NewsArticle]: Deduplicated articles
        """
        unique_articles = []
        seen_titles = set()

        for article in articles:
            # Create a normalized title key for comparison
            title_key = article.title[:30].lower().strip().replace(' ', '')

            if title_key not in seen_titles:
                seen_titles.add(title_key)
                unique_articles.append(article)

        return unique_articles

    def get_symbol_specific_news(self, symbol: str, max_articles: int = 20) -> List[NewsArticle]:
        """
        Get news specifically mentioning a particular symbol

        Args:
            symbol: Stock symbol to search for
            max_articles: Maximum articles to return

        Returns:
            List[NewsArticle]: Articles mentioning the symbol
        """
        # Use Yahoo Finance's search functionality for symbol-specific news
        search_url = f"https://finance.yahoo.com/quote/{symbol}/news"

        try:
            self.logger.info(f"Fetching news for {symbol}")
            articles = self._scrape_section(search_url)

            # Filter to only articles that actually mention the symbol
            symbol_articles = [
                article for article in articles
                if symbol.upper() in article.symbols_mentioned
            ]

            return symbol_articles[:max_articles]

        except Exception as e:
            self.logger.error(f"Error fetching news for {symbol}: {e}")
            return []

    def test_connection(self) -> bool:
        """Test connection to Yahoo Finance news"""
        try:
            self.logger.info("Testing Yahoo Finance news connection...")
            test_url = "https://finance.yahoo.com/news/"
            response = self.session.get(test_url, timeout=10)

            if response.status_code == 200:
                self.logger.info("Yahoo Finance news connection successful!")
                return True
            else:
                self.logger.error(f"Connection test failed with status: {response.status_code}")
                return False

        except Exception as e:
            self.logger.error(f"Connection test failed: {e}")
            return False


# Example usage and testing
if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)

    print("=" * 60)
    print("YAHOO FINANCE NEWS DATA COLLECTOR TEST")
    print("=" * 60)

    try:
        collector = YahooFinanceNewsData()

        if collector.test_connection():
            print("\nTesting general news collection...")
            articles = collector.get_financial_news(max_articles=20, hours_back=48)

            if articles:
                print(f"Collected {len(articles)} articles")

                # Show examples
                print("\nSample articles:")
                for i, article in enumerate(articles[:5]):
                    print(f"  {i+1}. {article.title[:70]}...")
                    print(f"     Source: {article.source}")
                    print(f"     Symbols: {article.symbols_mentioned}")
                    print(f"     URL: {article.url}")
                    print()

                # Symbol extraction stats
                all_symbols = []
                articles_with_symbols = 0
                for article in articles:
                    if article.symbols_mentioned:
                        articles_with_symbols += 1
                        all_symbols.extend(article.symbols_mentioned)

                print(f"Symbol extraction stats:")
                print(f"  Articles with symbols: {articles_with_symbols}/{len(articles)} ({articles_with_symbols/len(articles)*100:.1f}%)")
                if all_symbols:
                    from collections import Counter
                    symbol_counts = Counter(all_symbols)
                    print(f"  Top mentioned symbols: {dict(symbol_counts.most_common(10))}")

                print("\nTesting symbol-specific news...")
                aapl_news = collector.get_symbol_specific_news('AAPL', max_articles=5)
                print(f"Found {len(aapl_news)} AAPL-specific articles")

            else:
                print("No articles collected")

        else:
            print("Cannot proceed - Yahoo Finance news connection failed")

    except Exception as e:
        print(f"Test failed with error: {e}")

    print("\n" + "=" * 60)
