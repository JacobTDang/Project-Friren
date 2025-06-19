import os
import requests
import praw
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any
import time
from dataclasses import dataclass, field
import re
from dotenv import load_dotenv
import yfinance as yf
from bs4 import BeautifulSoup
from urllib.parse import urljoin

# Load environment variables
load_dotenv()

@dataclass
class NewsArticle:
    """Standardized news article structure"""
    title: str
    source: str
    url: str
    published_date: datetime
    author: Optional[str] = None
    symbols_mentioned: List[str] = field(default_factory=list)
    sentiment_score: Optional[float] = None
    sentiment_label: Optional[str] = None
    sentiment_confidence: Optional[float] = None
    engagement_metrics: Dict[str, Any] = field(default_factory=dict)
    content: Optional[str] = None

class SymbolExtractor:
    """Enhanced symbol extraction with better coverage"""

    def __init__(self):
        # Expanded known symbols list
        self.known_symbols = {
            # Tech giants
            'AAPL', 'MSFT', 'GOOGL', 'GOOG', 'AMZN', 'TSLA', 'META', 'NVDA', 'NFLX', 'ADBE',
            'CRM', 'ORCL', 'IBM', 'INTC', 'AMD', 'QCOM', 'TXN', 'AVGO', 'CSCO',

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
            'GME', 'AMC', 'BB', 'NOK', 'PLTR', 'RBLX', 'HOOD', 'SOFI', 'RIVN', 'LCID', 'UBER', 'LYFT', 'SNAP',

            # Additional tickers from your output
            'OKLO', 'QUBT', 'SMR', 'RGTI', 'FLR', 'NEGG', 'LYELL', 'SMMT', 'CHWY', 'GTLB', 'VOYG',
            'COMP', 'CLF', 'RYCEY', 'ANVS', 'HBCP', 'SIRI', 'DVA', 'SBLK', 'ESEA', 'BCS', 'SFM',
            'CVBF', 'TFC', 'EWBC', 'NWBI', 'WTFCN', 'VSH', 'NVO', 'VRE', 'BHF', 'RZB', 'NMIH',
            'CRS', 'BFAM', 'DAVEW', 'ARTNB', 'FTK', 'GBNXF', 'ESE', 'KIGRY'
        }

        # Company name to symbol mappings
        self.company_mappings = {
            'apple': 'AAPL', 'microsoft': 'MSFT', 'google': 'GOOGL', 'alphabet': 'GOOGL',
            'amazon': 'AMZN', 'tesla': 'TSLA', 'facebook': 'META', 'meta': 'META',
            'nvidia': 'NVDA', 'netflix': 'NFLX', 'adobe': 'ADBE', 'salesforce': 'CRM',
            'oracle': 'ORCL', 'intel': 'INTC', 'qualcomm': 'QCOM', 'jpmorgan': 'JPM',
            'johnson': 'JNJ', 'pfizer': 'PFE', 'walmart': 'WMT', 'costco': 'COST',
            'boeing': 'BA', 'caterpillar': 'CAT', 'disney': 'DIS', 'nike': 'NKE',
            'starbucks': 'SBUX', 'mcdonald': 'MCD', 'visa': 'V', 'mastercard': 'MA',
            'honeywell': 'HON', 'chevron': 'CVX'
        }

    def extract_symbols(self, text: str) -> List[str]:
        """Extract stock symbols from text with aggressive matching"""
        if not text:
            return []

        symbols = set()
        text_lower = text.lower()

        # 1. Look for $SYMBOL format
        dollar_symbols = re.findall(r'\$([A-Z]{1,5})\b', text)
        symbols.update(dollar_symbols)

        # 2. Look for (SYMBOL) format
        paren_symbols = re.findall(r'\(([A-Z]{1,5})\)', text)
        symbols.update(paren_symbols)

        # 3. Yahoo Finance specific patterns
        yahoo_patterns = [
            r'([A-Z]{2,5})[+-]\d+\.\d+%?',  # AAPL+1.23% or AAPL-1.23%
            r'([A-Z]{2,5})\s*\d+\.\d+[+-]',  # AAPL 150.23+1.45
            r'([A-Z]{2,5})\s*Corporation',    # AAPL Corporation
            r'([A-Z]{2,5})\s*Inc\.',         # AAPL Inc.
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

        # 6. Context-aware patterns (for Reddit/informal text)
        trading_patterns = [
            r'\b(' + '|'.join(self.known_symbols) + r')\s+(?:calls?|puts?|options?|stock|shares?)\b',
            r'(?:bought?|sold?|buying|selling)\s+(' + '|'.join(self.known_symbols) + r')\b',
            r'\b(' + '|'.join(self.known_symbols) + r')\s+(?:up|down|gained?|lost)\s+\d+',
            r'\b(' + '|'.join(self.known_symbols) + r')\s+(?:to|at)\s+\$\d+',
        ]

        for pattern in trading_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            symbols.update([m.upper() for m in matches])

        return list(symbols)

class NewsAPIImporter:
    """NewsAPI integration for financial news"""

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv('NEWS_API_KEY')
        if not self.api_key:
            raise ValueError("NewsAPI key not found. Set NEWS_API_KEY environment variable.")

        self.base_url = "https://newsapi.org/v2"
        self.session = requests.Session()
        self.daily_requests = 0
        self.max_daily_requests = 500
        self.symbol_extractor = SymbolExtractor()

    def get_financial_news(self,
                          query: str = "business",
                          language: str = "en",
                          sort_by: str = "publishedAt",
                          page_size: int = 50,
                          hours_back: int = 168) -> List[NewsArticle]:
        """Fetch recent financial news article titles"""
        if self.daily_requests >= self.max_daily_requests:
            print("Daily NewsAPI limit reached")
            return []

        strategies = [
            {"q": "business", "sources": None},
            {"q": "finance", "sources": None},
            {"q": "stock market", "sources": None},
            {"q": "*", "sources": "cnbc,reuters,bloomberg,financial-times,business-insider"},
        ]

        for i, strategy in enumerate(strategies):
            try:
                url = f"{self.base_url}/everything"
                params = {
                    'language': language,
                    'sortBy': sort_by,
                    'pageSize': page_size,
                    'apiKey': self.api_key
                }

                if strategy["sources"]:
                    params['sources'] = strategy["sources"]
                else:
                    params['q'] = strategy["q"]

                print(f"NewsAPI strategy {i+1}: {strategy}")
                response = self.session.get(url, params=params, timeout=30)
                response.raise_for_status()
                self.daily_requests += 1

                data = response.json()

                if data['status'] != 'ok':
                    print(f"NewsAPI error: {data.get('message', 'Unknown error')}")
                    continue

                total_results = data.get('totalResults', 0)
                print(f"Strategy {i+1} found {total_results} total results")

                if total_results > 0:
                    articles = []
                    for article_data in data.get('articles', []):
                        try:
                            article = self._parse_newsapi_article(article_data)
                            if article:
                                articles.append(article)
                        except Exception as e:
                            print(f"Error parsing article: {e}")
                            continue

                    if len(articles) > 0:
                        print(f"Fetched {len(articles)} articles from NewsAPI strategy {i+1}")
                        return articles

            except requests.exceptions.RequestException as e:
                print(f"NewsAPI strategy {i+1} failed: {e}")
                continue

        print("All NewsAPI strategies failed, trying headlines...")
        return self._get_top_business_headlines(page_size)

    def _get_top_business_headlines(self, page_size: int = 50) -> List[NewsArticle]:
        """Fallback method to get top business headlines"""
        if self.daily_requests >= self.max_daily_requests:
            return []

        try:
            url = f"{self.base_url}/top-headlines"
            params = {
                'category': 'business',
                'language': 'en',
                'country': 'us',
                'pageSize': page_size,
                'apiKey': self.api_key
            }

            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            self.daily_requests += 1

            data = response.json()

            if data['status'] != 'ok':
                print(f"NewsAPI headlines error: {data.get('message', 'Unknown error')}")
                return []

            articles = []
            for article_data in data.get('articles', []):
                try:
                    article = self._parse_newsapi_article(article_data)
                    if article:
                        articles.append(article)
                except Exception as e:
                    continue

            print(f"Fetched {len(articles)} headlines from NewsAPI fallback")
            return articles

        except Exception as e:
            print(f"NewsAPI headlines fallback failed: {e}")
            return []

    def _parse_newsapi_article(self, article_data: Dict) -> Optional[NewsArticle]:
        """Parse NewsAPI article data into NewsArticle object"""
        title = article_data.get('title', '').strip()

        if not title or title == '[Removed]' or len(title) < 5:
            return None

        # Parse publication date
        published_str = article_data.get('publishedAt', '')
        try:
            published_date = datetime.fromisoformat(published_str.replace('Z', '+00:00'))
            if published_date.tzinfo is not None:
                published_date = published_date.replace(tzinfo=None)
        except:
            published_date = datetime.now()

        source = article_data.get('source', {}).get('name', 'Unknown')

        # Extract symbols from title and description
        description = article_data.get('description', '') or ''
        full_text = f"{title} {description}"
        symbols = self.symbol_extractor.extract_symbols(full_text)

        return NewsArticle(
            title=title,
            source=f"NewsAPI-{source}",
            url=article_data.get('url', ''),
            published_date=published_date,
            author=article_data.get('author'),
            symbols_mentioned=symbols,
            content=description
        )

class YahooFinanceImporter:
    """RELAXED Yahoo Finance news scraper - less aggressive filtering"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        })
        self.symbol_extractor = SymbolExtractor()

    def get_financial_news(self, sections: List[str] = None, max_articles: int = 100) -> List[NewsArticle]:
        """Scrape news from Yahoo Finance with RELAXED filtering"""
        if sections is None:
            sections = [
                'https://finance.yahoo.com/news/',
                'https://finance.yahoo.com/topic/stock-market-news/',
                'https://finance.yahoo.com/topic/earnings/',
            ]

        all_articles = []

        for section_url in sections:
            try:
                articles = self._scrape_section_relaxed(section_url)
                all_articles.extend(articles)
                time.sleep(2)

                if len(all_articles) >= max_articles:
                    break

            except Exception as e:
                print(f"Error scraping Yahoo Finance section {section_url}: {e}")
                continue

        unique_articles = self._remove_duplicates(all_articles)
        print(f"Fetched {len(unique_articles)} unique articles from Yahoo Finance")

        return unique_articles[:max_articles]

    def _scrape_section_relaxed(self, url: str) -> List[NewsArticle]:
        """RELAXED: Scrape with minimal filtering"""
        try:
            print(f"Scraping Yahoo Finance section: {url}")
            response = self.session.get(url, timeout=15)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, 'html.parser')
            articles = []

            # Use the same selectors that were working before
            selectors = [
                'h3 a',
                'a[data-testid]',
                'a[href*="/news/"]',
                '.js-content-viewer a',
                '[data-module] a',
                '.simpTicker a',
            ]

            all_links = []
            for selector in selectors:
                links = soup.select(selector)
                if links:
                    print(f"Found {len(links)} links with selector: {selector}")
                    all_links.extend(links)

            # MINIMAL filtering - only remove completely empty or obvious junk
            processed_links = []
            seen_hrefs = set()

            # Only filter out these obvious navigation items
            skip_keywords = ['sign in', 'more...', 'newsletters']

            for link in all_links:
                href = link.get('href', '')
                title_text = link.get_text(strip=True)

                if (href and href not in seen_hrefs and title_text and
                    len(title_text) >= 3 and  # Very minimal length
                    title_text.lower() not in skip_keywords):

                    seen_hrefs.add(href)
                    processed_links.append(link)

            print(f"Processing {len(processed_links)} links from {url}")

            for link in processed_links[:50]:
                try:
                    title = link.get_text(strip=True)
                    href = link.get('href', '')

                    if not title or len(title) < 3:
                        continue

                    # Simple title cleaning
                    title = ' '.join(title.split())

                    # Build full URL
                    if href.startswith('/'):
                        full_url = f"https://finance.yahoo.com{href}"
                    elif href.startswith('http'):
                        full_url = href
                    else:
                        continue

                    # Extract symbols with enhanced extractor
                    symbols = self.symbol_extractor.extract_symbols(title)

                    article = NewsArticle(
                        title=title,
                        source="Yahoo Finance",
                        url=full_url,
                        published_date=datetime.now(),
                        symbols_mentioned=symbols
                    )

                    articles.append(article)
                    print(f" Extracted: {title[:60]}... | Symbols: {symbols}")

                except Exception as e:
                    print(f"Error processing link: {e}")
                    continue

            print(f"Successfully scraped {len(articles)} articles from {url}")
            return articles

        except Exception as e:
            print(f"Error scraping Yahoo Finance section {url}: {e}")
            return []

    def _remove_duplicates(self, articles: List[NewsArticle]) -> List[NewsArticle]:
        """Remove duplicate articles"""
        unique_articles = []
        seen_titles = set()

        for article in articles:
            title_key = article.title[:20].lower().strip()
            if title_key not in seen_titles:
                seen_titles.add(title_key)
                unique_articles.append(article)

        return unique_articles

class RedditImporter:
    """Reddit integration for sentiment analysis"""

    def __init__(self,
                 client_id: Optional[str] = None,
                 client_secret: Optional[str] = None,
                 user_agent: Optional[str] = None):

        self.client_id = client_id or os.getenv('REDDIT_CLIENT_ID')
        self.client_secret = client_secret or os.getenv('REDDIT_CLIENT_SECRET')
        self.user_agent = user_agent or os.getenv('REDDIT_USER_AGENT')
        self.symbol_extractor = SymbolExtractor()

        if not all([self.client_id, self.client_secret, self.user_agent]):
            raise ValueError("Reddit credentials not found.")

        try:
            self.reddit = praw.Reddit(
                client_id=self.client_id,
                client_secret=self.client_secret,
                user_agent=self.user_agent
            )
            self.reddit.user.me()
            print("Reddit API connection successful")
        except Exception as e:
            print(f"Reddit API connection failed: {e}")
            self.reddit = None

    def get_trading_posts(self,
                         subreddits: List[str] = None,
                         limit: int = 100) -> List[NewsArticle]:
        """Fetch trading-related post titles from specified subreddits"""
        if not self.reddit:
            print("Reddit API not available")
            return []

        if subreddits is None:
            subreddits = [
                'wallstreetbets', 'investing', 'stocks', 'StockMarket',
                'SecurityAnalysis', 'ValueInvesting', 'pennystocks', 'options'
            ]

        articles = []

        for subreddit_name in subreddits:
            try:
                subreddit = self.reddit.subreddit(subreddit_name)
                posts = subreddit.hot(limit=limit)

                for post in posts:
                    try:
                        post_date = datetime.fromtimestamp(post.created_utc)
                        if (datetime.now() - post_date).days > 1:
                            continue

                        article = self._parse_reddit_post(post, subreddit_name)
                        if article:
                            articles.append(article)

                    except Exception as e:
                        print(f"Error parsing Reddit post: {e}")
                        continue

                time.sleep(0.1)

            except Exception as e:
                print(f"Error accessing subreddit {subreddit_name}: {e}")
                continue

        print(f"Fetched {len(articles)} posts from Reddit")
        return articles

    def _parse_reddit_post(self, post, subreddit_name: str) -> Optional[NewsArticle]:
        """Parse Reddit post into NewsArticle object"""
        title = post.title.strip()

        if not title or len(title) < 5:
            return None

        # Less restrictive filtering
        excluded_keywords = ['daily thread', 'daily discussion', 'weekly thread']
        if any(keyword in title.lower() for keyword in excluded_keywords):
            return None

        try:
            post_date = datetime.fromtimestamp(post.created_utc)
        except (ValueError, OSError, OverflowError):
            post_date = datetime.now()

        # Extract symbols with enhanced extractor
        symbols = self.symbol_extractor.extract_symbols(title)

        return NewsArticle(
            title=title,
            source=f"Reddit-{subreddit_name}",
            url=f"https://reddit.com{post.permalink}",
            published_date=post_date,
            author=str(post.author) if post.author else None,
            symbols_mentioned=symbols,
            engagement_metrics={
                'upvotes': getattr(post, 'ups', 0),
                'downvotes': getattr(post, 'downs', 0),
                'upvote_ratio': getattr(post, 'upvote_ratio', 0.5),
                'comments': getattr(post, 'num_comments', 0),
                'awards': getattr(post, 'total_awards_received', 0)
            }
        )

class NewsSentimentCollector:
    """Complete news collector with all importers"""

    def __init__(self):
        try:
            self.news_api = NewsAPIImporter()
            print("NewsAPI initialized successfully")
        except Exception as e:
            print(f"NewsAPI initialization failed: {e}")
            self.news_api = None

        try:
            self.yahoo_finance = YahooFinanceImporter()
            print("Yahoo Finance scraper initialized successfully")
        except Exception as e:
            print(f"Yahoo Finance initialization failed: {e}")
            self.yahoo_finance = None

        try:
            self.reddit = RedditImporter()
            print("Reddit API initialized successfully")
        except Exception as e:
            print(f"Reddit initialization failed: {e}")
            self.reddit = None

    def collect_all_news(self, hours_back: int = 24) -> List[NewsArticle]:
        """Collect news from all available sources"""
        all_articles = []

        # Collect from NewsAPI
        if self.news_api:
            try:
                news_articles = self.news_api.get_financial_news(hours_back=hours_back)
                all_articles.extend(news_articles)
                print(f"Collected {len(news_articles)} articles from NewsAPI")
            except Exception as e:
                print(f"Error collecting from NewsAPI: {e}")

        # Collect from Yahoo Finance
        if self.yahoo_finance:
            try:
                yahoo_articles = self.yahoo_finance.get_financial_news(max_articles=50)
                all_articles.extend(yahoo_articles)
                print(f"Collected {len(yahoo_articles)} articles from Yahoo Finance")
            except Exception as e:
                print(f"Error collecting from Yahoo Finance: {e}")

        # Collect from Reddit
        if self.reddit:
            try:
                reddit_articles = self.reddit.get_trading_posts()
                all_articles.extend(reddit_articles)
                print(f"Collected {len(reddit_articles)} posts from Reddit")
            except Exception as e:
                print(f"Error collecting from Reddit: {e}")

        # Remove duplicates
        unique_articles = self._remove_duplicates(all_articles)
        print(f"Total unique articles collected: {len(unique_articles)}")

        # Show detailed symbol statistics
        self._show_detailed_symbol_stats(unique_articles)

        return unique_articles

    def _remove_duplicates(self, articles: List[NewsArticle]) -> List[NewsArticle]:
        """Remove duplicate articles"""
        unique_articles = []
        seen_titles = set()

        for article in articles:
            title_key = article.title[:30].lower().strip()
            if title_key not in seen_titles:
                seen_titles.add(title_key)
                unique_articles.append(article)

        return unique_articles

    def _show_detailed_symbol_stats(self, articles: List[NewsArticle]):
        """Show detailed symbol extraction statistics"""
        all_symbols = []
        articles_with_symbols = 0
        source_stats = {}

        for article in articles:
            source = article.source.split('-')[0]
            if source not in source_stats:
                source_stats[source] = {'total': 0, 'with_symbols': 0, 'symbols': []}

            source_stats[source]['total'] += 1

            if article.symbols_mentioned:
                articles_with_symbols += 1
                source_stats[source]['with_symbols'] += 1
                source_stats[source]['symbols'].extend(article.symbols_mentioned)
                all_symbols.extend(article.symbols_mentioned)

        print(f"\nDETAILED Symbol Extraction Results:")
        print(f"   Total articles: {len(articles)}")
        print(f"   Articles with symbols: {articles_with_symbols} ({articles_with_symbols/len(articles)*100:.1f}%)")
        print(f"   Unique symbols found: {len(set(all_symbols))}")

        print(f"\nBy Source:")
        for source, stats in source_stats.items():
            symbol_rate = (stats['with_symbols'] / stats['total']) * 100 if stats['total'] > 0 else 0
            unique_symbols = len(set(stats['symbols']))
            print(f"   {source}: {stats['with_symbols']}/{stats['total']} articles ({symbol_rate:.1f}%) | {unique_symbols} unique symbols")

        if all_symbols:
            from collections import Counter
            symbol_counts = Counter(all_symbols)
            print(f"\nTop mentioned symbols:")
            for symbol, count in symbol_counts.most_common(15):
                print(f"     {symbol}: {count}")
        else:
            print("No symbols extracted from any articles")

def test_complete_news_collection():
    """Test the complete news collection system"""
    print("Testing COMPLETE News Collection with All Sources...")
    print("=" * 60)

    collector = NewsSentimentCollector()
    articles = collector.collect_all_news(hours_back=6)

    if articles:
        print(f"\nCollected {len(articles)} articles total")

        # Show examples with symbols from each source
        print(f"\nSample articles with symbols by source:")

        sources = {}
        for article in articles:
            source = article.source.split('-')[0]
            if source not in sources:
                sources[source] = []
            if article.symbols_mentioned:
                sources[source].append(article)

        for source, source_articles in sources.items():
            if source_articles:
                print(f"\n--- {source} ---")
                for i, article in enumerate(source_articles[:3]):
                    print(f"  {i+1}. {article.title[:80]}...")
                    print(f"     Symbols: {article.symbols_mentioned}")

    else:
        print("No articles collected")

    return articles

if __name__ == "__main__":
    test_complete_news_collection()
