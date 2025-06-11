import os
import requests
import praw
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any
import time
import logging
from dataclasses import dataclass, field
import re
from dotenv import load_dotenv
import yfinance as yf
from bs4 import BeautifulSoup
from urllib.parse import urljoin

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class NewsArticle:
    """Standardized news article structure - TITLE FOCUSED"""
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

class NewsAPIImporter:
    """
    NewsAPI integration for financial news titles
    """

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv('NEWS_API_KEY')
        if not self.api_key:
            raise ValueError("NewsAPI key not found. Set NEWS_API_KEY environment variable.")

        self.base_url = "https://newsapi.org/v2"
        self.session = requests.Session()
        self.daily_requests = 0
        self.max_daily_requests = 500

    def get_financial_news(self,
                          query: str = "business",
                          language: str = "en",
                          sort_by: str = "publishedAt",
                          page_size: int = 50,
                          hours_back: int = 168) -> List[NewsArticle]:
        """
        Fetch recent financial news article titles
        """
        if self.daily_requests >= self.max_daily_requests:
            logger.warning("Daily NewsAPI limit reached")
            return []

        # Try multiple strategies
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

                # Add query or sources
                if strategy["sources"]:
                    params['sources'] = strategy["sources"]
                else:
                    params['q'] = strategy["q"]

                logger.info(f"NewsAPI strategy {i+1}: {strategy}")
                response = self.session.get(url, params=params, timeout=30)
                response.raise_for_status()
                self.daily_requests += 1

                data = response.json()

                if data['status'] != 'ok':
                    logger.error(f"NewsAPI error: {data.get('message', 'Unknown error')}")
                    continue

                total_results = data.get('totalResults', 0)
                logger.info(f"Strategy {i+1} found {total_results} total results")

                if total_results > 0:
                    articles = []
                    for article_data in data.get('articles', []):
                        try:
                            article = self._parse_newsapi_article(article_data)
                            if article:
                                articles.append(article)
                        except Exception as e:
                            logger.warning(f"Error parsing article: {e}")
                            continue

                    if len(articles) > 0:
                        logger.info(f"Fetched {len(articles)} articles from NewsAPI strategy {i+1}")
                        return articles

            except requests.exceptions.RequestException as e:
                logger.error(f"NewsAPI strategy {i+1} failed: {e}")
                continue

        # Final fallback to headlines
        logger.info("All NewsAPI strategies failed, trying headlines...")
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
                logger.error(f"NewsAPI headlines error: {data.get('message', 'Unknown error')}")
                return []

            articles = []
            for article_data in data.get('articles', []):
                try:
                    article = self._parse_newsapi_article(article_data)
                    if article:
                        articles.append(article)
                except Exception as e:
                    continue

            logger.info(f"Fetched {len(articles)} headlines from NewsAPI fallback")
            return articles

        except Exception as e:
            logger.error(f"NewsAPI headlines fallback failed: {e}")
            return []

    def _parse_newsapi_article(self, article_data: Dict) -> Optional[NewsArticle]:
        """Parse NewsAPI article data into NewsArticle object - TITLE ONLY"""

        title = article_data.get('title', '').strip()

        if not title or title == '[Removed]' or len(title) < 10:
            return None

        # Parse publication date and make timezone-naive
        published_str = article_data.get('publishedAt', '')
        try:
            published_date = datetime.fromisoformat(published_str.replace('Z', '+00:00'))
            if published_date.tzinfo is not None:
                published_date = published_date.replace(tzinfo=None)
        except:
            published_date = datetime.now()

        source = article_data.get('source', {}).get('name', 'Unknown')

        return NewsArticle(
            title=title,
            source=f"NewsAPI-{source}",
            url=article_data.get('url', ''),
            published_date=published_date,
            author=article_data.get('author'),
            symbols_mentioned=self._extract_symbols(title)
        )

    def _extract_symbols(self, text: str) -> List[str]:
        """Extract stock symbols from NewsAPI text"""
        symbols = set()

        # Look for $SYMBOL format
        dollar_symbols = re.findall(r'\$([A-Z]{1,5})\b', text)
        symbols.update(dollar_symbols)

        # Known stock symbols to match against
        known_symbols = {
            'AAPL', 'MSFT', 'GOOGL', 'GOOG', 'AMZN', 'TSLA', 'META', 'NVDA', 'BRK', 'UNH',
            'JNJ', 'JPM', 'V', 'PG', 'HD', 'CVX', 'MA', 'ABBV', 'PFE', 'AVGO', 'COST',
            'DIS', 'KO', 'PEP', 'TMO', 'WMT', 'ABT', 'ACN', 'VZ', 'DHR', 'LLY', 'NKE',
            'TXN', 'NEE', 'ORCL', 'CRM', 'ADBE', 'XOM', 'MRK', 'BAC', 'CSCO', 'NFLX',
            'INTC', 'LIN', 'AMD', 'QCOM', 'RTX', 'UPS', 'INTU', 'IBM', 'CAT',
            'GS', 'SPGI', 'MDT', 'HON', 'SBUX', 'AXP', 'BKNG', 'DE', 'TJX',
            'GILD', 'ADP', 'MMM', 'BLK', 'SYK', 'CVS', 'MDLZ', 'CI', 'AMT', 'VRTX',
            'MO', 'PYPL', 'SCHW', 'LRCX', 'ZTS', 'CB', 'ISRG', 'DUK', 'BSX',
            'SPY', 'QQQ', 'IWM', 'DIA', 'VTI', 'VOO', 'VEA', 'VWO', 'AGG', 'BND',
            'PLTR', 'GME', 'AMC', 'BB', 'NOK', 'WISH', 'CLOV', 'RBLX', 'COIN', 'HOOD',
            'OKLO', 'SOFI', 'RIVN', 'LCID', 'GE', 'UBER', 'LYFT', 'SNAP'
        }

        # Look for known symbols in text
        words = re.findall(r'\b[A-Za-z]{1,5}\b', text)
        for word in words:
            if word.upper() in known_symbols:
                symbols.add(word.upper())

        return list(symbols)

class YahooFinanceImporter:
    """
    Yahoo Finance news scraper for financial headlines
    """

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })

    def get_financial_news(self, sections: List[str] = None, max_articles: int = 100) -> List[NewsArticle]:
        """
        Scrape news titles from Yahoo Finance
        """
        if sections is None:
            sections = [
                'https://finance.yahoo.com/news/',
                'https://finance.yahoo.com/topic/stock-market-news/',
                'https://finance.yahoo.com/topic/earnings/',
                'https://finance.yahoo.com/topic/economic-news/',
                'https://finance.yahoo.com/sector/ms_technology/',
                'https://finance.yahoo.com/sector/ms_healthcare/',
                'https://finance.yahoo.com/sector/ms_financial_services/',
                'https://finance.yahoo.com/trending-tickers/'
            ]

        all_articles = []

        for section_url in sections:
            try:
                articles = self._scrape_section(section_url)
                all_articles.extend(articles)
                time.sleep(1)  # Be respectful to Yahoo's servers

                if len(all_articles) >= max_articles:
                    break

            except Exception as e:
                logger.warning(f"Error scraping Yahoo Finance section {section_url}: {e}")
                continue

        # Remove duplicates and limit results
        unique_articles = self._remove_duplicates(all_articles)
        logger.info(f"Fetched {len(unique_articles)} unique articles from Yahoo Finance")

        return unique_articles[:max_articles]

    def _scrape_section(self, url: str) -> List[NewsArticle]:
        """Scrape a specific Yahoo Finance section"""
        try:
            logger.info(f"Scraping Yahoo Finance section: {url}")
            response = self.session.get(url, timeout=15)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, 'html.parser')
            articles = []

            # Yahoo Finance uses different selectors for different pages
            selectors = [
                'h3[data-test-locator="cachedev-stream-item-title"] a',
                'h3 a[data-test-locator]',
                '.js-content-viewer a[data-test-locator]',
                'a[data-test-locator="headline"]',
                'a[data-testid="headline-link"]',
                '.simpTicker a',
                '[data-module="stream"] a',
                'h3 a',
                '.Mb\(5px\) a',
                '.Fw\(600\) a',
                'a[href*="/news/"]',
            ]

            links_found = []
            for selector in selectors:
                links = soup.select(selector)
                if links:
                    logger.info(f"Found {len(links)} links with selector: {selector}")
                    links_found.extend(links)

            # Remove duplicates from links_found
            unique_links = []
            seen_hrefs = set()
            for link in links_found:
                href = link.get('href', '')
                if href and href not in seen_hrefs:
                    seen_hrefs.add(href)
                    unique_links.append(link)

            if not unique_links:
                logger.warning(f"No links found for {url}")
                return []

            logger.info(f"Processing {len(unique_links)} unique links from {url}")

            for link in unique_links[:20]:
                try:
                    title = link.get_text(strip=True)
                    href = link.get('href', '')

                    if not title or len(title) < 10:
                        logger.debug(f"Skipping short title: {title}")
                        continue

                    # Clean up title
                    title = self._clean_title(title)

                    # Skip if title is still too short after cleaning or empty
                    if not title or len(title) < 15:
                        logger.debug(f"Skipping short cleaned title: {title}")
                        continue

                    # Build full URL
                    if href.startswith('/'):
                        full_url = f"https://finance.yahoo.com{href}"
                    elif href.startswith('http'):
                        full_url = href
                    else:
                        logger.debug(f"Skipping invalid href: {href}")
                        continue

                    # Extract timestamp
                    published_date = datetime.now()

                    article = NewsArticle(
                        title=title,
                        source="Yahoo Finance",
                        url=full_url,
                        published_date=published_date,
                        symbols_mentioned=self._extract_symbols(title)
                    )

                    articles.append(article)
                    logger.debug(f"Successfully parsed: {title[:50]}...")

                except Exception as e:
                    logger.debug(f"Error parsing individual article: {e}")
                    continue

            logger.info(f"Successfully scraped {len(articles)} articles from {url}")
            return articles

        except Exception as e:
            logger.error(f"Error scraping Yahoo Finance section {url}: {e}")
            return []

    def _clean_title(self, title: str) -> str:
      """Clean and normalize article titles - simplified version"""
      # Remove extra whitespace
      title = ' '.join(title.split())

      # Only skip if completely empty or very short
      if len(title) < 5:
          return ""

      return title.strip()

    def _extract_symbols(self, text: str) -> List[str]:
        """Extract stock symbols from Yahoo Finance titles"""
        symbols = set()

        # Look for ticker symbols in parentheses: "Apple (AAPL) reports earnings"
        paren_symbols = re.findall(r'\(([A-Z]{1,5})\)', text)
        symbols.update(paren_symbols)

        # Look for $SYMBOL format
        dollar_symbols = re.findall(r'\$([A-Z]{1,5})\b', text)
        symbols.update(dollar_symbols)

        # Known symbols (same as other importers)
        known_symbols = {
            'AAPL', 'MSFT', 'GOOGL', 'GOOG', 'AMZN', 'TSLA', 'META', 'NVDA', 'BRK', 'UNH',
            'JNJ', 'JPM', 'V', 'PG', 'HD', 'CVX', 'MA', 'ABBV', 'PFE', 'AVGO', 'COST',
            'DIS', 'KO', 'PEP', 'TMO', 'WMT', 'ABT', 'ACN', 'VZ', 'DHR', 'LLY', 'NKE',
            'TXN', 'NEE', 'ORCL', 'CRM', 'ADBE', 'XOM', 'MRK', 'BAC', 'CSCO', 'NFLX',
            'INTC', 'LIN', 'AMD', 'QCOM', 'RTX', 'UPS', 'INTU', 'IBM', 'CAT',
            'GS', 'SPGI', 'MDT', 'HON', 'SBUX', 'AXP', 'BKNG', 'DE', 'TJX',
            'GILD', 'ADP', 'MMM', 'BLK', 'SYK', 'CVS', 'MDLZ', 'CI', 'AMT', 'VRTX',
            'MO', 'PYPL', 'SCHW', 'LRCX', 'ZTS', 'CB', 'ISRG', 'DUK', 'BSX',
            'SPY', 'QQQ', 'IWM', 'DIA', 'VTI', 'VOO', 'VEA', 'VWO', 'AGG', 'BND',
            'PLTR', 'GME', 'AMC', 'BB', 'NOK', 'WISH', 'CLOV', 'RBLX', 'COIN', 'HOOD',
            'OKLO', 'SOFI', 'RIVN', 'LCID', 'GE', 'UBER', 'LYFT', 'SNAP'
        }

        # Look for known symbols in text
        words = re.findall(r'\b[A-Za-z]{1,5}\b', text)
        for word in words:
            if word.upper() in known_symbols:
                symbols.add(word.upper())

        return list(symbols)

    def _remove_duplicates(self, articles: List[NewsArticle]) -> List[NewsArticle]:
        """Remove duplicate articles based on title similarity"""
        unique_articles = []
        seen_titles = set()

        for article in articles:
            title_key = article.title[:30].lower().strip()
            if title_key not in seen_titles:
                seen_titles.add(title_key)
                unique_articles.append(article)

        return unique_articles

class RedditImporter:
    """
    Reddit integration for sentiment analysis - TITLE FOCUSED
    """

    def __init__(self,
                 client_id: Optional[str] = None,
                 client_secret: Optional[str] = None,
                 user_agent: Optional[str] = None):

        self.client_id = client_id or os.getenv('REDDIT_CLIENT_ID')
        self.client_secret = client_secret or os.getenv('REDDIT_CLIENT_SECRET')
        self.user_agent = user_agent or os.getenv('REDDIT_USER_AGENT')

        if not all([self.client_id, self.client_secret, self.user_agent]):
            raise ValueError("Reddit credentials not found.")

        try:
            self.reddit = praw.Reddit(
                client_id=self.client_id,
                client_secret=self.client_secret,
                user_agent=self.user_agent
            )
            self.reddit.user.me()
            logger.info("Reddit API connection successful")
        except Exception as e:
            logger.error(f"Reddit API connection failed: {e}")
            self.reddit = None

    def get_trading_posts(self,
                         subreddits: List[str] = None,
                         limit: int = 100) -> List[NewsArticle]:
        """
        Fetch trading-related post titles from specified subreddits
        """
        if not self.reddit:
            logger.error("Reddit API not available")
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
                        logger.warning(f"Error parsing Reddit post: {e}")
                        continue

                time.sleep(0.1)

            except Exception as e:
                logger.error(f"Error accessing subreddit {subreddit_name}: {e}")
                continue

        logger.info(f"Fetched {len(articles)} posts from Reddit")
        return articles

    def _parse_reddit_post(self, post, subreddit_name: str) -> Optional[NewsArticle]:
        """Parse Reddit post into NewsArticle object - TITLE ONLY"""

        title = post.title.strip()

        if not title or len(title) < 10:
            return None

        # Filter out common non-trading posts
        excluded_keywords = ['daily thread', 'what are your moves', 'daily discussion']
        if any(keyword in title.lower() for keyword in excluded_keywords):
            return None

        try:
            post_date = datetime.fromtimestamp(post.created_utc)
        except (ValueError, OSError, OverflowError):
            post_date = datetime.now()

        return NewsArticle(
            title=title,
            source=f"Reddit-{subreddit_name}",
            url=f"https://reddit.com{post.permalink}",
            published_date=post_date,
            author=str(post.author) if post.author else None,
            symbols_mentioned=self._extract_symbols(title),
            engagement_metrics={
                'upvotes': getattr(post, 'ups', 0),
                'downvotes': getattr(post, 'downs', 0),
                'upvote_ratio': getattr(post, 'upvote_ratio', 0.5),
                'comments': getattr(post, 'num_comments', 0),
                'awards': getattr(post, 'total_awards_received', 0)
            }
        )

    def _extract_symbols(self, text: str) -> List[str]:
        """Extract stock symbols from Reddit text - context aware"""
        symbols = set()

        # Known stock symbols
        known_symbols = {
            'AAPL', 'MSFT', 'GOOGL', 'GOOG', 'AMZN', 'TSLA', 'META', 'NVDA', 'BRK', 'UNH',
            'JNJ', 'JPM', 'PG', 'HD', 'CVX', 'MA', 'ABBV', 'PFE', 'AVGO', 'COST',
            'DIS', 'KO', 'PEP', 'TMO', 'WMT', 'ABT', 'ACN', 'VZ', 'DHR', 'LLY', 'NKE',
            'TXN', 'NEE', 'ORCL', 'CRM', 'ADBE', 'XOM', 'MRK', 'BAC', 'CSCO', 'NFLX',
            'INTC', 'LIN', 'AMD', 'QCOM', 'RTX', 'UPS', 'INTU', 'IBM', 'CAT',
            'GS', 'SPGI', 'MDT', 'HON', 'SBUX', 'AXP', 'BKNG', 'DE', 'TJX',
            'GILD', 'ADP', 'MMM', 'BLK', 'SYK', 'CVS', 'MDLZ', 'CI', 'AMT', 'VRTX',
            'MO', 'PYPL', 'SCHW', 'LRCX', 'ZTS', 'CB', 'ISRG', 'DUK', 'BSX',
            'SPY', 'QQQ', 'IWM', 'DIA', 'VTI', 'VOO', 'VEA', 'VWO', 'AGG', 'BND',
            'PLTR', 'GME', 'AMC', 'BB', 'NOK', 'WISH', 'CLOV', 'RBLX', 'COIN', 'HOOD',
            'OKLO', 'SOFI', 'RIVN', 'LCID', 'GE', 'UBER', 'LYFT', 'SNAP'
        }

        # Context-aware trading patterns
        trading_patterns = [
            r'\$(' + '|'.join(known_symbols) + r')\b',
            r'\b(' + '|'.join(known_symbols) + r')\s+(?:calls?|puts?|options?|stock|shares?)\b',
            r'(?:bought?|sold?|buying|selling)\s+(' + '|'.join(known_symbols) + r')\b',
            r'\b(' + '|'.join(known_symbols) + r')\s+(?:up|down|gained?|lost)\s+\d+',
            r'\b(' + '|'.join(known_symbols) + r')\s+(?:to|at)\s+\$\d+',
        ]

        for pattern in trading_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            symbols.update([m.upper() for m in matches])

        return list(symbols)

class NewsSentimentCollector:
    """
    Main class to orchestrate news collection from multiple sources
    TITLE-FOCUSED VERSION for FinBERT integration
    """

    def __init__(self):
        try:
            self.news_api = NewsAPIImporter()
            logger.info("NewsAPI initialized successfully")
        except Exception as e:
            logger.warning(f"NewsAPI initialization failed: {e}")
            self.news_api = None

        try:
            self.yahoo_finance = YahooFinanceImporter()
            logger.info("Yahoo Finance scraper initialized successfully")
        except Exception as e:
            logger.warning(f"Yahoo Finance initialization failed: {e}")
            self.yahoo_finance = None

        try:
            self.reddit = RedditImporter()
            logger.info("Reddit API initialized successfully")
        except Exception as e:
            logger.warning(f"Reddit initialization failed: {e}")
            self.reddit = None

    def collect_all_news(self, hours_back: int = 24) -> List[NewsArticle]:
        """
        Collect news titles from all available sources
        """
        all_articles = []

        # Collect from NewsAPI
        if self.news_api:
            try:
                news_articles = self.news_api.get_financial_news(hours_back=hours_back)
                all_articles.extend(news_articles)
                logger.info(f"Collected {len(news_articles)} articles from NewsAPI")
            except Exception as e:
                logger.error(f"Error collecting from NewsAPI: {e}")

        # Collect from Yahoo Finance
        if self.yahoo_finance:
            try:
                yahoo_articles = self.yahoo_finance.get_financial_news(max_articles=100)
                all_articles.extend(yahoo_articles)
                logger.info(f"Collected {len(yahoo_articles)} articles from Yahoo Finance")
            except Exception as e:
                logger.error(f"Error collecting from Yahoo Finance: {e}")

        # Collect from Reddit
        if self.reddit:
            try:
                reddit_articles = self.reddit.get_trading_posts()
                all_articles.extend(reddit_articles)
                logger.info(f"Collected {len(reddit_articles)} posts from Reddit")
            except Exception as e:
                logger.error(f"Error collecting from Reddit: {e}")

        # Remove duplicates
        unique_articles = self._remove_duplicates(all_articles)
        logger.info(f"Total unique articles collected: {len(unique_articles)}")

        return unique_articles

    def _remove_duplicates(self, articles: List[NewsArticle]) -> List[NewsArticle]:
        """Remove duplicate articles based on title similarity"""
        unique_articles = []
        seen_titles = set()

        for article in articles:
            title_key = article.title[:50].lower().strip()
            if title_key not in seen_titles:
                seen_titles.add(title_key)
                unique_articles.append(article)

        return unique_articles

    def get_articles_dataframe(self, articles: List[NewsArticle]) -> pd.DataFrame:
        """Convert articles to pandas DataFrame for analysis"""
        data = []

        for article in articles:
            # Handle timestamp
            if isinstance(article.published_date, datetime):
                timestamp = article.published_date
                if timestamp.tzinfo is not None:
                    timestamp = timestamp.replace(tzinfo=None)
            else:
                timestamp = datetime.now()

            data.append({
                'timestamp': timestamp,
                'title': article.title,
                'source': article.source,
                'url': article.url,
                'author': article.author,
                'symbols_mentioned': ','.join(article.symbols_mentioned),
                'sentiment_score': article.sentiment_score,
                'sentiment_label': article.sentiment_label,
                'sentiment_confidence': article.sentiment_confidence,
                'upvotes': article.engagement_metrics.get('upvotes', 0),
                'comments': article.engagement_metrics.get('comments', 0)
            })

        df = pd.DataFrame(data)
        if not df.empty:
            df['timestamp'] = pd.to_datetime(df['timestamp'], utc=False)
            df = df.sort_values('timestamp', ascending=False)

        return df

def debug_environment():
    """Debug environment variables"""
    print("=== DEBUGGING ENVIRONMENT VARIABLES ===")
    print(f"NEWS_API_KEY: {os.getenv('NEWS_API_KEY')}")
    print(f"REDDIT_CLIENT_ID: {os.getenv('REDDIT_CLIENT_ID')}")
    print(f"REDDIT_CLIENT_SECRET: {os.getenv('REDDIT_CLIENT_SECRET')}")
    print(f"REDDIT_USER_AGENT: {os.getenv('REDDIT_USER_AGENT')}")
    print("=========================================")


    def _extract_symbols(self, text: str) -> List[str]:
        """Extract stock symbols from Yahoo Finance titles"""
        symbols = set()

        # Look for ticker symbols in parentheses: "Apple (AAPL) reports earnings"
        paren_symbols = re.findall(r'\(([A-Z]{1,5})\)', text)
        symbols.update(paren_symbols)

        # Look for $SYMBOL format
        dollar_symbols = re.findall(r'\$([A-Z]{1,5})\b', text)
        symbols.update(dollar_symbols)

        # Known symbols (same as other importers)
        known_symbols = {
            'AAPL', 'MSFT', 'GOOGL', 'GOOG', 'AMZN', 'TSLA', 'META', 'NVDA', 'BRK', 'UNH',
            'JNJ', 'JPM', 'V', 'PG', 'HD', 'CVX', 'MA', 'ABBV', 'PFE', 'AVGO', 'COST',
            'DIS', 'KO', 'PEP', 'TMO', 'WMT', 'ABT', 'ACN', 'VZ', 'DHR', 'LLY', 'NKE',
            'TXN', 'NEE', 'ORCL', 'CRM', 'ADBE', 'XOM', 'MRK', 'BAC', 'CSCO', 'NFLX',
            'INTC', 'LIN', 'AMD', 'QCOM', 'RTX', 'UPS', 'INTU', 'IBM', 'CAT',
            'GS', 'SPGI', 'MDT', 'HON', 'SBUX', 'AXP', 'BKNG', 'DE', 'TJX',
            'GILD', 'ADP', 'MMM', 'BLK', 'SYK', 'CVS', 'MDLZ', 'CI', 'AMT', 'VRTX',
            'MO', 'PYPL', 'SCHW', 'LRCX', 'ZTS', 'CB', 'ISRG', 'DUK', 'BSX',
            'SPY', 'QQQ', 'IWM', 'DIA', 'VTI', 'VOO', 'VEA', 'VWO', 'AGG', 'BND',
            'PLTR', 'GME', 'AMC', 'BB', 'NOK', 'WISH', 'CLOV', 'RBLX', 'COIN', 'HOOD',
            'OKLO', 'SOFI', 'RIVN', 'LCID', 'GE', 'UBER', 'LYFT', 'SNAP'
        }

        # Look for known symbols in text
        words = re.findall(r'\b[A-Za-z]{1,5}\b', text)
        for word in words:
            if word.upper() in known_symbols:
                symbols.add(word.upper())

        return list(symbols)

    def _remove_duplicates(self, articles: List[NewsArticle]) -> List[NewsArticle]:
        """Remove duplicate articles based on title similarity"""
        unique_articles = []
        seen_titles = set()

        for article in articles:
            title_key = article.title[:30].lower().strip()
            if title_key not in seen_titles:
                seen_titles.add(title_key)
                unique_articles.append(article)

        return unique_articles

class RedditImporter:
    """
    Reddit integration for sentiment analysis - TITLE FOCUSED
    """

    def __init__(self,
                 client_id: Optional[str] = None,
                 client_secret: Optional[str] = None,
                 user_agent: Optional[str] = None):

        self.client_id = client_id or os.getenv('REDDIT_CLIENT_ID')
        self.client_secret = client_secret or os.getenv('REDDIT_CLIENT_SECRET')
        self.user_agent = user_agent or os.getenv('REDDIT_USER_AGENT')

        if not all([self.client_id, self.client_secret, self.user_agent]):
            raise ValueError("Reddit credentials not found.")

        try:
            self.reddit = praw.Reddit(
                client_id=self.client_id,
                client_secret=self.client_secret,
                user_agent=self.user_agent
            )
            self.reddit.user.me()
            logger.info("Reddit API connection successful")
        except Exception as e:
            logger.error(f"Reddit API connection failed: {e}")
            self.reddit = None

    def get_trading_posts(self,
                         subreddits: List[str] = None,
                         limit: int = 100) -> List[NewsArticle]:
        """
        Fetch trading-related post titles from specified subreddits
        """
        if not self.reddit:
            logger.error("Reddit API not available")
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
                        logger.warning(f"Error parsing Reddit post: {e}")
                        continue

                time.sleep(0.1)

            except Exception as e:
                logger.error(f"Error accessing subreddit {subreddit_name}: {e}")
                continue

        logger.info(f"Fetched {len(articles)} posts from Reddit")
        return articles

    def _parse_reddit_post(self, post, subreddit_name: str) -> Optional[NewsArticle]:
        """Parse Reddit post into NewsArticle object - TITLE ONLY"""

        title = post.title.strip()

        if not title or len(title) < 10:
            return None

        # Filter out common non-trading posts
        excluded_keywords = ['daily thread', 'what are your moves', 'daily discussion']
        if any(keyword in title.lower() for keyword in excluded_keywords):
            return None

        try:
            post_date = datetime.fromtimestamp(post.created_utc)
        except (ValueError, OSError, OverflowError):
            post_date = datetime.now()

        return NewsArticle(
            title=title,
            source=f"Reddit-{subreddit_name}",
            url=f"https://reddit.com{post.permalink}",
            published_date=post_date,
            author=str(post.author) if post.author else None,
            symbols_mentioned=self._extract_symbols(title),
            engagement_metrics={
                'upvotes': getattr(post, 'ups', 0),
                'downvotes': getattr(post, 'downs', 0),
                'upvote_ratio': getattr(post, 'upvote_ratio', 0.5),
                'comments': getattr(post, 'num_comments', 0),
                'awards': getattr(post, 'total_awards_received', 0)
            }
        )

    def _extract_symbols(self, text: str) -> List[str]:
        """Extract stock symbols from Reddit text - context aware"""
        symbols = set()

        # Known stock symbols
        known_symbols = {
            'AAPL', 'MSFT', 'GOOGL', 'GOOG', 'AMZN', 'TSLA', 'META', 'NVDA', 'BRK', 'UNH',
            'JNJ', 'JPM', 'PG', 'HD', 'CVX', 'MA', 'ABBV', 'PFE', 'AVGO', 'COST',
            'DIS', 'KO', 'PEP', 'TMO', 'WMT', 'ABT', 'ACN', 'VZ', 'DHR', 'LLY', 'NKE',
            'TXN', 'NEE', 'ORCL', 'CRM', 'ADBE', 'XOM', 'MRK', 'BAC', 'CSCO', 'NFLX',
            'INTC', 'LIN', 'AMD', 'QCOM', 'RTX', 'UPS', 'INTU', 'IBM', 'CAT',
            'GS', 'SPGI', 'MDT', 'HON', 'SBUX', 'AXP', 'BKNG', 'DE', 'TJX',
            'GILD', 'ADP', 'MMM', 'BLK', 'SYK', 'CVS', 'MDLZ', 'CI', 'AMT', 'VRTX',
            'MO', 'PYPL', 'SCHW', 'LRCX', 'ZTS', 'CB', 'ISRG', 'DUK', 'BSX',
            'SPY', 'QQQ', 'IWM', 'DIA', 'VTI', 'VOO', 'VEA', 'VWO', 'AGG', 'BND',
            'PLTR', 'GME', 'AMC', 'BB', 'NOK', 'WISH', 'CLOV', 'RBLX', 'COIN', 'HOOD',
            'OKLO', 'SOFI', 'RIVN', 'LCID', 'GE', 'UBER', 'LYFT', 'SNAP'
        }

        # Context-aware trading patterns
        trading_patterns = [
            r'\$(' + '|'.join(known_symbols) + r')\b',
            r'\b(' + '|'.join(known_symbols) + r')\s+(?:calls?|puts?|options?|stock|shares?)\b',
            r'(?:bought?|sold?|buying|selling)\s+(' + '|'.join(known_symbols) + r')\b',
            r'\b(' + '|'.join(known_symbols) + r')\s+(?:up|down|gained?|lost)\s+\d+',
            r'\b(' + '|'.join(known_symbols) + r')\s+(?:to|at)\s+\$\d+',
        ]

        for pattern in trading_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            symbols.update([m.upper() for m in matches])

        return list(symbols)

class NewsSentimentCollector:
    """
    Main class to orchestrate news collection from multiple sources
    TITLE-FOCUSED VERSION for FinBERT integration
    """

    def __init__(self):
        try:
            self.news_api = NewsAPIImporter()
            logger.info("NewsAPI initialized successfully")
        except Exception as e:
            logger.warning(f"NewsAPI initialization failed: {e}")
            self.news_api = None

        try:
            self.yahoo_finance = YahooFinanceImporter()
            logger.info("Yahoo Finance scraper initialized successfully")
        except Exception as e:
            logger.warning(f"Yahoo Finance initialization failed: {e}")
            self.yahoo_finance = None

        try:
            self.reddit = RedditImporter()
            logger.info("Reddit API initialized successfully")
        except Exception as e:
            logger.warning(f"Reddit initialization failed: {e}")
            self.reddit = None

    def collect_all_news(self, hours_back: int = 24) -> List[NewsArticle]:
        """
        Collect news titles from all available sources
        """
        all_articles = []

        # Collect from NewsAPI
        if self.news_api:
            try:
                news_articles = self.news_api.get_financial_news(hours_back=hours_back)
                all_articles.extend(news_articles)
                logger.info(f"Collected {len(news_articles)} articles from NewsAPI")
            except Exception as e:
                logger.error(f"Error collecting from NewsAPI: {e}")

        # Collect from Yahoo Finance
        if self.yahoo_finance:
            try:
                yahoo_articles = self.yahoo_finance.get_financial_news(max_articles=100)
                all_articles.extend(yahoo_articles)
                logger.info(f"Collected {len(yahoo_articles)} articles from Yahoo Finance")
            except Exception as e:
                logger.error(f"Error collecting from Yahoo Finance: {e}")

        # Collect from Reddit
        if self.reddit:
            try:
                reddit_articles = self.reddit.get_trading_posts()
                all_articles.extend(reddit_articles)
                logger.info(f"Collected {len(reddit_articles)} posts from Reddit")
            except Exception as e:
                logger.error(f"Error collecting from Reddit: {e}")

        # Remove duplicates
        unique_articles = self._remove_duplicates(all_articles)
        logger.info(f"Total unique articles collected: {len(unique_articles)}")

        return unique_articles

    def _remove_duplicates(self, articles: List[NewsArticle]) -> List[NewsArticle]:
        """Remove duplicate articles based on title similarity"""
        unique_articles = []
        seen_titles = set()

        for article in articles:
            title_key = article.title[:50].lower().strip()
            if title_key not in seen_titles:
                seen_titles.add(title_key)
                unique_articles.append(article)

        return unique_articles

    def get_articles_dataframe(self, articles: List[NewsArticle]) -> pd.DataFrame:
        """Convert articles to pandas DataFrame for analysis"""
        data = []

        for article in articles:
            # Handle timestamp
            if isinstance(article.published_date, datetime):
                timestamp = article.published_date
                if timestamp.tzinfo is not None:
                    timestamp = timestamp.replace(tzinfo=None)
            else:
                timestamp = datetime.now()

            data.append({
                'timestamp': timestamp,
                'title': article.title,
                'source': article.source,
                'url': article.url,
                'author': article.author,
                'symbols_mentioned': ','.join(article.symbols_mentioned),
                'sentiment_score': article.sentiment_score,
                'sentiment_label': article.sentiment_label,
                'sentiment_confidence': article.sentiment_confidence,
                'upvotes': article.engagement_metrics.get('upvotes', 0),
                'comments': article.engagement_metrics.get('comments', 0)
            })

        df = pd.DataFrame(data)
        if not df.empty:
            df['timestamp'] = pd.to_datetime(df['timestamp'], utc=False)
            df = df.sort_values('timestamp', ascending=False)

        return df

def debug_environment():
    """Debug environment variables"""
    print("=== DEBUGGING ENVIRONMENT VARIABLES ===")
    print(f"NEWS_API_KEY: {os.getenv('NEWS_API_KEY')}")
    print(f"REDDIT_CLIENT_ID: {os.getenv('REDDIT_CLIENT_ID')}")
    print(f"REDDIT_CLIENT_SECRET: {os.getenv('REDDIT_CLIENT_SECRET')}")
    print(f"REDDIT_USER_AGENT: {os.getenv('REDDIT_USER_AGENT')}")
    print("=========================================")

def test_news_collection():
    """Test the title-focused news collection"""
    debug_environment()

    collector = NewsSentimentCollector()

    print("Testing title-focused news collection...")
    articles = collector.collect_all_news(hours_back=6)

    if articles:
        print(f"\nCollected {len(articles)} articles (titles only)")

        # Show first few articles
        for i, article in enumerate(articles[:5]):
            print(f"\n--- Article {i+1} ---")
            print(f"Source: {article.source}")
            print(f"Title: {article.title}")
            print(f"Symbols: {article.symbols_mentioned}")
            print(f"Date: {article.published_date}")
            print(f"URL: {article.url[:80]}...")
            if article.engagement_metrics:
                print(f"Engagement: {article.engagement_metrics}")

        # Show symbol distribution
        all_symbols = []
        for article in articles:
            all_symbols.extend(article.symbols_mentioned)

        if all_symbols:
            from collections import Counter
            symbol_counts = Counter(all_symbols)
            print(f"\nTop mentioned symbols:")
            for symbol, count in symbol_counts.most_common(10):
                print(f"  {symbol}: {count}")

        print(f"\n📊 Ready for FinBERT analysis!")
        print(f"   - {len(articles)} titles collected")
        print(f"   - Average title length: {sum(len(a.title) for a in articles) / len(articles):.1f} chars")
        print(f"   - Sources: {set(a.source.split('-')[0] for a in articles)}")

    else:
        print("No articles collected. Check your API keys and internet connection.")

    return articles

if __name__ == "__main__":
    # Install required packages if running standalone
    try:
        import bs4
    except ImportError:
        print("❌ Missing required package. Install with: pip install beautifulsoup4")
        exit(1)

    test_news_collection()
