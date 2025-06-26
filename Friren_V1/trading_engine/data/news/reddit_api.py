"""
Reddit News Data Collector

Social sentiment and retail trader buzz from Reddit.
Key subreddits: wallstreetbets, investing, stocks, options.
"""

import os
import praw
import time
from datetime import datetime, timedelta
from typing import List, Optional, Dict
import logging
import threading

try:
    from .base import NewsDataSource, NewsArticle
    from .yahoo_news import SymbolExtractor
except ImportError:
    from base import NewsDataSource, NewsArticle
    from yahoo_news import SymbolExtractor


class RedditSymbolExtractor(SymbolExtractor):
    """Enhanced symbol extraction for Reddit trading posts with slang"""

    def __init__(self):
        super().__init__()

        # Reddit-specific trading patterns and slang
        self.reddit_trading_terms = [
            r'\b(' + '|'.join(self.known_symbols) + r')\s+(?:calls?|puts?|options?)\b',
            r'(?:bought?|sold?|buying|selling)\s+(' + '|'.join(self.known_symbols) + r')\b',
            r'\b(' + '|'.join(self.known_symbols) + r')\s+(?:to the moon|moon|rocket|diamond hands|hodl)\b',
            r'\b(' + '|'.join(self.known_symbols) + r')\s+(?:yolo|squeeze|apes?|tendies)\b',
            r'(?:position|holding|bag)\s+(' + '|'.join(self.known_symbols) + r')\b',
            r'\b(' + '|'.join(self.known_symbols) + r')\s+(?:gain|loss|profit|rip)\b',
            r'\$(' + '|'.join(self.known_symbols) + r')\b',  # $AAPL format
        ]

    def extract_symbols(self, text: str) -> List[str]:
        """Enhanced symbol extraction for Reddit trading posts"""
        import re

        # Get base symbols first
        symbols = set(super().extract_symbols(text))

        # Add Reddit-specific trading patterns
        for pattern in self.reddit_trading_terms:
            matches = re.findall(pattern, text, re.IGNORECASE)
            symbols.update(m.upper() for m in matches)

        return sorted(list(symbols))


class RedditNews(NewsDataSource):
    """Clean Reddit collector for trading sentiment and social buzz"""

    def __init__(self, client_id: Optional[str] = None, client_secret: Optional[str] = None,
                 user_agent: Optional[str] = None):
        super().__init__("Reddit")

        # Reddit API credentials
        self.client_id = client_id or os.getenv('REDDIT_CLIENT_ID')
        self.client_secret = client_secret or os.getenv('REDDIT_CLIENT_SECRET')
        self.user_agent = user_agent or os.getenv('REDDIT_USER_AGENT', 'TradingBot:1.0')

        if not all([self.client_id, self.client_secret]):
            raise ValueError("Reddit credentials not found. Set REDDIT_CLIENT_ID and REDDIT_CLIENT_SECRET environment variables.")

        self.logger = logging.getLogger(f"{__name__}.RedditNews")
        self.symbol_extractor = RedditSymbolExtractor()

        # Initialize Reddit connection with error handling
        self.reddit = None
        self._initialize_reddit()

        # Curated trading subreddits (prioritized by quality)
        self.trading_subreddits = [
            'wallstreetbets',     # High-energy retail trading (most active)
            'investing',          # Long-term investment discussion
            'stocks',            # General stock discussion
            'SecurityAnalysis',  # Fundamental analysis
            'ValueInvesting',    # Value investing strategies
            'options',           # Options trading
            'StockMarket',       # Market analysis
            'pennystocks',       # Penny stock discussion
        ]

        # Track API usage for rate limiting
        self.requests_made = 0
        self.last_request_time = time.time()

    def _initialize_reddit(self):
        """Initialize Reddit connection with proper error handling"""
        try:
            self.reddit = praw.Reddit(
                client_id=self.client_id,
                client_secret=self.client_secret,
                user_agent=self.user_agent
            )

            # Test connection with timeout to prevent hanging
            connection_result = [False]
            connection_error = [None]

            def test_connection():
                try:
                    # Test connection - this will fail if credentials are wrong
                    if self.reddit is not None:
                        self.reddit.user.me()
                        connection_result[0] = True
                except Exception as e:
                    connection_error[0] = e

            # Start connection test in a separate thread with timeout
            test_thread = threading.Thread(target=test_connection)
            test_thread.daemon = True
            test_thread.start()

            # Wait for connection test with timeout
            test_thread.join(timeout=10)  # 10 second timeout

            if test_thread.is_alive():
                self.logger.warning("Reddit connection test timed out, API may be slow")
                # Don't fail - just warn and continue
            elif connection_error[0]:
                self.logger.error(f"Reddit API connection failed: {connection_error[0]}")
                self.reddit = None
            else:
                self.logger.info("Reddit API connection successful")

        except Exception as e:
            self.logger.error(f"Reddit API connection failed: {e}")
            self.reddit = None

    def collect_news(self, hours_back: int = 24, max_articles: int = 50) -> List[NewsArticle]:
        """Collect general trading posts from top Reddit communities"""
        if not self.reddit:
            self.logger.error("Reddit API not available")
            return []

        try:
            self.logger.info("Collecting general trading posts from Reddit")
            cutoff_time = datetime.now() - timedelta(hours=hours_back)
            all_posts = []

            # Get posts from top trading subreddits
            for subreddit_name in self.trading_subreddits[:4]:  # Limit for efficiency
                try:
                    posts = self._get_subreddit_posts(
                        subreddit_name,
                        limit=15,
                        cutoff_time=cutoff_time
                    )
                    all_posts.extend(posts)

                    if len(all_posts) >= max_articles:
                        break

                    self._rate_limit()

                except Exception as e:
                    self.logger.error(f"Error collecting from r/{subreddit_name}: {e}")
                    continue

            unique_posts = self._remove_duplicates(all_posts)
            self.logger.info(f"Collected {len(unique_posts)} Reddit posts")
            return unique_posts[:max_articles]

        except Exception as e:
            self.logger.error(f"Error in Reddit general collection: {e}")
            return []

    def get_symbol_news(self, symbol: str, hours_back: int = 24, max_articles: int = 20) -> List[NewsArticle]:
        """Get Reddit posts mentioning a specific symbol - KEY for decision engine"""
        if not self.reddit:
            self.logger.error("Reddit API not available")
            return []

        try:
            self.logger.info(f"Searching Reddit for {symbol} mentions")
            cutoff_time = datetime.now() - timedelta(hours=hours_back)
            symbol_posts = []

            # Strategy 1: Search recent posts from trading subreddits
            for subreddit_name in self.trading_subreddits[:5]:  # Top 5 subreddits
                try:
                    posts = self._get_subreddit_posts(
                        subreddit_name,
                        limit=30,
                        cutoff_time=cutoff_time
                    )

                    # Filter for symbol mentions
                    for post in posts:
                        if self._post_mentions_symbol(post, symbol):
                            # Ensure symbol is in symbols_mentioned
                            if symbol.upper() not in post.symbols_mentioned:
                                post.symbols_mentioned.append(symbol.upper())
                            symbol_posts.append(post)

                    self._rate_limit()

                    if len(symbol_posts) >= max_articles:
                        break

                except Exception as e:
                    self.logger.debug(f"Error searching r/{subreddit_name} for {symbol}: {e}")
                    continue

            # Strategy 2: Direct Reddit search (if we need more posts)
            if len(symbol_posts) < max_articles // 2:
                try:
                    search_posts = self._search_reddit_for_symbol(symbol, hours_back, max_articles // 2)
                    symbol_posts.extend(search_posts)
                except Exception as e:
                    self.logger.debug(f"Reddit search failed for {symbol}: {e}")

            unique_posts = self._remove_duplicates(symbol_posts)
            self.logger.info(f"Found {len(unique_posts)} Reddit posts mentioning {symbol}")
            return unique_posts[:max_articles]

        except Exception as e:
            self.logger.error(f"Error getting Reddit news for {symbol}: {e}")
            return []

    def get_watchlist_news(self, symbols: List[str], hours_back: int = 24,
                          max_articles_per_symbol: int = 10) -> Dict[str, List[NewsArticle]]:
        """Get Reddit posts for multiple symbols (watchlist) - KEY for decision engine"""
        if not self.reddit:
            self.logger.error("Reddit API not available")
            return {symbol: [] for symbol in symbols}

        watchlist_news = {symbol: [] for symbol in symbols}

        try:
            self.logger.info(f"Collecting Reddit news for {len(symbols)} watchlist symbols")
            cutoff_time = datetime.now() - timedelta(hours=hours_back)

            # Collect posts from trading subreddits once (efficient approach)
            all_recent_posts = []

            for subreddit_name in self.trading_subreddits[:6]:  # More subreddits for watchlist
                try:
                    posts = self._get_subreddit_posts(
                        subreddit_name,
                        limit=50,
                        cutoff_time=cutoff_time
                    )
                    all_recent_posts.extend(posts)
                    self._rate_limit()

                except Exception as e:
                    self.logger.debug(f"Error collecting from r/{subreddit_name}: {e}")
                    continue

            # Distribute posts to relevant symbols
            for symbol in symbols:
                try:
                    symbol_posts = [
                        post for post in all_recent_posts
                        if self._post_mentions_symbol(post, symbol)
                    ]

                    # Ensure symbol is added to symbols_mentioned
                    for post in symbol_posts:
                        if symbol.upper() not in post.symbols_mentioned:
                            post.symbols_mentioned.append(symbol.upper())

                    # Deduplicate and limit
                    unique_posts = self._remove_duplicates(symbol_posts)
                    watchlist_news[symbol] = unique_posts[:max_articles_per_symbol]

                except Exception as e:
                    self.logger.error(f"Error processing Reddit watchlist for {symbol}: {e}")
                    watchlist_news[symbol] = []

            total_posts = sum(len(posts) for posts in watchlist_news.values())
            self.logger.info(f"Reddit watchlist: {total_posts} total posts for {len(symbols)} symbols")

        except Exception as e:
            self.logger.error(f"Error in Reddit watchlist collection: {e}")

        return watchlist_news

    def test_connection(self) -> bool:
        """Test Reddit API connection"""
        try:
            if not self.reddit:
                return False

            # Simple test - access investing subreddit
            test_subreddit = self.reddit.subreddit('investing')
            list(test_subreddit.hot(limit=1))
            return True

        except Exception as e:
            self.logger.error(f"Reddit connection test failed: {e}")
            return False

    def _get_subreddit_posts(self, subreddit_name: str, limit: int = 50,
                           cutoff_time: Optional[datetime] = None) -> List[NewsArticle]:
        """Get posts from a specific subreddit with filtering"""
        posts = []

        try:
            subreddit = self.reddit.subreddit(subreddit_name)

            # Get hot posts (most engaging content)
            for post in subreddit.hot(limit=limit):
                try:
                    article = self._parse_reddit_post(post, subreddit_name)

                    if article:
                        # Filter by time if specified
                        if cutoff_time and article.published_date < cutoff_time:
                            continue
                        posts.append(article)

                except Exception as e:
                    self.logger.debug(f"Error parsing post from r/{subreddit_name}: {e}")
                    continue

        except Exception as e:
            self.logger.error(f"Error accessing r/{subreddit_name}: {e}")

        return posts

    def _search_reddit_for_symbol(self, symbol: str, hours_back: int, max_results: int) -> List[NewsArticle]:
        """Search Reddit for specific symbol mentions using search API"""
        posts = []

        try:
            # Search across trading subreddits for the symbol
            search_query = f'"{symbol}" OR "${symbol}"'  # Include $ format

            for subreddit_name in self.trading_subreddits[:3]:  # Limit search scope
                try:
                    subreddit = self.reddit.subreddit(subreddit_name)
                    search_results = subreddit.search(
                        search_query,
                        sort='new',
                        time_filter='day' if hours_back <= 24 else 'week',
                        limit=max_results // 3
                    )

                    cutoff_time = datetime.now() - timedelta(hours=hours_back)

                    for post in search_results:
                        try:
                            article = self._parse_reddit_post(post, subreddit_name)

                            if article and article.published_date >= cutoff_time:
                                posts.append(article)

                        except Exception as e:
                            self.logger.debug(f"Error parsing search result: {e}")
                            continue

                    self._rate_limit()

                except Exception as e:
                    self.logger.debug(f"Search failed in r/{subreddit_name}: {e}")
                    continue

        except Exception as e:
            self.logger.debug(f"Reddit search failed: {e}")

        return posts

    def _parse_reddit_post(self, post, subreddit_name: str) -> Optional[NewsArticle]:
        """Parse Reddit post into NewsArticle format with quality filtering"""
        try:
            title = post.title.strip()

            if not title or len(title) < 10:  # Minimum title length
                return None

            # Enhanced filtering for low-quality posts
            excluded_patterns = [
                'daily thread', 'daily discussion', 'weekly thread', 'what are your moves',
                'daily general discussion', 'weekend discussion', 'daily options',
                'gain/loss thread', 'yolo update', 'weekend discussion'
            ]

            if any(pattern in title.lower() for pattern in excluded_patterns):
                return None

            # Parse post creation time
            try:
                post_date = datetime.fromtimestamp(post.created_utc)
            except (ValueError, OSError, OverflowError):
                post_date = datetime.now()

            # Extract symbols from title and limited post text
            full_text = title
            if hasattr(post, 'selftext') and post.selftext and len(post.selftext) < 500:
                full_text += f" {post.selftext[:200]}"  # Limit for performance

            symbols = self.symbol_extractor.extract_symbols(full_text)

            # Skip posts with no symbols (unless from WSB where context matters)
            if not symbols and subreddit_name.lower() != 'wallstreetbets':
                return None

            # Create article with Reddit-specific structure
            article = NewsArticle(
                title=title,
                content=title,  # Use title as primary content
                source=f"Reddit-{subreddit_name}",
                url=f"https://reddit.com{post.permalink}",
                published_date=post_date,
                symbols_mentioned=symbols,
                author=str(post.author) if post.author else "Unknown"
            )

            # Add Reddit engagement metrics (crucial for sentiment weighting)
            article.additional_metadata = {
                'upvotes': getattr(post, 'ups', 0),
                'downvotes': getattr(post, 'downs', 0),
                'upvote_ratio': getattr(post, 'upvote_ratio', 0.5),
                'comments': getattr(post, 'num_comments', 0),
                'awards': getattr(post, 'total_awards_received', 0),
                'subreddit': subreddit_name,
                'post_flair': getattr(post, 'link_flair_text', ''),
                'is_self': getattr(post, 'is_self', True)
            }

            return article

        except Exception as e:
            self.logger.debug(f"Error parsing Reddit post: {e}")
            return None

    def _post_mentions_symbol(self, post: NewsArticle, symbol: str) -> bool:
        """Check if a post mentions a specific symbol"""
        symbol_upper = symbol.upper()
        symbol_lower = symbol.lower()

        return (
            symbol_upper in post.symbols_mentioned or
            symbol_lower in post.title.lower() or
            symbol_upper in post.title.upper() or
            f"${symbol_upper}" in post.title
        )

    def _remove_duplicates(self, posts: List[NewsArticle]) -> List[NewsArticle]:
        """Remove duplicate Reddit posts based on title similarity"""
        unique_posts = []
        seen_titles = set()

        for post in posts:
            # Create normalized title key
            title_key = post.title[:40].lower().replace(' ', '').replace('-', '')

            if title_key not in seen_titles:
                seen_titles.add(title_key)
                unique_posts.append(post)

        return unique_posts

    def _rate_limit(self):
        """Simple rate limiting to be respectful to Reddit API"""
        current_time = time.time()

        # Ensure at least 0.5 seconds between requests
        time_since_last = current_time - self.last_request_time
        if time_since_last < 0.5:
            time.sleep(0.5 - time_since_last)

        self.last_request_time = time.time()
        self.requests_made += 1

    def get_engagement_metrics(self, articles: List[NewsArticle]) -> Dict[str, float]:
        """Calculate engagement metrics for sentiment weighting"""
        if not articles:
            return {}

        total_upvotes = sum(article.additional_metadata.get('upvotes', 0) for article in articles)
        total_comments = sum(article.additional_metadata.get('comments', 0) for article in articles)
        avg_upvote_ratio = sum(article.additional_metadata.get('upvote_ratio', 0.5) for article in articles) / len(articles)
        total_awards = sum(article.additional_metadata.get('awards', 0) for article in articles)

        return {
            'total_upvotes': total_upvotes,
            'total_comments': total_comments,
            'total_awards': total_awards,
            'average_upvote_ratio': avg_upvote_ratio,
            'engagement_score': (total_upvotes + total_comments + total_awards * 5) / len(articles),
            'sentiment_strength': avg_upvote_ratio,  # Higher ratio = more positive
            'viral_factor': total_awards / len(articles)  # Awards indicate viral content
        }

    def get_subreddit_breakdown(self, articles: List[NewsArticle]) -> Dict[str, int]:
        """Get breakdown of articles by subreddit for analysis"""
        breakdown = {}

        for article in articles:
            subreddit = article.additional_metadata.get('subreddit', 'Unknown')
            breakdown[subreddit] = breakdown.get(subreddit, 0) + 1

        return breakdown
