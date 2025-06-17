"""
Reddit News Data Collector

Clean Reddit integration for social sentiment and retail trader buzz.
Key subreddits: wallstreetbets, investing, stocks, options.
"""

import os
import praw
import time
from datetime import datetime, timedelta
from typing import List, Optional, Dict
import logging

try:
    from .base import NewsDataSource, NewsArticle
    from .yahoo_news import SymbolExtractor  # Reuse symbol extractor
except ImportError:
    from base import NewsDataSource, NewsArticle
    from yahoo_news import SymbolExtractor


class RedditSymbolExtractor(SymbolExtractor):
    """Enhanced symbol extraction for Reddit posts with trading slang"""

    def __init__(self):
        super().__init__()

        # Add Reddit-specific trading patterns
        self.reddit_patterns = [
            # Common Reddit trading patterns
            r'\b(' + '|'.join(self.known_symbols) + r')\s+(?:calls?|puts?|options?)\b',
            r'(?:bought?|sold?|buying|selling)\s+(' + '|'.join(self.known_symbols) + r')\b',
            r'\b(' + '|'.join(self.known_symbols) + r')\s+(?:to the moon|moon|rocket|diamond hands)\b',
            r'\b(' + '|'.join(self.known_symbols) + r')\s+(?:yolo|hodl|squeeze)\b',
            r'(?:position|holding)\s+(' + '|'.join(self.known_symbols) + r')\b',
            r'\b(' + '|'.join(self.known_symbols) + r')\s+(?:gain|loss|profit)\b',
        ]

    def extract_symbols(self, text: str) -> List[str]:
        """Enhanced symbol extraction for Reddit posts"""
        # Get base symbols first
        symbols = set(super().extract_symbols(text))

        # Add Reddit-specific patterns
        import re
        for pattern in self.reddit_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            symbols.update(m.upper() for m in matches)

        return sorted(list(symbols))


class RedditNews(NewsDataSource):
    """Simplified Reddit news collector for trading sentiment"""

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

        # Initialize Reddit connection
        try:
            self.reddit = praw.Reddit(
                client_id=self.client_id,
                client_secret=self.client_secret,
                user_agent=self.user_agent
            )
            # Test connection
            self.reddit.user.me()
            self.logger.info("Reddit API connection successful")
        except Exception as e:
            self.logger.error(f"Reddit API connection failed: {e}")
            self.reddit = None

        # Trading subreddits for financial sentiment
        self.trading_subreddits = [
            'wallstreetbets',    # High-energy retail trading
            'investing',         # Long-term investment discussion
            'stocks',           # General stock discussion
            'StockMarket',      # Market analysis
            'SecurityAnalysis', # Fundamental analysis
            'ValueInvesting',   # Value investing
            'options',          # Options trading
            'pennystocks',      # Penny stock discussion
            # 'investing_discussion', # Alternative if main subs are restricted
        ]

    def collect_news(self, hours_back: int = 24, max_articles: int = 50) -> List[NewsArticle]:
        """Collect general trading posts from Reddit"""
        if not self.reddit:
            self.logger.error("Reddit API not available")
            return []

        cutoff_time = datetime.now() - timedelta(hours=hours_back)
        all_posts = []

        for subreddit_name in self.trading_subreddits[:4]:  # Limit to top 4 for general collection
            try:
                posts = self._get_subreddit_posts(subreddit_name, limit=20, cutoff_time=cutoff_time)
                all_posts.extend(posts)

                if len(all_posts) >= max_articles:
                    break

                time.sleep(0.5)  # Rate limiting

            except Exception as e:
                self.logger.error(f"Error collecting from r/{subreddit_name}: {e}")
                continue

        unique_posts = self._remove_duplicates(all_posts)
        return unique_posts[:max_articles]

    def get_symbol_news(self, symbol: str, hours_back: int = 24, max_articles: int = 20) -> List[NewsArticle]:
        """Get Reddit posts specifically mentioning a symbol - KEY for decision engine"""
        if not self.reddit:
            self.logger.error("Reddit API not available")
            return []

        try:
            self.logger.info(f"Searching Reddit for {symbol} mentions")

            cutoff_time = datetime.now() - timedelta(hours=hours_back)
            symbol_posts = []

            # Search across trading subreddits for symbol mentions
            for subreddit_name in self.trading_subreddits:
                try:
                    # Get recent posts from subreddit
                    posts = self._get_subreddit_posts(subreddit_name, limit=50, cutoff_time=cutoff_time)

                    # Filter for symbol mentions
                    for post in posts:
                        if (symbol.upper() in post.symbols_mentioned or
                            symbol.lower() in post.title.lower() or
                            symbol.upper() in post.title.upper()):

                            # Ensure symbol is in symbols_mentioned
                            if symbol.upper() not in post.symbols_mentioned:
                                post.symbols_mentioned.append(symbol.upper())

                            symbol_posts.append(post)

                    # Rate limiting
                    time.sleep(0.3)

                    if len(symbol_posts) >= max_articles:
                        break

                except Exception as e:
                    self.logger.debug(f"Error searching r/{subreddit_name} for {symbol}: {e}")
                    continue

            # Also try Reddit search API for symbol
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

        watchlist_news = {}
        cutoff_time = datetime.now() - timedelta(hours=hours_back)

        try:
            self.logger.info(f"Collecting Reddit news for {len(symbols)} watchlist symbols")

            # Collect all recent posts from trading subreddits once
            all_recent_posts = []

            for subreddit_name in self.trading_subreddits[:6]:  # More subreddits for watchlist
                try:
                    posts = self._get_subreddit_posts(subreddit_name, limit=100, cutoff_time=cutoff_time)
                    all_recent_posts.extend(posts)
                    time.sleep(0.5)  # Rate limiting
                except Exception as e:
                    self.logger.debug(f"Error collecting from r/{subreddit_name}: {e}")
                    continue

            # Filter posts for each symbol in watchlist
            for symbol in symbols:
                try:
                    symbol_posts = []

                    # Filter from all recent posts
                    for post in all_recent_posts:
                        if (symbol.upper() in post.symbols_mentioned or
                            symbol.lower() in post.title.lower() or
                            symbol.upper() in post.title.upper()):

                            # Ensure symbol is in symbols_mentioned
                            if symbol.upper() not in post.symbols_mentioned:
                                post.symbols_mentioned.append(symbol.upper())

                            symbol_posts.append(post)

                    # Deduplicate and limit
                    unique_posts = self._remove_duplicates(symbol_posts)
                    watchlist_news[symbol] = unique_posts[:max_articles_per_symbol]

                except Exception as e:
                    self.logger.error(f"Error processing Reddit watchlist for {symbol}: {e}")
                    watchlist_news[symbol] = []

            total_posts = sum(len(posts) for posts in watchlist_news.values())
            self.logger.info(f"Reddit watchlist complete: {total_posts} total posts for {len(symbols)} symbols")

        except Exception as e:
            self.logger.error(f"Error in Reddit watchlist collection: {e}")
            watchlist_news = {symbol: [] for symbol in symbols}

        return watchlist_news

    def test_connection(self) -> bool:
        """Test Reddit API connection"""
        try:
            if not self.reddit:
                return False

            # Try to access a simple subreddit
            test_subreddit = self.reddit.subreddit('investing')
            # Get just one post to test connection
            list(test_subreddit.hot(limit=1))
            return True

        except Exception as e:
            self.logger.error(f"Reddit connection test failed: {e}")
            return False

    def _get_subreddit_posts(self, subreddit_name: str, limit: int = 50,
                           cutoff_time: Optional[datetime] = None) -> List[NewsArticle]:
        """Get posts from a specific subreddit"""
        posts = []

        try:
            subreddit = self.reddit.subreddit(subreddit_name)

            # Get hot posts (most active/relevant)
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
        """Search Reddit for specific symbol mentions"""
        posts = []

        try:
            # Search across all of Reddit for the symbol
            search_results = self.reddit.subreddit('all').search(
                f'"{symbol}"',
                sort='new',
                time_filter='day' if hours_back <= 24 else 'week',
                limit=max_results
            )

            cutoff_time = datetime.now() - timedelta(hours=hours_back)

            for post in search_results:
                try:
                    # Only include posts from trading-related subreddits
                    if post.subreddit.display_name.lower() in [sub.lower() for sub in self.trading_subreddits]:
                        article = self._parse_reddit_post(post, post.subreddit.display_name)

                        if article and article.published_date >= cutoff_time:
                            posts.append(article)

                except Exception as e:
                    self.logger.debug(f"Error parsing search result: {e}")
                    continue

        except Exception as e:
            self.logger.debug(f"Reddit search failed: {e}")

        return posts

    def _parse_reddit_post(self, post, subreddit_name: str) -> Optional[NewsArticle]:
        """Parse Reddit post into NewsArticle format"""
        try:
            title = post.title.strip()

            if not title or len(title) < 5:
                return None

            # Filter out daily/weekly threads and low-quality posts
            excluded_keywords = [
                'daily thread', 'daily discussion', 'weekly thread', 'what are your moves',
                'daily general discussion', 'weekend discussion'
            ]

            if any(keyword in title.lower() for keyword in excluded_keywords):
                return None

            # Parse post date
            try:
                post_date = datetime.fromtimestamp(post.created_utc)
            except (ValueError, OSError, OverflowError):
                post_date = datetime.now()

            # Extract symbols from title and body
            full_text = title
            if hasattr(post, 'selftext') and post.selftext:
                full_text += f" {post.selftext[:200]}"  # Limit text for processing

            symbols = self.symbol_extractor.extract_symbols(full_text)

            # Create article with Reddit-specific data
            article = NewsArticle(
                title=title,
                content=title,  # Use title as content for consistency
                source=f"Reddit-{subreddit_name}",
                url=f"https://reddit.com{post.permalink}",
                published_date=post_date,
                symbols_mentioned=symbols,
                author=str(post.author) if post.author else None
            )

            # Add Reddit engagement metrics (stored in additional_metadata for now)
            article.additional_metadata = {
                'upvotes': getattr(post, 'ups', 0),
                'downvotes': getattr(post, 'downs', 0),
                'upvote_ratio': getattr(post, 'upvote_ratio', 0.5),
                'comments': getattr(post, 'num_comments', 0),
                'awards': getattr(post, 'total_awards_received', 0),
                'subreddit': subreddit_name
            }

            return article

        except Exception as e:
            self.logger.debug(f"Error parsing Reddit post: {e}")
            return None

    def _remove_duplicates(self, posts: List[NewsArticle]) -> List[NewsArticle]:
        """Remove duplicate Reddit posts"""
        unique_posts = []
        seen_titles = set()

        for post in posts:
            title_key = post.title[:30].lower().replace(' ', '')
            if title_key not in seen_titles:
                seen_titles.add(title_key)
                unique_posts.append(post)

        return unique_posts

    def get_engagement_metrics(self, articles: List[NewsArticle]) -> Dict[str, float]:
        """Get Reddit engagement metrics for sentiment weighting"""
        if not articles:
            return {}

        total_upvotes = sum(article.additional_metadata.get('upvotes', 0) for article in articles)
        total_comments = sum(article.additional_metadata.get('comments', 0) for article in articles)
        avg_upvote_ratio = sum(article.additional_metadata.get('upvote_ratio', 0.5) for article in articles) / len(articles)

        return {
            'total_upvotes': total_upvotes,
            'total_comments': total_comments,
            'average_upvote_ratio': avg_upvote_ratio,
            'engagement_score': (total_upvotes + total_comments) / len(articles),  # Per article engagement
            'sentiment_strength': avg_upvote_ratio  # Higher ratio = more positive sentiment
        }
