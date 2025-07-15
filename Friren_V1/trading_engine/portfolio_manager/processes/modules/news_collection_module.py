"""
News Collection Module for Enhanced News Pipeline
================================================

Handles news collection, terminal bridge communication, and basic news processing
for the enhanced news pipeline process. This module provides a clean interface
for collecting news articles from multiple sources.

Extracted from enhanced_news_pipeline_process.py for better maintainability.
"""

import sys
import os
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
import json
from dataclasses import dataclass

# Import news collector components
from Friren_V1.trading_engine.data.news_collector import EnhancedNewsCollector, ProcessedNewsData
from Friren_V1.trading_engine.data.news.base import NewsArticle

# Import OutputCoordinator for standardized news collection output
from Friren_V1.trading_engine.output.output_coordinator import OutputCoordinator


class Colors:
    """Console color constants for enhanced output"""
    YELLOW = '\033[93m'
    RED = '\033[91m'
    GREEN = '\033[92m'
    BLUE = '\033[94m'
    WHITE = '\033[97m'
    BOLD = '\033[1m'
    RESET = '\033[0m'


def send_colored_business_output(process_id: str, message: str, output_type: str) -> bool:
    """
    Send colored business output with multiple fallback methods
    
    Args:
        process_id: Process identifier
        message: Message to output
        output_type: Type of output (news, finbert, xgboost, etc.)
        
    Returns:
        True if output was successful via any method
    """
    success = False
    
    # Method 1: Try direct main terminal bridge import with proper path resolution
    try:
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))))
        if project_root not in sys.path:
            sys.path.append(project_root)
        from main_terminal_bridge import send_colored_business_output as bridge_output
        bridge_output(process_id, message, output_type)
        success = True
    except ImportError as e:
        logging.getLogger(__name__).debug(f"Main bridge import failed: {e}")
    except Exception as e:
        logging.getLogger(__name__).warning(f"Main bridge communication failed: {e}")
    
    # Method 2: Use Redis direct communication if bridge failed
    if not success:
        try:
            from Friren_V1.multiprocess_infrastructure.trading_redis_manager import get_trading_redis_manager
            
            # Create message data for main terminal
            message_data = {
                'process_id': process_id,
                'output': message,
                'color_type': output_type,
                'timestamp': datetime.now().isoformat()
            }

            # Get Redis manager and send message
            redis_manager = get_trading_redis_manager()
            if redis_manager:
                redis_client = getattr(redis_manager, 'redis_client', None)
                if redis_client:
                    redis_client.rpush("terminal_output", json.dumps(message_data))
                    success = True
        except Exception as e:
            logging.getLogger(__name__).debug(f"Redis communication failed: {e}")
    
    # Method 3: Enhanced fallback with colored output based on type
    if not success:
        if output_type == "news":
            print(f"{Colors.BLUE}[NEWS] {message}{Colors.RESET}")
        elif output_type == "finbert":
            print(f"{Colors.YELLOW}[FINBERT] {message}{Colors.RESET}")
        elif output_type == "xgboost":
            print(f"{Colors.GREEN}[XGBOOST] {message}{Colors.RESET}")
        elif output_type == "recommendation":
            print(f"{Colors.WHITE}[RECOMMENDATION] {message}{Colors.RESET}")
        else:
            print(f"{Colors.BLUE}[{process_id.upper()}] {message}{Colors.RESET}")
    
    return success


@dataclass
class NewsCollectionConfig:
    """Configuration for news collection"""
    max_articles_per_symbol: int = 15
    hours_back: int = 24
    quality_threshold: float = 0.6
    enable_caching: bool = True
    cache_ttl_minutes: int = 30


@dataclass
class CollectionMetrics:
    """News collection metrics"""
    articles_collected: int = 0
    symbols_processed: int = 0
    collection_time_ms: float = 0.0
    cache_hits: int = 0
    cache_misses: int = 0
    errors: int = 0
    timestamp: datetime = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()


class NewsCollectionModule:
    """
    News Collection Module
    
    Handles news collection from multiple sources with intelligent filtering,
    caching, and quality control. Provides standardized news data for
    downstream FinBERT and XGBoost processing.
    """
    
    def __init__(self, config: Optional[NewsCollectionConfig] = None, 
                 process_id: str = "news_collection", 
                 redis_client=None):
        """
        Initialize news collection module
        
        Args:
            config: News collection configuration
            process_id: Process identifier for output
            redis_client: Redis client for OutputCoordinator
        """
        self.config = config or NewsCollectionConfig()
        self.process_id = process_id
        self.logger = logging.getLogger(__name__)
        
        # Core components
        self.news_collector: Optional[EnhancedNewsCollector] = None
        
        # Initialize OutputCoordinator for standardized news collection output
        try:
            self.output_coordinator = OutputCoordinator(
                redis_client=redis_client,
                enable_terminal=True,
                enable_logging=True
            )
            self.logger.info("OutputCoordinator initialized for news collection output")
        except Exception as e:
            self.logger.warning(f"OutputCoordinator initialization failed: {e}")
            self.output_coordinator = None
        
        # State tracking
        self.last_collection_time: Optional[datetime] = None
        self.collection_cache: Dict[str, List[NewsArticle]] = {}
        self.metrics = CollectionMetrics()
        
        # Error tracking
        self.error_count = 0
        self.last_error_time: Optional[datetime] = None
        
    def initialize(self) -> bool:
        """
        Initialize news collection components
        
        Returns:
            True if initialization successful
        """
        try:
            self.logger.info("Initializing news collection module...")
            
            # Initialize enhanced news collector
            self.news_collector = EnhancedNewsCollector()
            if hasattr(self.news_collector, 'initialize'):
                self.news_collector.initialize()
            
            self.logger.info("News collection module initialized successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to initialize news collection module: {e}")
            self.error_count += 1
            self.last_error_time = datetime.now()
            return False
    
    def collect_news_for_symbols(self, symbols: List[str]) -> Dict[str, List[NewsArticle]]:
        """
        Collect news for specified symbols
        
        Args:
            symbols: List of stock symbols to collect news for
            
        Returns:
            Dictionary mapping symbols to collected news articles
        """
        start_time = time.time()
        collected_news = {}
        
        try:
            self.logger.info(f"Starting news collection for {len(symbols)} symbols")
            send_colored_business_output(
                self.process_id, 
                f"News collection starting for {len(symbols)} symbols...",
                "news"
            )
            
            # Process each symbol
            for symbol in symbols:
                try:
                    # Check cache first if enabled
                    if self.config.enable_caching and self._is_cache_valid(symbol):
                        articles = self.collection_cache[symbol]
                        self.metrics.cache_hits += 1
                        self.logger.debug(f"Using cached news for {symbol}: {len(articles)} articles")
                    else:
                        # Collect fresh news
                        articles = self._collect_symbol_news(symbol)
                        self.metrics.cache_misses += 1
                        
                        # Update cache if enabled
                        if self.config.enable_caching:
                            self.collection_cache[symbol] = articles
                    
                    collected_news[symbol] = articles
                    
                    # Output collection result with target format: [NEWS COLLECTOR] AAPL: 'Article Title' from Source - collected HH:MM:SS
                    if articles:
                        for article in articles[:2]:  # Show first 2 articles for each symbol
                            # Extract real source from article (NO HARDCODING)
                            source = getattr(article, 'source', None)
                            if not source:
                                # Skip article if no real source available - NO FALLBACK HARDCODING
                                continue
                            
                            # Use real article collection timestamp or current time
                            article_timestamp = getattr(article, 'timestamp', None) or getattr(article, 'published_at', None)
                            if article_timestamp:
                                if isinstance(article_timestamp, datetime):
                                    timestamp = article_timestamp.strftime('%H:%M:%S')
                                else:
                                    timestamp = datetime.now().strftime('%H:%M:%S')
                            else:
                                timestamp = datetime.now().strftime('%H:%M:%S')
                            
                            # Use real article title (NO TRUNCATION HARDCODING)
                            title = article.title
                            
                            # Use OutputCoordinator for target format with REAL DATA ONLY
                            if self.output_coordinator:
                                self.output_coordinator.output_news_collection(
                                    symbol=symbol,
                                    title=title,
                                    source=source,
                                    timestamp=timestamp
                                )
                            else:
                                # Fallback with exact target format using REAL DATA ONLY
                                print(f"[NEWS COLLECTOR] {symbol}: '{title}' from {source} - collected {timestamp}")
                    # NO OUTPUT if no articles found - system should work with real data or fail gracefully
                    
                except Exception as e:
                    self.logger.error(f"Error collecting news for {symbol}: {e}")
                    self.metrics.errors += 1
                    collected_news[symbol] = []
            
            # Update metrics
            collection_time = (time.time() - start_time) * 1000
            self.metrics.collection_time_ms = collection_time
            self.metrics.articles_collected = sum(len(articles) for articles in collected_news.values())
            self.metrics.symbols_processed = len(symbols)
            self.last_collection_time = datetime.now()
            
            self.logger.info(f"News collection completed: {self.metrics.articles_collected} articles in {collection_time:.1f}ms")
            
            return collected_news
            
        except Exception as e:
            self.logger.error(f"News collection failed: {e}")
            self.error_count += 1
            self.last_error_time = datetime.now()
            return {}
    
    def _collect_symbol_news(self, symbol: str) -> List[NewsArticle]:
        """
        Collect news for a specific symbol
        
        Args:
            symbol: Stock symbol
            
        Returns:
            List of news articles for the symbol
        """
        try:
            if not self.news_collector:
                self.logger.error("News collector not initialized")
                return []
            
            # Calculate time window
            end_time = datetime.now()
            start_time = end_time - timedelta(hours=self.config.hours_back)
            
            # Collect news using the enhanced collector
            articles = self.news_collector.collect_news_for_symbol(
                symbol=symbol,
                max_articles=self.config.max_articles_per_symbol,
                start_time=start_time,
                end_time=end_time
            )
            
            # Filter by quality threshold
            if self.config.quality_threshold > 0:
                filtered_articles = []
                for article in articles:
                    quality_score = self._calculate_article_quality(article)
                    if quality_score >= self.config.quality_threshold:
                        filtered_articles.append(article)
                articles = filtered_articles
            
            self.logger.debug(f"Collected {len(articles)} quality articles for {symbol}")
            return articles
            
        except Exception as e:
            self.logger.error(f"Error collecting news for {symbol}: {e}")
            return []
    
    def _calculate_article_quality(self, article: NewsArticle) -> float:
        """
        Calculate quality score for an article
        
        Args:
            article: News article to evaluate
            
        Returns:
            Quality score between 0.0 and 1.0
        """
        try:
            quality_score = 0.5  # Base score
            
            # Title quality (30% weight)
            if hasattr(article, 'title') and article.title:
                if len(article.title) >= 20:
                    quality_score += 0.15
                if len(article.title.split()) >= 5:
                    quality_score += 0.15
            
            # Content quality (40% weight)
            if hasattr(article, 'content') and article.content:
                if len(article.content) >= 200:
                    quality_score += 0.2
                if len(article.content.split()) >= 50:
                    quality_score += 0.2
            
            # Source quality (20% weight)
            if hasattr(article, 'source') and article.source:
                trusted_sources = ['reuters', 'bloomberg', 'wsj', 'marketwatch', 'cnbc']
                if any(source in article.source.lower() for source in trusted_sources):
                    quality_score += 0.2
            
            # Recency (10% weight)
            if hasattr(article, 'published_date') and article.published_date:
                hours_old = (datetime.now() - article.published_date).total_seconds() / 3600
                if hours_old <= 6:
                    quality_score += 0.1
                elif hours_old <= 24:
                    quality_score += 0.05
            
            return min(1.0, quality_score)
            
        except Exception as e:
            self.logger.debug(f"Error calculating article quality: {e}")
            return 0.5
    
    def _is_cache_valid(self, symbol: str) -> bool:
        """
        Check if cached news for symbol is still valid
        
        Args:
            symbol: Stock symbol
            
        Returns:
            True if cache is valid and fresh
        """
        try:
            if symbol not in self.collection_cache:
                return False
            
            if not self.last_collection_time:
                return False
            
            cache_age_minutes = (datetime.now() - self.last_collection_time).total_seconds() / 60
            return cache_age_minutes < self.config.cache_ttl_minutes
            
        except Exception:
            return False
    
    def get_metrics(self) -> CollectionMetrics:
        """
        Get current collection metrics
        
        Returns:
            Current metrics snapshot
        """
        return self.metrics
    
    def clear_cache(self):
        """Clear the news collection cache"""
        self.collection_cache.clear()
        self.logger.info("News collection cache cleared")
    
    def cleanup(self):
        """Cleanup module resources"""
        try:
            self.clear_cache()
            if self.news_collector and hasattr(self.news_collector, 'cleanup'):
                self.news_collector.cleanup()
            self.logger.info("News collection module cleanup completed")
        except Exception as e:
            self.logger.error(f"Error during news collection cleanup: {e}")