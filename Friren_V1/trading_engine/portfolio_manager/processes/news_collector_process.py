"""
processes/enhanced_news_collector_process.py

Enhanced News Collector Process - Optimized integration with new FinBERT architecture.
Removes redundant sentiment processing and focuses on news collection and caching.
"""

import time
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import logging
import pickle
import os
import sys
from dataclasses import asdict

# Add the root directory to path to ensure imports work
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Add project root for colorized output
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
if project_root not in sys.path:
    sys.path.append(project_root)

# Import colorized output functions
try:
    from colorized_news_output import (
        show_news_collection, show_news_processing, 
        show_trading_signal, show_pipeline_completion
    )
    COLORIZED_OUTPUT_AVAILABLE = True
except ImportError:
    COLORIZED_OUTPUT_AVAILABLE = False
    def show_news_collection(symbol, count): pass
    def show_news_processing(symbol, count, sentiment=None): pass  
    def show_trading_signal(symbol, action, confidence, reasoning=""): pass
    def show_pipeline_completion(symbol, total, time_taken, signals): pass

# Import new color system for orange news collection messages
try:
    from terminal_color_system import print_news_collection, print_error, print_warning, print_success, create_colored_logger
    NEW_COLOR_SYSTEM_AVAILABLE = True
except ImportError:
    NEW_COLOR_SYSTEM_AVAILABLE = False

# Import existing multiprocess infrastructure
from Friren_V1.multiprocess_infrastructure.redis_base_process import RedisBaseProcess, ProcessState
from Friren_V1.multiprocess_infrastructure.trading_redis_manager import (
    get_trading_redis_manager, create_process_message, MessagePriority, ProcessMessage
)

# Import the Enhanced News Collector utility we built
from Friren_V1.trading_engine.data.news_collector import EnhancedNewsCollector, ProcessedNewsData
from Friren_V1.trading_engine.data.news.base import NewsArticle

# PRODUCTION: Using Redis ProcessMessage system - no legacy queue messages needed

class NewsCache:
    """Memory-aware cache for news data with TTL and size limits"""

    def __init__(self, max_memory_mb: int = 100, default_ttl_minutes: int = 30):
        self.max_memory_bytes = max_memory_mb * 1024 * 1024
        self.default_ttl = default_ttl_minutes * 60  # Convert to seconds

        self.cache = {}  # {symbol: {data: ProcessedNewsData, timestamp: float, size: int}}
        self.total_memory_usage = 0
        self._lock = threading.Lock()

        self.logger = logging.getLogger(f"{__name__}.NewsCache")

    def get(self, symbol: str) -> Optional[ProcessedNewsData]:
        """Get cached news data if still valid"""
        with self._lock:
            if symbol not in self.cache:
                return None

            entry = self.cache[symbol]
            age = time.time() - entry['timestamp']

            if age > self.default_ttl:
                # Expired - remove from cache
                self._remove_entry(symbol)
                return None

            return entry['data']

    def set(self, symbol: str, data: ProcessedNewsData):
        """Cache news data with memory management"""
        with self._lock:
            # Estimate size of the data
            estimated_size = self._estimate_size(data)

            # Clean old data if needed
            while (self.total_memory_usage + estimated_size > self.max_memory_bytes and
                   len(self.cache) > 0):
                self._remove_oldest_entry()

            # Remove existing entry if present
            if symbol in self.cache:
                self._remove_entry(symbol)

            # Add new entry
            self.cache[symbol] = {
                'data': data,
                'timestamp': time.time(),
                'size': estimated_size
            }
            self.total_memory_usage += estimated_size

            self.logger.debug(f"Cached news for {symbol} ({estimated_size} bytes)")

    def _estimate_size(self, data: ProcessedNewsData) -> int:
        """Estimate memory usage of ProcessedNewsData"""
        # Conservative estimate based on typical content
        base_size = 1000  # Base object overhead

        # Articles content
        articles_size = len(data.key_articles) * 500  # ~500 bytes per article

        # String fields
        string_size = len(data.symbol) + len(' '.join(data.market_events)) + len(' '.join(data.sources_used))

        return base_size + articles_size + string_size

    def _remove_entry(self, symbol: str):
        """Remove entry and update memory usage"""
        if symbol in self.cache:
            self.total_memory_usage -= self.cache[symbol]['size']
            del self.cache[symbol]

    def _remove_oldest_entry(self):
        """Remove the oldest entry to free memory"""
        if not self.cache:
            return

        oldest_symbol = min(self.cache.keys(), key=lambda s: self.cache[s]['timestamp'])
        self._remove_entry(oldest_symbol)

    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        with self._lock:
            return {
                'entries': len(self.cache),
                'memory_usage_mb': self.total_memory_usage / (1024 * 1024),
                'memory_limit_mb': self.max_memory_bytes / (1024 * 1024),
                'utilization': self.total_memory_usage / self.max_memory_bytes,
                'symbols_cached': list(self.cache.keys())
            }

    def clear(self):
        """Clear all cached data"""
        with self._lock:
            self.cache.clear()
            self.total_memory_usage = 0


class EnhancedNewsCollectorProcess(RedisBaseProcess):
    """
    Enhanced News Collector Process - Optimized for new FinBERT architecture

    Core responsibilities:
    - Multi-source news collection and basic processing
    - Memory-aware caching for t3.micro constraints
    - Feeding raw articles to dedicated FinBERT process
    - Shared state updates with news metadata
    - Regime detection triggers and watchlist precaching
    """

    def __init__(self, process_id: str = "enhanced_news_collector",
                 watchlist_symbols: Optional[List[str]] = None,
                 cache_memory_mb: int = 80,  # Reduced to leave room for FinBERT
                 precache_interval_minutes: int = 20):

        super().__init__(process_id)

        # Configuration
        # Load symbols dynamically from database or use provided watchlist
        self.watchlist_symbols = watchlist_symbols or []
        
        # Validate symbol list - try to load from database if empty
        if not self.watchlist_symbols:
            try:
                # Import from main module where load_dynamic_watchlist is actually defined
                import sys
                import os
                
                # Add project root to path to import from main
                project_root = os.path.join(os.path.dirname(__file__), '..', '..', '..', '..')
                project_root = os.path.abspath(project_root)
                if project_root not in sys.path:
                    sys.path.insert(0, project_root)
                
                # Import load_dynamic_watchlist from main.py
                import main
                
                # Create a minimal logger for the function call
                import logging
                temp_logger = logging.getLogger(__name__)
                dynamic_symbols = main.load_dynamic_watchlist(temp_logger)
                
                if dynamic_symbols:
                    self.watchlist_symbols = dynamic_symbols
                    print(f"News Collector: Loaded {len(self.watchlist_symbols)} symbols from database")
                else:
                    raise ValueError("No symbols available from database")
            except Exception as e:
                print(f"News Collector: Failed to load symbols from database: {e}")
                raise ValueError(f"News Collector requires symbols to operate: {e}")
        self.precache_interval = precache_interval_minutes * 60  # Convert to seconds

        # Components (initialized in _initialize)
        self.news_collector = None
        self.cache = NewsCache(max_memory_mb=cache_memory_mb)

        # Timing control
        self.last_precache_time = 0
        self.last_regime_check = 0

        # Statistics
        self.stats = {
            'news_requests_processed': 0,
            'regime_triggers_processed': 0,
            'cache_hits': 0,
            'cache_misses': 0,
            'articles_sent_to_finbert': 0,
            'total_articles_collected': 0,
            'watchlist_precaches': 0
        }

        self.logger.info(f"Enhanced News Collector Process configured with {len(self.watchlist_symbols)} watchlist symbols")

    def _initialize(self):
        """Initialize the Enhanced News Collector utility and components"""
        try:
            self.logger.info("Initializing Enhanced News Collector Process...")

            # Initialize the news collector utility
            self.news_collector = EnhancedNewsCollector()

            # Get available sources for logging
            available_sources = self.news_collector.get_available_sources()
            self.logger.info(f"News sources available: {available_sources}")

            self.state = ProcessState.RUNNING
            self.logger.info("Enhanced News Collector Process initialization complete")

        except Exception as e:
            self.logger.error(f"Failed to initialize Enhanced News Collector Process: {e}")
            self.state = ProcessState.ERROR
            raise

    def _execute(self):
        """Execute main process logic (required by RedisBaseProcess)"""
        self._process_cycle()

    def _process_cycle(self):
        """Main processing cycle - optimized for FinBERT separation"""
        try:
            # Check for high-priority regime change alerts
            regime_message = self._check_regime_alerts()
            if regime_message:
                self._handle_regime_change(regime_message)

            # Check for regular news requests
            news_request = self._check_news_requests()
            if news_request:
                self._handle_news_request(news_request)

            # Check if it's time for precaching
            if self._should_precache():
                self._precache_watchlist_news()

            # Brief sleep to prevent busy waiting
            time.sleep(5)

        except Exception as e:
            self.logger.error(f"Error in processing cycle: {e}")
            self.error_count += 1
            time.sleep(10)  # Longer sleep on error

    def _check_regime_alerts(self) -> Optional[ProcessMessage]:
        """Check for regime change alerts (highest priority)"""
        try:
            # Check for regime change messages with timeout
            message = self._get_priority_message("REGIME_CHANGE", timeout=0.1)
            return message
        except:
            return None

    def _check_news_requests(self) -> Optional[ProcessMessage]:
        """Check for NEWS_REQUEST messages"""
        try:
            if not self.priority_queue:
                return None

            # Check for any relevant messages
            message = self.priority_queue.get(timeout=0.1)
            if message:
                self.logger.info(f"Received message: {message.message_type}")
                print(f"Received message: {message.message_type}")

                # Handle different message types
                if message.message_type == "NEWS_REQUEST":
                    return message
                elif message.message_type == "REGIME_CHANGE":
                    # Handle regime change
                    self._handle_regime_change(message)
                    return None
                elif message.message_type == "SENTIMENT_UPDATE":
                    # Handle sentiment updates - could trigger news collection
                    self.logger.info(f"Received sentiment update: {message.data}")
                    print(f"Received sentiment update: {message.data}")
                    # Could trigger news collection for symbols with sentiment changes
                    return None
                else:
                    # Put back other message types for other processes via Redis
                    if self.redis_manager:
                        self.redis_manager.send_message(message)
                    return None

            return None

        except:
            return None

    def _handle_regime_change(self, message: ProcessMessage):
        """Handle regime change alert - immediate news processing"""
        try:
            self.logger.info("Processing regime change alert")

            data = message.data
            affected_symbols = data.get('symbols', self.watchlist_symbols)
            regime_type = data.get('new_regime', 'UNKNOWN')

            self.logger.info(f"Regime change to {regime_type}, updating news for {len(affected_symbols)} symbols")

            # Collect fresh news for affected symbols (skip cache)
            for symbol in affected_symbols:
                try:
                    news_data = self.news_collector.collect_symbol_news(
                        symbol=symbol,
                        hours_back=4,  # More recent for regime changes
                        max_articles_per_source=10
                    )

                    # Cache the fresh data
                    self.cache.set(symbol, news_data)

                    # Update shared state with news metadata only
                    self._update_shared_state_metadata(symbol, news_data, priority=True)

                    # Send raw articles to FinBERT for sentiment analysis
                    self._send_articles_to_finbert(symbol, news_data.key_articles, priority=True)

                    self.stats['total_articles_collected'] += news_data.news_volume

                except Exception as e:
                    self.logger.error(f"Error processing regime change for {symbol}: {e}")
                    continue

            self.stats['regime_triggers_processed'] += 1
            self.logger.info(f"Regime change processing complete for {len(affected_symbols)} symbols")

        except Exception as e:
            self.logger.error(f"Error handling regime change: {e}")

    def _handle_news_request(self, message: ProcessMessage):
        """Handle specific news request from decision engine or strategy analyzer"""
        try:
            data = message.data
            symbol = data.get('symbol')
            force_refresh = data.get('force_refresh', False)

            if not symbol:
                self.logger.warning("Received news request without symbol")
                return

            self.logger.debug(f"Processing news request for {symbol}")

            # Check cache first (unless forced refresh)
            if not force_refresh:
                cached_data = self.cache.get(symbol)
                if cached_data:
                    self.logger.debug(f"Cache hit for {symbol}")
                    self._update_shared_state_metadata(symbol, cached_data)
                    # Send cached articles to FinBERT if they haven't been processed recently
                    if self._should_reprocess_for_finbert(symbol, cached_data):
                        self._send_articles_to_finbert(symbol, cached_data.key_articles)
                    self.stats['cache_hits'] += 1
                    self.stats['news_requests_processed'] += 1
                    return

            # Cache miss - collect fresh news
            self.logger.debug(f"Cache miss for {symbol}, collecting fresh news")
            self.stats['cache_misses'] += 1

            news_data = self.news_collector.collect_symbol_news(
                symbol=symbol,
                hours_back=6,
                max_articles_per_source=15
            )

            # Cache the data
            self.cache.set(symbol, news_data)

            # Update shared state with metadata
            self._update_shared_state_metadata(symbol, news_data)

            # Send raw articles to FinBERT for sentiment processing
            if news_data.key_articles:
                self._send_articles_to_finbert(symbol, news_data.key_articles)

            self.stats['news_requests_processed'] += 1
            self.stats['total_articles_collected'] += news_data.news_volume

        except Exception as e:
            self.logger.error(f"Error handling news request: {e}")

    def _should_precache(self) -> bool:
        """Check if we should precache news data - MODIFIED FOR IMMEDIATE TESTING"""
        current_time = time.time()

        # FORCE IMMEDIATE EXECUTION: Always return True for first few runs
        if self.stats['watchlist_precaches'] < 3:  # Force first 3 cycles
            self.logger.info(f"FORCING NEWS COLLECTION: Run #{self.stats['watchlist_precaches'] + 1}")
            return True

        # After forced runs, check normal timing
        if not self._is_market_hours():
            # After hours, precache less frequently
            return current_time - self.last_precache_time > (self.precache_interval * 2)

        # During market hours, precache more frequently
        return current_time - self.last_precache_time > self.precache_interval

    def _precache_watchlist_news(self):
        """Proactively collect and cache news for watchlist symbols with detailed logging"""
        try:
            self.logger.info(f"=== NEWS COLLECTION CYCLE STARTING ===")
            self.logger.info(f"TARGET SYMBOLS: {self.watchlist_symbols}")
            self.logger.info(f"COLLECTION PARAMETERS: 6 hours back, max 12 articles per symbol")

            # Use batch collection for efficiency
            watchlist_data = self.news_collector.collect_watchlist_news(
                symbols=self.watchlist_symbols,
                hours_back=6,
                max_articles_per_symbol=12
            )

            # Cache and update shared state for each symbol
            total_articles = 0
            articles_for_finbert = []

            for symbol, news_data in watchlist_data.items():
                try:
                    self.logger.info(f"--- NEWS RESULTS FOR {symbol} ---")

                    if news_data and news_data.key_articles:
                        article_count = len(news_data.key_articles)
                        total_articles += news_data.news_volume

                        # Show colorized collection output
                        if NEW_COLOR_SYSTEM_AVAILABLE:
                            print_news_collection(f"--- NEWS RESULTS FOR {symbol} ---")
                            print_news_collection(f"ARTICLES FOUND: {article_count}")
                            print_news_collection(f"NEWS VOLUME: {news_data.news_volume}")
                            print_news_collection(f"DATA QUALITY: {news_data.data_quality_score:.2f}")
                            print_news_collection(f"SOURCES USED: {', '.join(news_data.sources_used)}")
                            print_news_collection(f"STALENESS: {news_data.staleness_minutes} minutes")
                        else:
                            self.logger.info(f"ARTICLES FOUND: {article_count}")
                            self.logger.info(f"NEWS VOLUME: {news_data.news_volume}")
                            self.logger.info(f"DATA QUALITY: {news_data.data_quality_score:.2f}")
                            self.logger.info(f"SOURCES USED: {', '.join(news_data.sources_used)}")
                            self.logger.info(f"STALENESS: {news_data.staleness_minutes} minutes")

                        # ENHANCED ARTICLE LOGGING - Show detailed article information
                        self.logger.info(f"=== DETAILED ARTICLE BREAKDOWN FOR {symbol} ===")
                        print(f"\n=== DETAILED ARTICLE BREAKDOWN FOR {symbol} ===")
                        for i, article in enumerate(news_data.key_articles):
                            self.logger.info(f"ARTICLE #{i+1}:")
                            print(f"ARTICLE #{i+1}:")
                            self.logger.info(f"  TITLE: {article.title}")
                            print(f"  TITLE: {article.title}")
                            self.logger.info(f"  SOURCE: {article.source}")
                            print(f"  SOURCE: {article.source}")
                            self.logger.info(f"  PUBLISHED: {article.published_date.strftime('%Y-%m-%d %H:%M:%S')}")
                            print(f"  PUBLISHED: {article.published_date.strftime('%Y-%m-%d %H:%M:%S')}")
                            self.logger.info(f"  URL: {article.url}")
                            print(f"  URL: {article.url}")

                            if hasattr(article, 'author') and article.author:
                                self.logger.info(f"  AUTHOR: {article.author}")
                                print(f"  AUTHOR: {article.author}")

                            # Show content preview (first 300 characters)
                            if hasattr(article, 'content') and article.content:
                                content_preview = article.content[:300].replace('\n', ' ').replace('\r', ' ')
                                self.logger.info(f"  CONTENT PREVIEW: {content_preview}...")
                                print(f"  CONTENT PREVIEW: {content_preview}...")

                            # Show symbols mentioned
                            if hasattr(article, 'symbols_mentioned') and article.symbols_mentioned:
                                self.logger.info(f"  SYMBOLS MENTIONED: {article.symbols_mentioned}")
                                print(f"  SYMBOLS MENTIONED: {article.symbols_mentioned}")

                            # Show any existing sentiment if available
                            if hasattr(article, 'sentiment_score') and article.sentiment_score is not None:
                                self.logger.info(f"  EXISTING SENTIMENT: {article.sentiment_score:.3f}")
                                print(f"  EXISTING SENTIMENT: {article.sentiment_score:.3f}")

                            self.logger.info(f"  STATUS: QUEUED FOR FINBERT ANALYSIS")
                            print(f"  STATUS: QUEUED FOR FINBERT ANALYSIS")
                            self.logger.info("  " + "="*60)
                            print("  " + "="*60)

                        # Show first few article details
                        for i, article in enumerate(news_data.key_articles[:3]):  # Show first 3 articles
                            self.logger.info(f"  ARTICLE {i+1}:")
                            self.logger.info(f"    TITLE: {article.title[:100]}{'...' if len(article.title) > 100 else ''}")
                            self.logger.info(f"    SOURCE: {article.source}")
                            self.logger.info(f"    PUBLISHED: {article.published_date.strftime('%H:%M:%S')}")
                            if hasattr(article, 'content') and article.content:
                                content_preview = article.content[:200].replace('\n', ' ')
                                self.logger.info(f"    CONTENT: {content_preview}{'...' if len(article.content) > 200 else ''}")
                            if hasattr(article, 'symbols_mentioned') and article.symbols_mentioned:
                                self.logger.info(f"    SYMBOLS MENTIONED: {article.symbols_mentioned}")

                        if len(news_data.key_articles) > 3:
                            self.logger.info(f"  ... and {len(news_data.key_articles) - 3} more articles")

                        # Show market events if any
                        if news_data.market_events:
                            self.logger.info(f"MARKET EVENTS DETECTED:")
                            for event in news_data.market_events[:3]:  # Show first 3 events
                                self.logger.info(f"  -> {event}")

                    # Cache the data
                    self.cache.set(symbol, news_data)

                    # Update shared state with metadata
                    self._update_shared_state_metadata(symbol, news_data)

                    # Collect articles for batch FinBERT processing
                    if news_data.key_articles:
                        articles_for_finbert.extend([
                            (symbol, article) for article in news_data.key_articles
                        ])
                        
                        # Show colorized processing output
                        if COLORIZED_OUTPUT_AVAILABLE:
                            show_news_processing(symbol, len(news_data.key_articles))
                        
                        self.logger.info(f"QUEUED FOR FINBERT: {len(news_data.key_articles)} articles for sentiment analysis")
                    else:
                        self.logger.info(f"NO NEWS FOUND: No articles collected for {symbol}")

                except Exception as e:
                    self.logger.error(f"ERROR PROCESSING {symbol}: {e}")
                    continue

            # Send all collected articles to FinBERT in batch
            if articles_for_finbert:
                self.logger.info(f"=== SENDING TO FINBERT ===")
                self.logger.info(f"TOTAL ARTICLES: {len(articles_for_finbert)} articles from {len([s for s, _ in articles_for_finbert])} symbols")
                self._send_batch_to_finbert(articles_for_finbert, priority=False)

            # Show colorized completion for each symbol
            if COLORIZED_OUTPUT_AVAILABLE:
                processing_time = time.time() - (self.last_precache_time or time.time())
                for symbol in self.watchlist_symbols:
                    if symbol in watchlist_data and watchlist_data[symbol]:
                        article_count = len(watchlist_data[symbol].key_articles) if watchlist_data[symbol].key_articles else 0
                        show_pipeline_completion(symbol, article_count, processing_time, 1)  # Assume 1 signal per symbol

            self.last_precache_time = time.time()
            self.stats['total_articles_collected'] += total_articles
            self.stats['watchlist_precaches'] += 1

            self.logger.info(f"=== NEWS COLLECTION COMPLETE ===")
            self.logger.info(f"SUMMARY: {total_articles} total articles collected")
            self.logger.info(f"FINBERT QUEUE: {len(articles_for_finbert)} articles sent for sentiment analysis")

        except Exception as e:
            self.logger.error(f"ERROR IN NEWS COLLECTION: {e}")
            import traceback
            self.logger.error(f"TRACEBACK: {traceback.format_exc()}")

    def _update_shared_state_metadata(self, symbol: str, news_data: ProcessedNewsData, priority: bool = False):
        """Update shared state with news metadata only (no sentiment)"""
        try:
            if not self.shared_state:
                return

            # Only update news metadata - sentiment will come from FinBERT process
            news_metadata = {
                'news_volume': news_data.news_volume,
                'market_events': news_data.market_events,
                'data_quality_score': news_data.data_quality_score,
                'staleness_minutes': news_data.staleness_minutes,
                'sources_used': news_data.sources_used,
                'article_count': len(news_data.key_articles),
                'sources_count': len(news_data.sources_used),
                'last_updated': news_data.timestamp.isoformat(),
                'priority_update': priority
            }

            # Update shared state news metadata
            self.shared_state.update_news_metadata(symbol, news_metadata)

            self.logger.debug(f"Updated news metadata for {symbol}: "
                            f"volume={news_data.news_volume}, quality={news_data.data_quality_score:.3f}")

        except Exception as e:
            self.logger.error(f"Error updating shared state metadata for {symbol}: {e}")

    def _send_articles_to_finbert(self, symbol: str, articles: List[NewsArticle], priority: bool = False):
        """Send individual symbol's articles to FinBERT process"""
        try:
            if not articles:
                return

            # Prepare articles for FinBERT analysis
            articles_for_sentiment = []
            for article in articles:
                articles_for_sentiment.append({
                    'id': f"{symbol}_{hash(article.url)}",
                    'title': article.title,
                    'content': article.content[:800],  # Increased limit for better sentiment analysis
                    'source': article.source,
                    'published_date': article.published_date.isoformat(),
                    'symbols': article.symbols_mentioned,
                    'url': article.url,
                    'author': article.author
                })

            # Create message for FinBERT using Redis ProcessMessage
            finbert_message = create_process_message(
                message_type="FINBERT_ANALYSIS",
                sender_id=self.process_id,
                recipient_id="finbert_sentiment",
                data={
                    'symbol': symbol,
                    'articles': articles_for_sentiment,
                    'request_id': f"{symbol}_{int(time.time())}",
                    'priority': priority,
                    'batch_processing': False
                },
                priority=MessagePriority.HIGH if priority else MessagePriority.MEDIUM
            )

            # Send to FinBERT queue via Redis for cross-process communication
            if self.redis_manager:
                self.redis_manager.send_message(finbert_message)
                self.stats['articles_sent_to_finbert'] += len(articles_for_sentiment)

                self.logger.debug(f"Sent {len(articles_for_sentiment)} articles to FinBERT for {symbol}")

        except Exception as e:
            self.logger.error(f"Error sending articles to FinBERT for {symbol}: {e}")

    def _send_batch_to_finbert(self, symbol_articles: List[tuple], priority: bool = False):
        """Send multiple symbols' articles to FinBERT in batch"""
        try:
            if not symbol_articles:
                return

            # Group articles by symbol for efficient processing
            symbol_groups = {}
            for symbol, article in symbol_articles:
                if symbol not in symbol_groups:
                    symbol_groups[symbol] = []
                symbol_groups[symbol].append(article)

            # Send each symbol group to FinBERT
            for symbol, articles in symbol_groups.items():
                self._send_articles_to_finbert(symbol, articles, priority)

            self.logger.info(f"Sent batch of {len(symbol_articles)} articles for {len(symbol_groups)} symbols to FinBERT")

        except Exception as e:
            self.logger.error(f"Error sending batch to FinBERT: {e}")

    def _should_reprocess_for_finbert(self, symbol: str, news_data: ProcessedNewsData) -> bool:
        """Check if cached news should be reprocessed by FinBERT"""
        # Simple heuristic: reprocess if data is older than 1 hour
        age_minutes = (datetime.now() - news_data.timestamp).total_seconds() / 60
        return age_minutes > 60

    def _is_market_hours(self) -> bool:
        """FIXED: Always return True - collect news 24/7 regardless of market hours"""
        # User requested news collection no matter what day or time
        # News sentiment is valuable even outside market hours for preparation
        return True

    def _get_priority_message(self, message_type: str, timeout: float = 0.1) -> Optional[ProcessMessage]:
        """Get message of specific type from priority queue"""
        try:
            if not self.priority_queue:
                return None

            # This is a simplified implementation
            # In practice, you'd need to implement priority queue filtering
            # based on your existing QueueManager implementation
            message = self.priority_queue.get(timeout=timeout)

            if message and message.message_type == message_type:
                return message
            elif message:
                # Put it back if it's not the type we want
                self.priority_queue.put(message)

            return None

        except:
            return None

    def _cleanup(self):
        """Cleanup resources"""
        try:
            self.logger.info("Cleaning up Enhanced News Collector Process")

            # Clear cache to free memory
            self.cache.clear()

            # Log final statistics
            self.logger.info(f"Final statistics: {self.stats}")

        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")

    def get_process_info(self) -> Dict[str, Any]:
        """Return process-specific information for monitoring"""
        return {
            'process_type': 'enhanced_news_collector',
            'watchlist_symbols': len(self.watchlist_symbols),
            'cache_stats': self.cache.get_cache_stats(),
            'available_sources': self.news_collector.get_available_sources() if self.news_collector else [],
            'statistics': self.stats.copy(),
            'last_precache': datetime.fromtimestamp(self.last_precache_time).isoformat() if self.last_precache_time > 0 else None,
            'is_market_hours': self._is_market_hours(),
            'finbert_integration': True
        }
