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

# Import existing multiprocess infrastructure
try:
    from multiprocess_infrastructure.base_process import BaseProcess, ProcessState
    from multiprocess_infrastructure.queue_manager import QueueMessage, MessageType, MessagePriority
except ImportError:
    # Fallback for different project structures
    try:
        from Friren_V1.multiprocess_infrastructure.base_process import BaseProcess, ProcessState
        from Friren_V1.multiprocess_infrastructure.queue_manager import QueueMessage, MessageType, MessagePriority
    except ImportError:
        # Create minimal stubs if infrastructure not available
        from enum import Enum
        from abc import ABC, abstractmethod

        class ProcessState(Enum):
            INITIALIZING = "initializing"
            RUNNING = "running"
            STOPPED = "stopped"
            ERROR = "error"

        class MessageType(Enum):
            NEWS_REQUEST = "news_request"
            REGIME_CHANGE = "regime_change"
            FINBERT_ANALYSIS = "finbert_analysis"

        class MessagePriority(Enum):
            HIGH = "high"
            MEDIUM = "medium"
            LOW = "low"

        class QueueMessage:
            def __init__(self, type, priority, sender_id, recipient_id, payload):
                self.type = type
                self.priority = priority
                self.sender_id = sender_id
                self.recipient_id = recipient_id
                self.payload = payload

        class BaseProcess(ABC):
            def __init__(self, process_id: str):
                self.process_id = process_id
                self.state = ProcessState.INITIALIZING
                self.logger = logging.getLogger(f"process.{process_id}")
                self.error_count = 0
                self.shared_state = None
                self.priority_queue = None

            @abstractmethod
            def _initialize(self): pass
            @abstractmethod
            def _process_cycle(self): pass
            @abstractmethod
            def _cleanup(self): pass
            @abstractmethod
            def get_process_info(self): pass

# Import the Enhanced News Collector utility we built
try:
    from trading_engine.data.news_collector import EnhancedNewsCollector, ProcessedNewsData
    from trading_engine.data.news.base import NewsArticle
except ImportError:
    try:
        from Friren_V1.trading_engine.data.news_collector import EnhancedNewsCollector, ProcessedNewsData
        from Friren_V1.trading_engine.data.news.base import NewsArticle
    except ImportError:
        # Create minimal stubs for testing
        from dataclasses import dataclass
        from datetime import datetime

        @dataclass
        class NewsArticle:
            title: str
            content: str
            source: str
            url: str
            published_date: datetime
            symbols_mentioned: List[str]
            author: Optional[str] = None

        @dataclass
        class ProcessedNewsData:
            symbol: str
            key_articles: List[NewsArticle]
            news_volume: int
            overall_sentiment_score: float
            sentiment_confidence: float
            professional_sentiment: float
            social_sentiment: float
            market_events: List[str]
            data_quality_score: float
            staleness_minutes: int
            sources_used: List[str]
            timestamp: datetime

        class EnhancedNewsCollector:
            def collect_symbol_news(self, symbol, hours_back=6, max_articles_per_source=15):
                # Mock implementation for testing
                return ProcessedNewsData(
                    symbol=symbol,
                    key_articles=[],
                    news_volume=0,
                    overall_sentiment_score=0.0,
                    sentiment_confidence=0.0,
                    professional_sentiment=0.0,
                    social_sentiment=0.0,
                    market_events=[],
                    data_quality_score=0.0,
                    staleness_minutes=0,
                    sources_used=[],
                    timestamp=datetime.now()
                )

            def collect_watchlist_news(self, symbols, hours_back=6, max_articles_per_symbol=12):
                return {symbol: self.collect_symbol_news(symbol) for symbol in symbols}

            def get_available_sources(self):
                return ["MockSource"]


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


class EnhancedNewsCollectorProcess(BaseProcess):
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
        self.watchlist_symbols = watchlist_symbols or ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']
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

    def _check_regime_alerts(self) -> Optional[QueueMessage]:
        """Check for regime change alerts (highest priority)"""
        try:
            # Check for regime change messages with timeout
            message = self._get_priority_message(MessageType.REGIME_CHANGE, timeout=0.1)
            return message
        except:
            return None

    def _check_news_requests(self) -> Optional[QueueMessage]:
        """Check for regular news requests"""
        try:
            # Check for news request messages
            message = self._get_priority_message(MessageType.NEWS_REQUEST, timeout=0.1)
            return message
        except:
            return None

    def _handle_regime_change(self, message: QueueMessage):
        """Handle regime change alert - immediate news processing"""
        try:
            self.logger.info("Processing regime change alert")

            payload = message.payload
            affected_symbols = payload.get('symbols', self.watchlist_symbols)
            regime_type = payload.get('new_regime', 'UNKNOWN')

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

    def _handle_news_request(self, message: QueueMessage):
        """Handle specific news request from decision engine or strategy analyzer"""
        try:
            payload = message.payload
            symbol = payload.get('symbol')
            force_refresh = payload.get('force_refresh', False)

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
        """Check if it's time for watchlist precaching"""
        current_time = time.time()

        # Check if enough time has passed
        if current_time - self.last_precache_time < self.precache_interval:
            return False

        # During market hours, precache more frequently
        if self._is_market_hours():
            return True

        # After hours, precache less frequently
        return current_time - self.last_precache_time > (self.precache_interval * 2)

    def _precache_watchlist_news(self):
        """Proactively collect and cache news for watchlist symbols"""
        try:
            self.logger.info(f"Starting watchlist precaching for {len(self.watchlist_symbols)} symbols")

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
                    # Cache the data
                    self.cache.set(symbol, news_data)

                    # Update shared state with metadata
                    self._update_shared_state_metadata(symbol, news_data)

                    # Collect articles for batch FinBERT processing
                    if news_data.key_articles:
                        articles_for_finbert.extend([
                            (symbol, article) for article in news_data.key_articles
                        ])

                    total_articles += news_data.news_volume

                except Exception as e:
                    self.logger.error(f"Error precaching {symbol}: {e}")
                    continue

            # Send all collected articles to FinBERT in batch
            if articles_for_finbert:
                self._send_batch_to_finbert(articles_for_finbert, priority=False)

            self.last_precache_time = time.time()
            self.stats['total_articles_collected'] += total_articles
            self.stats['watchlist_precaches'] += 1

            self.logger.info(f"Watchlist precaching complete: {total_articles} total articles, "
                           f"{len(articles_for_finbert)} sent to FinBERT")

        except Exception as e:
            self.logger.error(f"Error in watchlist precaching: {e}")

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

            # Create message for FinBERT
            finbert_message = QueueMessage(
                type=MessageType.FINBERT_ANALYSIS,
                priority=MessagePriority.HIGH if priority else MessagePriority.MEDIUM,
                sender_id=self.process_id,
                recipient_id="finbert_sentiment",
                payload={
                    'symbol': symbol,
                    'articles': articles_for_sentiment,
                    'request_id': f"{symbol}_{int(time.time())}",
                    'priority': priority,
                    'batch_processing': False
                }
            )

            # Send to FinBERT queue
            if self.priority_queue:
                self.priority_queue.put(finbert_message)
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
        """Check if it's currently market hours (simple heuristic)"""
        from datetime import datetime

        now = datetime.now()

        # Monday = 0, Sunday = 6
        if now.weekday() >= 5:  # Weekend
            return False

        # Market hours: 9:30 AM - 4:00 PM ET (simplified)
        market_start = now.replace(hour=9, minute=30, second=0, microsecond=0)
        market_end = now.replace(hour=16, minute=0, second=0, microsecond=0)

        return market_start <= now <= market_end

    def _get_priority_message(self, message_type: MessageType, timeout: float = 0.1) -> Optional[QueueMessage]:
        """Get message of specific type from priority queue"""
        try:
            if not self.priority_queue:
                return None

            # This is a simplified implementation
            # In practice, you'd need to implement priority queue filtering
            # based on your existing QueueManager implementation
            message = self.priority_queue.get(timeout=timeout)

            if message and message.type == message_type:
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

