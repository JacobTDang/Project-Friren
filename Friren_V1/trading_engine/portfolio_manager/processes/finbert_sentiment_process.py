"""
processes/finbert_sentiment_process.py

FinBERT Sentiment Analysis Process - Clean Architecture

Consumes FINBERT_ANALYSIS messages from Enhanced News Collector,
uses the EnhancedFinBERT utility tool for sentiment analysis,
and updates shared state with sentiment scores for the decision engine.

Clean Architecture:
- Layer 1: Process infrastructure (BaseProcess, queues, shared state)
- Layer 2: Task parallelization (MultiprocessManager)
- Analytics: Business logic (EnhancedFinBERT utility tool)
"""

import time
import threading
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import logging
from dataclasses import dataclass, asdict
from collections import deque
import numpy as np
import sys
import os
import torch

# Add project root to Python path for imports
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
if project_root not in sys.path:
    sys.path.append(project_root)

# Import color system for FinBERT analysis (dark yellow)
try:
    from terminal_color_system import print_finbert_analysis, print_error, print_warning, print_success, create_colored_logger
    COLOR_SYSTEM_AVAILABLE = True
except ImportError:
    COLOR_SYSTEM_AVAILABLE = False

# Import Redis-based infrastructure
try:
    from Friren_V1.multiprocess_infrastructure.redis_base_process import RedisBaseProcess, ProcessState
    from Friren_V1.multiprocess_infrastructure.trading_redis_manager import (
        get_trading_redis_manager, create_process_message, MessagePriority, ProcessMessage
    )
except ImportError:
    # Create minimal stubs if infrastructure not available
    from enum import Enum
    from abc import ABC, abstractmethod

    class ProcessState(Enum):
        INITIALIZING = "initializing"
        RUNNING = "running"
        STOPPED = "stopped"
        ERROR = "error"

    # Legacy stub classes no longer needed - using Redis ProcessMessage system

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

# Import the FinBERT utility tool
try:
    from Friren_V1.trading_engine.sentiment.finBERT_analysis import EnhancedFinBERT, BatchSentimentResult
except ImportError:
    try:
        from trading_engine.sentiment.finBERT_analysis import EnhancedFinBERT, BatchSentimentResult
    except ImportError:
        # Create minimal stub for testing
        from dataclasses import dataclass

        @dataclass
        class BatchSentimentResult:
            results: List = None
            batch_processing_time: float = 0.0
            success_count: int = 0
            error_count: int = 0
            average_confidence: float = 0.0
            sentiment_distribution: Dict = None

        # NO MOCK FINBERT - Real FinBERT required or process fails
        class FailureFinBERT:
            def __init__(self, **kwargs):
                self.initialized = False

            def initialize(self):
                raise RuntimeError("FinBERT not available - no mock/fallback allowed")

            def analyze_batch(self, texts, article_ids):
                raise RuntimeError("FinBERT not available - no mock/fallback allowed")


@dataclass
class SymbolSentimentSummary:
    """Aggregated sentiment summary for a symbol"""
    symbol: str
    overall_sentiment: float    # -1.0 to +1.0
    confidence: float          # 0.0 to 1.0
    article_count: int
    positive_ratio: float
    negative_ratio: float
    neutral_ratio: float
    sentiment_strength: str    # WEAK, MODERATE, STRONG
    source_breakdown: Dict[str, float]  # Sentiment by news source
    timestamp: datetime


# Add color constants for console output
class Colors:
    YELLOW = '\033[93m'
    RED = '\033[91m'
    GREEN = '\033[92m'
    BLUE = '\033[94m'
    WHITE = '\033[97m'
    BOLD = '\033[1m'
    RESET = '\033[0m'


def sentiment_analysis_worker(task: Dict[str, Any]) -> Dict[str, Any]:
    """
    Worker function for parallel sentiment analysis
    Uses the EnhancedFinBERT utility tool for actual analysis

    Args:
        task: Dictionary containing sentiment analysis task data

    Returns:
        Dictionary with sentiment analysis results
    """
    try:
        # Initialize FinBERT tool in worker process - NO FALLBACK ALLOWED
        finbert_tool = EnhancedFinBERT(
            batch_size=8,  # Optimized for t3.micro
            device="cpu"   # CPU-only for t3.micro
        )

        # Initialize the tool - MUST succeed or process fails
        initialization_success = finbert_tool.initialize()
        if not initialization_success:
            raise RuntimeError("FinBERT initialization failed - no mock/fallback allowed")

        # Extract task data
        symbol = task['symbol']
        articles = task['articles']
        request_id = task.get('request_id', f"{symbol}_{int(time.time())}")

        # Prepare texts and metadata for analysis
        texts = []
        article_ids = []
        article_metadata = []

        for i, article in enumerate(articles):
            # Combine title and content for richer sentiment analysis
            title = article.get('title', '')
            content = article.get('content', '')
            combined_text = f"{title}. {content}" if content else title

            article_id = article.get('id', f"{symbol}_{i}_{hash(combined_text) % 10000}")

            texts.append(combined_text)
            article_ids.append(article_id)
            article_metadata.append({
                'source': article.get('source', ''),
                'published_date': article.get('published_date', ''),
                'url': article.get('url', ''),
                'symbols': article.get('symbols', []),
                'author': article.get('author', '')
            })

        if not texts:
            return {
                'symbol': symbol,
                'request_id': request_id,
                'summary': None,
                'detailed_results': [],
                'success': False,
                'error': 'No articles to analyze',
                'model_used': 'none'
            }

        # Run batch sentiment analysis using the utility tool
        batch_result = finbert_tool.analyze_batch(texts, article_ids)

        # Create symbol summary from batch results
        summary = _create_symbol_summary(symbol, batch_result, article_metadata)

        # Prepare detailed results for debugging/monitoring
        detailed_results = []
        for result, metadata in zip(batch_result.results, article_metadata):
            detailed_results.append({
                'article_id': result.article_id,
                'sentiment_score': result.sentiment_score,
                'confidence': result.confidence,
                'classification': result.classification,
                'source': metadata['source'],
                'processing_time': result.processing_time,
                'model_version': result.model_version
            })

        return {
            'symbol': symbol,
            'request_id': request_id,
            'summary': asdict(summary),
            'detailed_results': detailed_results,
            'batch_stats': {
                'processing_time': batch_result.batch_processing_time,
                'success_count': batch_result.success_count,
                'error_count': batch_result.error_count,
                'average_confidence': batch_result.average_confidence,
                'distribution': batch_result.sentiment_distribution
            },
            'success': True,
            'model_used': 'finbert'
        }

    except Exception as e:
        return {
            'symbol': task.get('symbol', 'unknown'),
            'request_id': task.get('request_id', 'unknown'),
            'summary': None,
            'detailed_results': [],
            'success': False,
            'error': str(e),
            'model_used': 'error'
        }


def _create_symbol_summary(symbol: str, batch_result: BatchSentimentResult,
                          article_metadata: List[Dict]) -> SymbolSentimentSummary:
    """Create aggregated sentiment summary for a symbol"""

    if not batch_result.results:
        return SymbolSentimentSummary(
            symbol=symbol,
            overall_sentiment=0.0,
            confidence=0.0,
            article_count=0,
            positive_ratio=0.0,
            negative_ratio=0.0,
            neutral_ratio=0.0,
            sentiment_strength='WEAK',
            source_breakdown={},
            timestamp=datetime.now()
        )

    # Calculate weighted averages
    sentiments = [r.sentiment_score for r in batch_result.results]
    confidences = [r.confidence for r in batch_result.results]
    classifications = [r.classification for r in batch_result.results]

    # Weighted average sentiment by confidence
    if confidences and sum(confidences) > 0:
        weights = np.array(confidences)
        overall_sentiment = np.average(sentiments, weights=weights)
        avg_confidence = np.mean(confidences)
    else:
        overall_sentiment = np.mean(sentiments)
        avg_confidence = 0.5

    # Calculate distribution ratios
    total_articles = len(batch_result.results)
    positive_ratio = batch_result.sentiment_distribution.get('POSITIVE', 0) / total_articles
    negative_ratio = batch_result.sentiment_distribution.get('NEGATIVE', 0) / total_articles
    neutral_ratio = batch_result.sentiment_distribution.get('NEUTRAL', 0) / total_articles

    # Calculate sentiment by source
    source_sentiments = {}
    for result, metadata in zip(batch_result.results, article_metadata):
        source = metadata.get('source', 'Unknown')
        if source not in source_sentiments:
            source_sentiments[source] = []
        source_sentiments[source].append(result.sentiment_score)

    # Average sentiment by source
    source_breakdown = {}
    for source, scores in source_sentiments.items():
        source_breakdown[source] = np.mean(scores)

    # Determine sentiment strength
    sentiment_magnitude = abs(overall_sentiment)
    if sentiment_magnitude > 0.6 and avg_confidence > 0.7:
        strength = 'STRONG'
    elif sentiment_magnitude > 0.3 and avg_confidence > 0.5:
        strength = 'MODERATE'
    else:
        strength = 'WEAK'

    return SymbolSentimentSummary(
        symbol=symbol,
        overall_sentiment=overall_sentiment,
        confidence=avg_confidence,
        article_count=total_articles,
        positive_ratio=positive_ratio,
        negative_ratio=negative_ratio,
        neutral_ratio=neutral_ratio,
        sentiment_strength=strength,
        source_breakdown=source_breakdown,
        timestamp=datetime.now()
    )


class FinBERTSentimentProcess(RedisBaseProcess):
    """
    FinBERT Sentiment Analysis Process - Clean Architecture

    Layer 1: Process Infrastructure
    - Inherits from BaseProcess for lifecycle management
    - Handles queue messages and shared state updates
    - Manages timing, batching, and error recovery

    Layer 2: Task Parallelization
    - Uses MultiprocessManager for efficient parallel processing
    - Optimized batch processing for t3.micro constraints

    Analytics Layer: EnhancedFinBERT Utility
    - Delegates all sentiment analysis to the utility tool
    - Clean separation of business logic from process logic
    """

    def __init__(self, process_id: str = "finbert_sentiment",
                 batch_size: int = 4,           # Small for t3.micro
                 batch_timeout: int = 5,        # 5 seconds max wait
                 processing_timeout: int = 30,  # 30 seconds per batch
                 max_queue_size: int = 50):
        super().__init__(process_id)

        self.batch_size = batch_size
        self.batch_timeout = batch_timeout
        self.processing_timeout = processing_timeout
        self.max_queue_size = max_queue_size

        # Layer 2: Task parallelization (initialized in _initialize)
        self.multiprocess_manager = None

        # Processing queue and batching
        self.processing_queue = deque()
        self.batch_timer = None

        # Process state tracking
        self.last_processing_time = None
        self.messages_processed = 0
        self.articles_analyzed = 0
        self.sentiment_updates_sent = 0

        # Performance tracking
        self.processing_times = deque(maxlen=100)
        self.error_count = 0
        self.model_status = "unknown"  # finbert, mock, or error

        self._safe_log("info", f"FinBERT Sentiment Process configured - "
                        f"batch_size: {batch_size}, timeout: {processing_timeout}s")

    def _initialize(self):
        self.logger.critical("EMERGENCY: ENTERED _initialize for sentiment_analyzer")
        print("EMERGENCY: ENTERED _initialize for sentiment_analyzer")
        """Initialize process-specific components"""
        self.logger.info("Initializing FinBERT Sentiment Process...")

        try:
            # Layer 2: Initialize generic multiprocess manager for parallel analysis
            try:
                from Friren_V1.trading_engine.portfolio_manager.tools.multiprocess_manager import MultiprocessManager
            except ImportError:
                try:
                    from trading_engine.portfolio_manager.tools.multiprocess_manager import MultiprocessManager
                except ImportError:
                    self.logger.warning("MultiprocessManager not available, using single-threaded processing")
                    MultiprocessManager = None

            if MultiprocessManager:
                self.multiprocess_manager = MultiprocessManager(
                    max_workers=2,  # Conservative for t3.micro (2 cores)
                    max_tasks=10   # Limited task queue for memory
                )
                self.logger.info("MultiprocessManager initialized for FinBERT processing")
            else:
                self.multiprocess_manager = None
                self.logger.info("Using single-threaded FinBERT processing")

            # Test FinBERT tool availability
            self._test_finbert_availability()

            # Start batch processing timer
            self._start_batch_timer()

            self.state = ProcessState.RUNNING
            self.logger.info("FinBERT Sentiment Process initialization complete")

        except Exception as e:
            self.logger.error(f"Failed to initialize FinBERT Sentiment Process: {e}")
            self.state = ProcessState.ERROR
            raise
        self.logger.critical("EMERGENCY: EXITING _initialize for sentiment_analyzer")
        print("EMERGENCY: EXITING _initialize for sentiment_analyzer")

    def _test_finbert_availability(self):
        """Test if FinBERT model is available with fast startup"""
        try:
            # PRODUCTION FIX: Fast startup - only test imports, defer model loading
            self.logger.info("FAST STARTUP: Testing FinBERT imports only...")
            
            try:
                from Friren_V1.trading_engine.sentiment.finBERT_analysis import EnhancedFinBERT
            except ImportError:
                from trading_engine.sentiment.finBERT_analysis import EnhancedFinBERT

            # PRODUCTION: Don't initialize model during startup - do it lazily
            self.model_status = "finbert_lazy"  # Will initialize on first use
            self.logger.info("FinBERT imports successful - model will load on first use")
            self._finbert_class = EnhancedFinBERT  # Store class for lazy loading
            self._finbert_instance = None  # Will be created on first use
            
        except ImportError as ie:
            self.model_status = "mock"
            self.logger.warning(f"FinBERT imports failed: {ie} - using mock analysis")
        except Exception as e:
            self.model_status = "error"
            self.logger.error(f"Error testing FinBERT availability: {e}")
            self.logger.info("Will use fallback processing")

    def _ensure_finbert_loaded(self):
        """Lazy initialization of FinBERT model when actually needed"""
        if self.model_status == "finbert_lazy" and self._finbert_instance is None:
            self.logger.info("LAZY LOADING: Initializing FinBERT model on first use...")
            try:
                self._finbert_instance = self._finbert_class()
                success = self._finbert_instance.initialize()
                if success:
                    self.model_status = "finbert"
                    self.logger.info("FinBERT model successfully loaded on demand")
                else:
                    self.model_status = "mock"
                    self.logger.warning("FinBERT model failed to load - using mock")
            except Exception as e:
                self.model_status = "error"
                self.logger.error(f"FinBERT lazy loading failed: {e}")
        
        return self.model_status in ["finbert", "mock"]

    def _process_cycle(self):
        self.logger.critical("EMERGENCY: ENTERED MAIN LOOP for sentiment_analyzer")
        print("EMERGENCY: ENTERED MAIN LOOP for sentiment_analyzer")
        
        # BUSINESS LOGIC VERIFICATION: Test Redis communication immediately
        try:
            self.logger.info("BUSINESS LOGIC TEST: Verifying Redis communication...")
            test_message = create_process_message(
                sender=self.process_id,
                recipient="test_recipient",
                message_type="STARTUP_TEST",
                data={"test": "business_logic_verification", "timestamp": datetime.now().isoformat()}
            )
            
            # Send test message to Redis
            if self.redis_manager.send_message(test_message):
                self.logger.info("✓ BUSINESS LOGIC: Redis send successful")
                # Try to receive it back
                received = self.redis_manager.receive_message(timeout=1)
                if received:
                    self.logger.info("✓ BUSINESS LOGIC: Redis receive successful")
                else:
                    self.logger.info("✓ BUSINESS LOGIC: No messages in queue (normal)")
            else:
                self.logger.error("✗ BUSINESS LOGIC: Redis send failed")
                
        except Exception as e:
            self.logger.error(f"✗ BUSINESS LOGIC TEST FAILED: {e}")
        try:
            # Check for relevant messages
            finbert_message = self._get_finbert_message()

            if finbert_message:
                self._process_finbert_message(finbert_message)

            # Process batch if ready
            if self._should_process_batch():
                self._process_current_batch()

            # Brief sleep to prevent busy waiting
            time.sleep(2)

        except Exception as e:
            self.logger.error(f"Error in FinBERT processing cycle: {e}")
            self.error_count += 1
            time.sleep(5)

    def _get_finbert_message(self, timeout: float = 0.1) -> Optional[ProcessMessage]:
        """Get relevant messages from Redis queue"""
        try:
            if not self.redis_manager:
                return None

            # Non-blocking Redis message check
            message = self.redis_manager.receive_message(timeout=timeout)

            if message:
                # Handle different message types
                if message.message_type == "FINBERT_ANALYSIS":
                    return message
                elif message.message_type == "REGIME_CHANGE":
                    # Handle regime change - could affect sentiment processing
                    self.logger.info(f"Received regime change: {message.data}")
                    print(f"Received regime change: {message.data}")
                    # Could update processing parameters based on regime
                    return None  # Don't queue for batch processing
                elif message.message_type == "NEWS_REQUEST":
                    # Handle news request - could trigger sentiment analysis
                    self.logger.info(f"Received news request: {message.data}")
                    print(f"Received news request: {message.data}")
                    # Could trigger sentiment analysis for new articles
                    return None  # Don't queue for batch processing
                else:
                    # Put back other message types for other processes
                    self.redis_manager.send_message(message)
                    return None

            return None

        except Exception as e:
            self.logger.error(f"Error getting FinBERT message: {e}")
            return None

    def _process_finbert_message(self, message: ProcessMessage):
        """Process FINBERT_ANALYSIS message and queue for batch processing"""
        try:
            if message.message_type != "FINBERT_ANALYSIS":
                return

            # Queue the task for batch processing
            self._queue_finbert_task(message)

        except Exception as e:
            self.logger.error(f"Error processing FinBERT message: {e}")
            self.error_count += 1

    def _queue_finbert_task(self, message: ProcessMessage):
        """Queue a FinBERT analysis task for batch processing"""
        try:
            payload = message.data
            symbol = payload.get('symbol', 'UNKNOWN')
            articles = payload.get('articles', [])
            priority = payload.get('priority', False)

            if not articles:
                self.logger.warning(f"No articles provided for FinBERT analysis: {symbol}")
                return

            # ENHANCED LOGGING - Show what articles are being queued for analysis
            self.logger.info(f"=== FINBERT ANALYSIS QUEUE: {symbol} ===")
            print(f"\n=== FINBERT ANALYSIS QUEUE: {symbol} ===")
            self.logger.info(f"ARTICLES TO ANALYZE: {len(articles)}")
            print(f"ARTICLES TO ANALYZE: {len(articles)}")
            self.logger.info(f"PRIORITY PROCESSING: {priority}")
            print(f"PRIORITY PROCESSING: {priority}")

            for i, article in enumerate(articles):
                self.logger.info(f"QUEUED ARTICLE #{i+1}:")
                print(f"QUEUED ARTICLE #{i+1}:")
                self.logger.info(f"  TITLE: {article.get('title', 'Unknown')}")
                print(f"  TITLE: {article.get('title', 'Unknown')}")
                self.logger.info(f"  SOURCE: {article.get('source', 'Unknown')}")
                print(f"  SOURCE: {article.get('source', 'Unknown')}")
                self.logger.info(f"  PUBLISHED: {article.get('published_date', 'Unknown')}")
                print(f"  PUBLISHED: {article.get('published_date', 'Unknown')}")

                # Show content preview for sentiment analysis context
                content = article.get('content', '')
                if content:
                    content_preview = content[:200].replace('\n', ' ').replace('\r', ' ')
                    self.logger.info(f"  CONTENT FOR ANALYSIS: {content_preview}...")
                    print(f"  CONTENT FOR ANALYSIS: {content_preview}...")

                self.logger.info(f"  STATUS: READY FOR FINBERT PROCESSING")
                print(f"  STATUS: READY FOR FINBERT PROCESSING")

            # Create task for batch processing
            task = {
                'symbol': symbol,
                'articles': articles,
                'request_id': payload.get('request_id', f"{symbol}_{int(time.time())}"),
                'priority': priority,
                'timestamp': time.time()
            }

            # Add to processing queue
            self.processing_queue.append(task)

            # Log queue status
            self.logger.info(f"FINBERT QUEUE STATUS: {len(self.processing_queue)} tasks pending")

            # Trigger immediate processing if priority or queue is getting full
            if priority or len(self.processing_queue) >= self.batch_size:
                self.logger.info("TRIGGERING IMMEDIATE FINBERT BATCH PROCESSING...")
                self._process_current_batch()

        except Exception as e:
            self.logger.error(f"Error queuing FinBERT task for {symbol}: {e}")

    def _should_process_batch(self) -> bool:
        """Check if it's time to process the current batch"""
        if not self.processing_queue:
            return False

        # Process if batch is full
        if len(self.processing_queue) >= self.batch_size:
            return True

        # Process if oldest item has been waiting too long
        oldest_task = self.processing_queue[0]
        wait_time = time.time() - oldest_task['received_time']

        return wait_time >= self.batch_timeout

    def _process_current_batch(self):
        """Process current batch of FinBERT analysis tasks"""
        if not self.processing_queue:
            return

        try:
            batch_size = min(len(self.processing_queue), self.batch_size)
            self.logger.info(f"Processing FinBERT batch: {batch_size} tasks")
            start_time = time.time()

            # Extract tasks from queue
            current_batch = []
            for _ in range(batch_size):
                if self.processing_queue:
                    current_batch.append(self.processing_queue.popleft())

            if not current_batch:
                return

            # Run parallel sentiment analysis using the utility tool
            if self.multiprocess_manager:
                analysis_results = self.multiprocess_manager.execute_parallel_tasks(
                    current_batch,
                    sentiment_analysis_worker,
                    timeout=self.processing_timeout
                )
            else:
                # Fallback to sequential processing
                analysis_results = []
                for task in current_batch:
                    result = sentiment_analysis_worker(task)
                    analysis_results.append(result)

            # Process results and update shared state
            updates_sent = self._process_analysis_results(analysis_results)

            # Update process statistics
            batch_time = time.time() - start_time
            self.processing_times.append(batch_time)
            self.last_processing_time = datetime.now()
            self.messages_processed += len(current_batch)
            self.articles_analyzed += sum(len(task['articles']) for task in current_batch)
            self.sentiment_updates_sent += updates_sent

            self.logger.info(f"FinBERT batch complete - {batch_time:.2f}s, "
                           f"{updates_sent} sentiment updates sent")

        except Exception as e:
            self.logger.error(f"Error processing FinBERT batch: {e}")
            self.error_count += 1

    def _process_analysis_results(self, analysis_results: List[Dict]) -> int:
        """Process FinBERT analysis results and update shared state"""
        updates_sent = 0

        for result in analysis_results:
            try:
                if not result or 'error' in result:
                    self.logger.warning(f"Skipping failed analysis result: {result}")
                    continue

                symbol = result.get('symbol', 'UNKNOWN')
                sentiment_data = result.get('sentiment_data', {})

                if not sentiment_data:
                    self.logger.warning(f"No sentiment data for {symbol}")
                    continue

                # ENHANCED LOGGING - Show detailed FinBERT analysis results
                self.logger.info(f"=== FINBERT ANALYSIS RESULTS: {symbol} ===")
                print(f"\n=== FINBERT ANALYSIS RESULTS: {symbol} ===")

                articles_analyzed = sentiment_data.get('articles_analyzed', [])
                overall_sentiment = sentiment_data.get('overall_sentiment_score', 0.0)
                confidence = sentiment_data.get('confidence', 0.0)

                self.logger.info(f"OVERALL SENTIMENT SCORE: {overall_sentiment:.4f}")
                print(f"OVERALL SENTIMENT SCORE: {overall_sentiment:.4f}")
                self.logger.info(f"CONFIDENCE LEVEL: {confidence:.4f}")
                print(f"CONFIDENCE LEVEL: {confidence:.4f}")
                self.logger.info(f"ARTICLES ANALYZED: {len(articles_analyzed)}")
                print(f"ARTICLES ANALYZED: {len(articles_analyzed)}")

                # Show individual article sentiment analysis
                for i, article_result in enumerate(articles_analyzed):
                    self.logger.info(f"ARTICLE #{i+1} ANALYSIS:")
                    print(f"ARTICLE #{i+1} ANALYSIS:")
                    self.logger.info(f"  TITLE: {article_result.get('title', 'Unknown')[:80]}...")
                    print(f"  TITLE: {article_result.get('title', 'Unknown')[:80]}...")
                    self.logger.info(f"  SENTIMENT SCORE: {article_result.get('sentiment_score', 0.0):.4f}")
                    print(f"  SENTIMENT SCORE: {article_result.get('sentiment_score', 0.0):.4f}")
                    self.logger.info(f"  CONFIDENCE: {article_result.get('confidence', 0.0):.4f}")
                    print(f"  CONFIDENCE: {article_result.get('confidence', 0.0):.4f}")
                    self.logger.info(f"  SENTIMENT LABEL: {article_result.get('sentiment_label', 'Unknown')}")
                    print(f"  SENTIMENT LABEL: {article_result.get('sentiment_label', 'Unknown')}")

                    # Show the key phrases that influenced sentiment
                    if 'key_phrases' in article_result:
                        phrases = article_result['key_phrases'][:3]  # Show top 3 phrases
                        self.logger.info(f"  KEY PHRASES: {', '.join(phrases)}")
                        print(f"  KEY PHRASES: {', '.join(phrases)}")

                    # Show positive/negative indicators
                    if 'positive_score' in article_result:
                        pos_score = article_result['positive_score']
                        neg_score = article_result.get('negative_score', 0.0)
                        neu_score = article_result.get('neutral_score', 0.0)
                        self.logger.info(f"  DETAILED SCORES: Pos={pos_score:.3f}, Neg={neg_score:.3f}, Neu={neu_score:.3f}")
                        print(f"  DETAILED SCORES: Pos={pos_score:.3f}, Neg={neg_score:.3f}, Neu={neu_score:.3f}")

                # Update shared state with sentiment data
                if self.shared_state:
                    try:
                        self.shared_state.update_sentiment_data(symbol, sentiment_data)
                        updates_sent += 1
                        self.logger.info(f"SHARED STATE UPDATED: {symbol} sentiment data sent to system")
                        print(f"SHARED STATE UPDATED: {symbol} sentiment data sent to system")
                    except Exception as e:
                        self.logger.error(f"Failed to update shared state for {symbol}: {e}")

                # Log summary
                sentiment_direction = "POSITIVE" if overall_sentiment > 0.1 else "NEGATIVE" if overall_sentiment < -0.1 else "NEUTRAL"
                self.logger.info(f"FINBERT SUMMARY: {symbol} = {sentiment_direction} ({overall_sentiment:.3f}) with {confidence:.1%} confidence")
                print(f"FINBERT SUMMARY: {symbol} = {sentiment_direction} ({overall_sentiment:.3f}) with {confidence:.1%} confidence")
                self.logger.info("=" * 70)
                print("=" * 70)

            except Exception as e:
                self.logger.error(f"Error processing analysis result: {e}")
                continue

        self.logger.info(f"FINBERT BATCH COMPLETE: {updates_sent} sentiment updates sent to system")
        return updates_sent

    def _update_shared_state_sentiment(self, symbol: str, summary: Dict[str, Any], model_used: str):
        """Update shared state with sentiment scores"""
        try:
            if not self.shared_state:
                return

            # Prepare enhanced sentiment data for shared state
            sentiment_data = {
                'overall_sentiment': summary['overall_sentiment'],
                'confidence': summary['confidence'],
                'article_count': summary['article_count'],
                'positive_ratio': summary['positive_ratio'],
                'negative_ratio': summary['negative_ratio'],
                'neutral_ratio': summary['neutral_ratio'],
                'sentiment_strength': summary['sentiment_strength'],
                'source_breakdown': summary.get('source_breakdown', {}),
                'last_updated': summary['timestamp'],
                'data_source': 'finbert',
                'model_used': model_used,
                'process_id': self.process_id
            }

            # Update enhanced sentiment scores in shared state
            self.shared_state.update_enhanced_sentiment_scores(symbol, sentiment_data)

            self.logger.debug(f"Updated sentiment for {symbol}: "
                            f"score={summary['overall_sentiment']:.3f}, "
                            f"confidence={summary['confidence']:.3f}, "
                            f"strength={summary['sentiment_strength']}")

        except Exception as e:
            self.logger.error(f"Error updating shared state sentiment for {symbol}: {e}")

    def _send_sentiment_to_decision_engine(self, symbol: str, summary: Dict[str, Any],
                                          detailed_results: List[Dict], model_used: str) -> bool:
        """
        Send individual sentiment analysis to decision engine in main process

        Sends comprehensive sentiment data for immediate decision-making.
        Each symbol gets its own message for real-time processing.
        """
        try:
            if not self.priority_queue:
                return False

            # Determine message priority based on sentiment characteristics
            priority = self._determine_message_priority(summary)

            # Prepare comprehensive sentiment payload for decision engine
            sentiment_payload = {
                # Core sentiment data
                'symbol': symbol,
                'sentiment_score': summary['overall_sentiment'],      # -1.0 to +1.0
                'confidence': summary['confidence'],                  # 0.0 to 1.0
                'sentiment_strength': summary['sentiment_strength'],  # WEAK/MODERATE/STRONG
                'classification': self._get_primary_classification(summary),  # POSITIVE/NEGATIVE/NEUTRAL

                # Article analysis details
                'article_count': summary['article_count'],
                'positive_ratio': summary['positive_ratio'],
                'negative_ratio': summary['negative_ratio'],
                'neutral_ratio': summary['neutral_ratio'],
                'source_breakdown': summary.get('source_breakdown', {}),

                # Quality and reliability metrics
                'model_used': model_used,                            # finbert/mock/error
                'is_reliable': summary['confidence'] > 0.6,         # Confidence threshold
                'analysis_timestamp': summary['timestamp'],

                # Decision-making context
                'urgency_level': self._calculate_urgency_level(summary),
                'market_moving_potential': self._assess_market_impact(summary),
                'recommendation_context': self._generate_context(summary, detailed_results),

                # Process metadata
                'process_id': self.process_id,
                'request_id': f"{symbol}_{int(time.time())}",
                'sequence_number': self.sentiment_updates_sent + 1
            }

            # Create process message for decision engine
            message = create_process_message(
                sender=self.process_id,
                recipient="market_decision_engine",
                message_type="SENTIMENT_UPDATE",
                data=sentiment_payload,
                priority=priority
            )

            # Send to Redis
            result = self.redis_manager.send_message(message)
            if not result:
                self.logger.error(f"Failed to send sentiment message to Redis for {symbol}")
                return False

            # Log the sentiment message
            self.logger.info(f"Sent sentiment to decision engine: {symbol} "
                           f"score={summary['overall_sentiment']:.3f} "
                           f"confidence={summary['confidence']:.3f} "
                           f"strength={summary['sentiment_strength']} "
                           f"priority={priority.value}")

            return True

        except Exception as e:
            self.logger.error(f"Error sending sentiment to decision engine for {symbol}: {e}")
            return False

    def _determine_message_priority(self, summary: Dict[str, Any]) -> MessagePriority:
        """Determine queue priority based on sentiment characteristics"""
        sentiment_score = abs(summary['overall_sentiment'])
        confidence = summary['confidence']
        strength = summary['sentiment_strength']
        article_count = summary['article_count']

        # HIGH priority: Strong sentiment with high confidence
        if (sentiment_score > 0.6 and confidence > 0.75 and
            strength == 'STRONG' and article_count >= 3):
            return MessagePriority.HIGH

        # MEDIUM priority: Moderate sentiment or good confidence
        elif (sentiment_score > 0.3 and confidence > 0.5) or strength in ['MODERATE', 'STRONG']:
            return MessagePriority.MEDIUM

        # LOW priority: Weak sentiment or low confidence
        else:
            return MessagePriority.LOW

    def _get_primary_classification(self, summary: Dict[str, Any]) -> str:
        """Get primary sentiment classification"""
        sentiment_score = summary['overall_sentiment']

        if sentiment_score > 0.2:
            return 'POSITIVE'
        elif sentiment_score < -0.2:
            return 'NEGATIVE'
        else:
            return 'NEUTRAL'

    def _calculate_urgency_level(self, summary: Dict[str, Any]) -> str:
        """Calculate urgency level for decision engine"""
        sentiment_magnitude = abs(summary['overall_sentiment'])
        confidence = summary['confidence']

        if sentiment_magnitude > 0.7 and confidence > 0.8:
            return 'IMMEDIATE'  # Act within minutes
        elif sentiment_magnitude > 0.5 and confidence > 0.6:
            return 'HIGH'       # Act within 15 minutes
        elif sentiment_magnitude > 0.3:
            return 'MEDIUM'     # Act within 1 hour
        else:
            return 'LOW'        # Monitor only

    def _assess_market_impact(self, summary: Dict[str, Any]) -> str:
        """Assess potential market impact"""
        sentiment_strength = summary['sentiment_strength']
        article_count = summary['article_count']
        confidence = summary['confidence']

        # High impact: Strong sentiment with many articles
        if sentiment_strength == 'STRONG' and article_count >= 5 and confidence > 0.7:
            return 'HIGH'
        elif sentiment_strength in ['MODERATE', 'STRONG'] and article_count >= 3:
            return 'MEDIUM'
        else:
            return 'LOW'

    def _generate_context(self, summary: Dict[str, Any], detailed_results: List[Dict]) -> Dict[str, Any]:
        """Generate additional context for decision engine"""
        return {
            # Source diversity
            'source_diversity': len(summary.get('source_breakdown', {})),
            'dominant_source': max(summary.get('source_breakdown', {}).items(),
                                 key=lambda x: x[1], default=('unknown', 0))[0],

            # Sentiment consistency
            'sentiment_consensus': self._calculate_consensus(detailed_results),
            'sentiment_range': self._calculate_sentiment_range(detailed_results),

            # Article quality indicators
            'avg_confidence': summary['confidence'],
            'reliability_score': min(summary['confidence'] * summary['article_count'] / 10, 1.0),

            # Trading context
            'actionable': summary['confidence'] > 0.6 and abs(summary['overall_sentiment']) > 0.3,
            'watch_list': summary['confidence'] > 0.4 or summary['article_count'] >= 3
        }

    def _calculate_consensus(self, detailed_results: List[Dict]) -> float:
        """Calculate how much articles agree on sentiment direction"""
        if not detailed_results:
            return 0.0

        classifications = [r['classification'] for r in detailed_results]
        total = len(classifications)

        # Find most common classification
        from collections import Counter
        most_common_count = Counter(classifications).most_common(1)[0][1]

        return most_common_count / total

    def _calculate_sentiment_range(self, detailed_results: List[Dict]) -> float:
        """Calculate range of sentiment scores"""
        if not detailed_results:
            return 0.0

        scores = [r['sentiment_score'] for r in detailed_results]
        return max(scores) - min(scores) if scores else 0.0

    def _log_analysis_details(self, result: Dict[str, Any]):
        """Log detailed analysis results for monitoring"""
        if not result.get('success'):
            return

        symbol = result['symbol']
        batch_stats = result.get('batch_stats', {})
        model_used = result.get('model_used', 'unknown')

        self.logger.debug(f"FinBERT analysis for {symbol}: "
                         f"model={model_used}, "
                         f"time={batch_stats.get('processing_time', 0):.3f}s, "
                         f"success_rate={batch_stats.get('success_count', 0)}/{batch_stats.get('success_count', 0) + batch_stats.get('error_count', 0)}")

    def _start_batch_timer(self):
        """Start timer for periodic batch processing"""
        def timer_callback():
            try:
                if self.processing_queue:
                    self._process_current_batch()
            except Exception as e:
                self.logger.error(f"Error in batch timer: {e}")
            finally:
                # Restart timer if process is still running
                if self.state == ProcessState.RUNNING:
                    self._start_batch_timer()

        self.batch_timer = threading.Timer(self.batch_timeout, timer_callback)
        self.batch_timer.daemon = True
        self.batch_timer.start()

    def _cleanup(self):
        """Cleanup process resources"""
        try:
            self.logger.info("Cleaning up FinBERT Sentiment Process")

            # Stop batch timer
            if self.batch_timer:
                self.batch_timer.cancel()

            # Process any remaining items in queue
            if self.processing_queue:
                self.logger.info(f"Processing final batch of {len(self.processing_queue)} items")
                self._process_current_batch()

            # Cleanup multiprocess manager
            if self.multiprocess_manager:
                self.multiprocess_manager.cleanup()

            # Log final statistics
            self.logger.info(f"Final FinBERT statistics: "
                           f"messages={self.messages_processed}, "
                           f"articles={self.articles_analyzed}, "
                           f"updates={self.sentiment_updates_sent}, "
                           f"errors={self.error_count}")

        except Exception as e:
            self.logger.error(f"Error during FinBERT cleanup: {e}")

    def get_process_info(self) -> Dict[str, Any]:
        """Return process-specific information for monitoring"""
        avg_processing_time = (np.mean(self.processing_times)
                             if self.processing_times else 0.0)

        return {
            'process_type': 'finbert_sentiment',
            'model_status': self.model_status,
            'batch_configuration': {
                'batch_size': self.batch_size,
                'batch_timeout': self.batch_timeout,
                'processing_timeout': self.processing_timeout
            },
            'queue_status': {
                'current_queue_length': len(self.processing_queue),
                'max_queue_size': self.max_queue_size
            },
            'statistics': {
                'messages_processed': self.messages_processed,
                'articles_analyzed': self.articles_analyzed,
                'sentiment_updates_sent': self.sentiment_updates_sent,
                'error_count': self.error_count,
                'avg_processing_time': avg_processing_time,
                'error_rate': self.error_count / max(1, self.messages_processed)
            },
            'last_processing': (self.last_processing_time.isoformat()
                              if self.last_processing_time else None),
            'architecture': 'clean_separation',
            'utility_tool': 'EnhancedFinBERT',
            'optimizations': ['batch_processing', 'memory_efficient', 't3_micro_optimized']
        }

    async def _analyze_sentiment(self, text: str) -> Dict[str, float]:
        """Analyze sentiment of text using FinBERT model"""
        try:
            # Tokenize and get predictions
            inputs = self.tokenizer(text, return_tensors="pt", truncation=True, max_length=512, padding=True)

            with torch.no_grad():
                outputs = self.model(**inputs)
                probabilities = torch.nn.functional.softmax(outputs.logits, dim=-1)

            # Get scores for each sentiment class
            scores = {
                'positive': float(probabilities[0][0]),
                'negative': float(probabilities[0][1]),
                'neutral': float(probabilities[0][2])
            }

            # Determine sentiment label and confidence
            sentiment_label = max(scores, key=scores.get)
            confidence = scores[sentiment_label]

            self.logger.debug(f"Sentiment analysis: {sentiment_label} (confidence: {confidence:.3f})")
            print(f"{Colors.RED}Sentiment: {sentiment_label} (confidence: {confidence:.3f}) - {text[:50]}...{Colors.RESET}")

            return {
                'label': sentiment_label,
                'confidence': confidence,
                'scores': scores
            }

        except Exception as e:
            self.logger.error(f"Error analyzing sentiment: {e}")
            print(f"{Colors.RED}Error analyzing sentiment: {e}{Colors.RESET}")
            return {
                'label': 'neutral',
                'confidence': 0.0,
                'scores': {'positive': 0.0, 'negative': 0.0, 'neutral': 1.0}
            }

    async def _process_sentiment_request(self, message: ProcessMessage) -> None:
        """Process sentiment analysis request"""
        try:
            self.logger.info(f"Processing sentiment analysis request for {message.data.get('symbol', 'unknown')}")
            print(f"{Colors.RED}{Colors.BOLD}=== FINBERT SENTIMENT ANALYSIS REQUEST ==={Colors.RESET}")
            print(f"{Colors.RED}Symbol: {message.data.get('symbol', 'unknown')}{Colors.RESET}")

            # Extract data from message
            symbol = message.data.get('symbol')
            articles = message.data.get('articles', [])

            if not articles:
                self.logger.warning(f"No articles provided for sentiment analysis for {symbol}")
                print(f"{Colors.RED}No articles provided for sentiment analysis for {symbol}{Colors.RESET}")
                return

            self.logger.info(f"Analyzing sentiment for {len(articles)} articles for {symbol}")
            print(f"{Colors.RED}Analyzing sentiment for {len(articles)} articles for {symbol}{Colors.RESET}")

            # Process each article
            results = []
            for i, article in enumerate(articles):
                self.logger.debug(f"Processing article {i+1}/{len(articles)}: {article.get('title', 'No title')[:50]}...")
                print(f"{Colors.RED}Processing article {i+1}/{len(articles)}: {article.get('title', 'No title')[:50]}...{Colors.RESET}")

                # Analyze sentiment
                sentiment_result = await self._analyze_sentiment(article.get('content', ''))

                # Create result object
                result = {
                    'title': article.get('title', ''),
                    'source': article.get('source', ''),
                    'sentiment_label': sentiment_result['label'],
                    'confidence': sentiment_result['confidence'],
                    'positive_score': sentiment_result['scores']['positive'],
                    'negative_score': sentiment_result['scores']['negative'],
                    'neutral_score': sentiment_result['scores']['neutral'],
                    'timestamp': datetime.now().isoformat()
                }

                results.append(result)

                # Log detailed result
                self.logger.info(f"Article {i+1} sentiment: {result['sentiment_label']} (confidence: {result['confidence']:.3f})")
                print(f"{Colors.RED}  Result: {result['sentiment_label']} (confidence: {result['confidence']:.3f}){Colors.RESET}")
                print(f"{Colors.RED}    Positive: {result['positive_score']:.3f} | Negative: {result['negative_score']:.3f} | Neutral: {result['neutral_score']:.3f}{Colors.RESET}")

            # Send results back
            response_message = create_process_message(
                sender=self.process_id,
                recipient=message.sender,
                message_type="SENTIMENT_RESULT",
                data={
                    'symbol': symbol,
                    'results': results,
                    'timestamp': datetime.now().isoformat()
                },
                priority=MessagePriority.HIGH
            )

            self.logger.info(f"Sentiment analysis complete for {symbol}: {len(results)} results")
            print(f"{Colors.RED}{Colors.BOLD}Sentiment analysis complete for {symbol}: {len(results)} results{Colors.RESET}")

            # Send response
            await self.queue_manager.send_message(response_message)

        except Exception as e:
            self.logger.error(f"Error processing sentiment request: {e}")
            print(f"{Colors.RED}Error processing sentiment request: {e}{Colors.RESET}")
            # Send error response
            error_message = create_process_message(
                sender=self.process_id,
                recipient=message.sender,
                message_type="ERROR",
                data={
                    'error': str(e),
                    'original_message': message.data
                },
                priority=MessagePriority.HIGH
            )
            await self.queue_manager.send_message(error_message)
