#!/usr/bin/env python3
"""
Unified Trading System Orchestrator

Single, optimized orchestrator that combines:
- Windows-compatible single-threaded execution
- Enhanced position-aware decision logic from enhanced decision rules
- Integration with existing portfolio manager components
- Real trade execution via ExecutionOrchestrator
- Robust error handling and graceful fallbacks
- Positive sentiment queue for watchlist candidates

Architecture:
- Single-threaded to avoid Windows multiprocessing issues
- Sequential component execution in configurable cycles
- Enhanced position-aware logic with sentiment-based sizing
- Real portfolio manager integration when available
- Mock components for testing and fallbacks
- Queue-based positive sentiment discovery for watchlist expansion
"""

import sys
import os

# Configure Django BEFORE any other imports
def setup_django_config():
    """Setup Django configuration if not already done"""
    try:
        import django
        from django.conf import settings

        if not settings.configured:
            os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'Friren_V1.config.settings')
            django.setup()
            return True
        return True
    except Exception:
        # Django not available or misconfigured, continue without it
        return False

# Setup Django configuration first
DJANGO_AVAILABLE = setup_django_config()

import time
import logging
import signal
import queue
from typing import Dict, List, Optional, Any, Tuple, Union
from datetime import datetime, timedelta
from dataclasses import dataclass
from pathlib import Path
import random

# Add project root to path
project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

@dataclass
class MarketAnalysis:
    """Market analysis data for decision making"""
    symbol: str
    sentiment_score: float  # -1.0 to 1.0
    sentiment_label: str   # 'positive', 'negative', 'neutral'
    confidence: float      # 0.0 to 1.0
    news_count: int
    analysis_time: datetime

@dataclass
class PositiveSentimentCandidate:
    """Candidate stock with positive sentiment for watchlist consideration"""
    symbol: str
    sentiment_score: float
    confidence: float
    news_count: int
    key_articles: List[str]  # Headlines of key articles
    market_events: List[str]  # Earnings, analyst actions, etc.
    discovery_time: datetime
    source: str = "sentiment_discovery"  # How it was discovered
    priority_score: float = 0.0  # Combined score for ranking

@dataclass
class WatchlistDecision:
    """Decision on whether to add a candidate to watchlist"""
    symbol: str
    decision: str  # 'ADD_TO_WATCHLIST', 'REJECT', 'MONITOR'
    reasoning: str
    confidence: float
    priority: int  # 1 (high) to 5 (low)
    next_review_time: Optional[datetime] = None

@dataclass
class EnhancedTradingDecision:
    """Enhanced trading decision with position awareness and rebalancing"""
    symbol: str
    base_recommendation: str  # 'BUY', 'SELL', 'HOLD'
    final_action: str        # 'BUY', 'BUY_MORE', 'SELL', 'PARTIAL_SELL', 'REBALANCE_TRIM', 'CONTINUE_HOLDING', 'HOLD'
    confidence: float
    sentiment_score: float
    strategy_name: str
    reasoning: str
    current_position_pct: float
    target_position_pct: float
    is_position_addition: bool

@dataclass
class SystemStatus:
    """System status tracking"""
    start_time: datetime
    symbols: List[str]
    mode: str = "paper_trading"
    cycles_completed: int = 0
    successful_trades: int = 0
    failed_trades: int = 0
    position_additions: int = 0
    last_cycle_time: Optional[datetime] = None
    errors: List[str] = None

    def __post_init__(self):
        if self.errors is None:
            self.errors = []

class UnifiedOrchestrator:
    """
    Unified trading orchestrator that combines all best practices
    """

    def __init__(self,
                 symbols: List[str] = None,
                 cycle_interval_seconds: int = 60,
                 mode: str = "paper_trading"):

        self.symbols = symbols or ['AAPL', 'MSFT', 'GOOGL', 'TSLA', 'NVDA']
        self.cycle_interval = cycle_interval_seconds
        self.mode = mode
        self.running = False

        # Setup logging
        self.logger = self._setup_logging()

        # Initialize system status
        self.status = SystemStatus(
            start_time=datetime.now(),
            symbols=self.symbols,
            mode=mode
        )

        # Initialize sentiment discovery queue system
        self.positive_sentiment_queue = queue.Queue(maxsize=100)
        self.rejected_candidates = set()  # Track rejected symbols to avoid reprocessing
        self.watchlist_decisions = []  # Track all watchlist decisions
        self.last_watchlist_scan = datetime.now()
        self.watchlist_scan_interval = timedelta(hours=2)  # Scan for new candidates every 2 hours

        # Initialize components
        self._initialize_components()

        # Initialize portfolio manager integration
        self._initialize_portfolio_manager_integration()

        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        self.logger.info(f"Unified Orchestrator initialized - Mode: {mode}, Symbols: {self.symbols}")
        self.logger.info("Positive sentiment discovery queue initialized")

    def _initialize_portfolio_manager_integration(self):
        """Initialize integration with portfolio manager components"""
        self.db_manager = None  # Initialize as None

        # Only try to initialize database manager if Django is available
        if not DJANGO_AVAILABLE:
            self.logger.info("Django not available - skipping database integration")
            self.portfolio_manager_available = False
            return

        # Try to initialize database manager for watchlist functionality
        try:
            from Friren_V1.trading_engine.portfolio_manager.tools.db_manager import TradingDBManager

            # Test if Django/database is properly configured
            test_manager = TradingDBManager(process_name="unified_orchestrator")

            # If we get here, database is working
            self.db_manager = test_manager
            self.logger.info("Database manager integrated for watchlist functionality")

            # Try to load existing watchlist from database
            try:
                existing_watchlist = self.db_manager.get_watchlist(active_only=True)
                if existing_watchlist:
                    db_symbols = [item['symbol'] for item in existing_watchlist]
                    # Add database symbols to current watchlist if not already present
                    for symbol in db_symbols:
                        if symbol not in self.symbols:
                            self.symbols.append(symbol)
                    self.logger.info(f"Loaded {len(db_symbols)} symbols from database watchlist")
            except Exception as e:
                self.logger.warning(f"Could not load existing watchlist: {e}")

        except Exception as e:
            self.logger.info(f"Database manager not available - running without database persistence: {e}")
            self.db_manager = None

        # Try to initialize portfolio manager components if available
        try:
            from Friren_V1.trading_engine.portfolio_manager.orchestrator import MainOrchestrator, SystemConfig
            # Check if we should integrate with full portfolio manager
            # For now, we'll just note that it's available
            self.portfolio_manager_available = True
            self.logger.info("Portfolio manager components available for future integration")
        except ImportError:
            self.portfolio_manager_available = False
            self.logger.info("Using standalone orchestrator mode - portfolio manager not available")

    def _setup_logging(self) -> logging.Logger:
        """Setup centralized logging"""
        os.makedirs('logs', exist_ok=True)

        logger = logging.getLogger("unified_orchestrator")
        logger.setLevel(logging.INFO)

        if not logger.handlers:
            formatter = logging.Formatter(
                '%(asctime)s - %(levelname)s - %(message)s'
            )

            # File handler
            file_handler = logging.FileHandler('logs/unified_orchestrator.log')
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

            # Console handler
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)

        return logger

    def _initialize_components(self):
        """Initialize all trading components with fallbacks"""

        # Try to import real portfolio manager components
        try:
            from Friren_V1.trading_engine.portfolio_manager.decision_engine.decision_engine import EnhancedMarketDecisionEngineProcess
            from Friren_V1.trading_engine.portfolio_manager.decision_engine.execution_orchestrator import create_execution_orchestrator
            from Friren_V1.trading_engine.portfolio_manager.decision_engine.risk_manager import EnhancedRiskManager

            self.decision_engine = EnhancedMarketDecisionEngineProcess('main_decision_engine')
            self.execution_orchestrator = create_execution_orchestrator()
            self.risk_manager = EnhancedRiskManager()

            # Check if position sizer is available
            try:
                from Friren_V1.trading_engine.portfolio_manager.tools.position_sizer import PositionSizer
                self.position_sizer = PositionSizer()
            except ImportError:
                self.position_sizer = self._create_mock_position_sizer()

            # Initialize watchlist manager
            try:
                from Friren_V1.trading_engine.portfolio_manager.tools.watchlist_manager import WatchlistManager
                from Friren_V1.trading_engine.portfolio_manager.tools.db_manager import TradingDBManager

                db_manager = TradingDBManager("orchestrator_watchlist")
                self.watchlist_manager = WatchlistManager(db_manager, self.position_sizer)

                # Initialize watchlist from current portfolio and symbols
                init_result = self.watchlist_manager.initialize_from_portfolio(self.symbols)
                self.logger.info(f"Watchlist initialized: {init_result}")

            except ImportError as e:
                self.logger.warning(f"WatchlistManager not available: {e}")
                self.watchlist_manager = None

            self.components_mode = "REAL"
            self.logger.info("Real portfolio manager components loaded successfully")

        except (ImportError, AttributeError, Exception) as e:
            self.logger.warning(f"Portfolio manager components not available: {e}")
            self.logger.info("Using mock components for demonstration")
            self._initialize_mock_components()
            self.components_mode = "MOCK"

        # Initialize position-aware analyzer
        self.position_analyzer = EnhancedPositionAwareAnalyzer(
            self.position_sizer,
            self.logger
        )

        # Initialize sentiment analyzer
        try:
            from Friren_V1.trading_engine.sentiment.finBERT_analysis import FinBERTSentimentAnalyzer
            self.sentiment_analyzer = FinBERTSentimentAnalyzer()
            self.logger.info("Real FinBERT sentiment analyzer loaded")
        except ImportError:
            self.sentiment_analyzer = self._create_mock_sentiment_analyzer()
            self.logger.info("Using mock sentiment analyzer")

        # Initialize news collector
        try:
            from Friren_V1.trading_engine.data.news_collector import EnhancedNewsCollector
            self.news_collector = EnhancedNewsCollector()
            self.logger.info("Real enhanced news collector loaded")
        except ImportError:
            self.news_collector = self._create_mock_news_collector()
            self.logger.info("Using mock news collector")

    def _initialize_mock_components(self):
        """Initialize mock components for testing"""
        self.execution_orchestrator = self._create_mock_execution_orchestrator()
        self.risk_manager = self._create_mock_risk_manager()
        self.position_sizer = self._create_mock_position_sizer()
        self.decision_engine = None  # Will use simplified logic

    def _create_mock_execution_orchestrator(self):
        """Create mock execution orchestrator"""
        class MockExecutionOrchestrator:
            def __init__(self):
                self.execution_count = 0
                self.success_rate = 0.9

            def execute_approved_decision(self, risk_validation, strategy_name: str, strategy_confidence: float):
                self.execution_count += 1

                # Mock execution result
                class ExecutionResult:
                    def __init__(self, success: bool, summary: str = ""):
                        self.was_successful = success
                        self.execution_summary = summary
                        self.executed_shares = 100.0 if success else 0.0
                        self.executed_amount = 15000.0 if success else 0.0
                        self.average_price = 150.0 if success else 0.0
                        self.error_message = "" if success else "Mock execution failure"

                success = random.random() < self.success_rate
                summary = f"Mock execution of {risk_validation.symbol}" if success else ""
                return ExecutionResult(success, summary)

        return MockExecutionOrchestrator()

    def _create_mock_risk_manager(self):
        """Create enhanced mock risk manager with configurable risk profiles"""
        class EnhancedMockRiskManager:
            def __init__(self):
                # Dynamic risk profiles for different market conditions
                self.risk_profiles = {
                    'conservative': {'approval_rate': 0.75, 'position_limit': 0.08},
                    'moderate': {'approval_rate': 0.85, 'position_limit': 0.12},
                    'aggressive': {'approval_rate': 0.95, 'position_limit': 0.18}
                }

                # Current market sentiment affects risk tolerance
                self.current_profile = 'moderate'  # Start with moderate
                self.trade_count_today = 0
                self.last_profile_change = datetime.now()

                # Market volatility simulation
                self.market_volatility = random.uniform(0.15, 0.35)  # 15-35% volatility

            def validate_risk_comprehensive(self, request):
                class RiskValidationResult:
                    def __init__(self, approved: bool, symbol: str, quantity: float, reason: str = ""):
                        self.approved = approved
                        self.symbol = symbol
                        self.quantity = quantity
                        self.rejection_reason = reason

                # Dynamic risk adjustment based on market conditions
                profile = self.risk_profiles[self.current_profile]

                # Adjust approval rate based on:
                # 1. Market volatility
                # 2. Number of trades today
                # 3. Symbol-specific risk
                base_approval = profile['approval_rate']

                # Reduce approval rate if high volatility
                if self.market_volatility > 0.30:
                    base_approval *= 0.9

                # Reduce approval rate if too many trades today
                if self.trade_count_today > 10:
                    base_approval *= 0.8

                # Symbol-specific adjustments
                if hasattr(request, 'symbol'):
                    if request.symbol in ['TSLA', 'NVDA']:  # Higher risk symbols
                        base_approval *= 0.9

                approved = random.random() < base_approval

                if approved:
                    self.trade_count_today += 1
                    reason = ""
                else:
                    reasons = [
                        f"Risk limit exceeded (volatility: {self.market_volatility:.2%})",
                        f"Daily trade limit approaching ({self.trade_count_today}/15)",
                        f"Position concentration risk for {request.symbol}",
                        f"Market conditions unfavorable ({self.current_profile} profile)"
                    ]
                    reason = random.choice(reasons)

                # Occasionally change risk profile based on market conditions
                if random.random() < 0.05:  # 5% chance per decision
                    self._adjust_risk_profile()

                return RiskValidationResult(approved, request.symbol, request.quantity, reason)

            def _adjust_risk_profile(self):
                """Dynamically adjust risk profile based on market conditions"""
                profiles = list(self.risk_profiles.keys())

                # Market-driven profile changes
                if self.market_volatility > 0.30:
                    self.current_profile = 'conservative'
                elif self.market_volatility < 0.20:
                    self.current_profile = 'aggressive'
                else:
                    self.current_profile = 'moderate'

                # Update market volatility
                self.market_volatility *= random.uniform(0.95, 1.05)  # Small volatility changes
                self.market_volatility = max(0.10, min(0.50, self.market_volatility))

                self.last_profile_change = datetime.now()

        return EnhancedMockRiskManager()

    def _create_mock_position_sizer(self):
        """Create mock position sizer with realistic tracking"""
        class MockPositionSizer:
            def __init__(self):
                self.positions = {}
                self.portfolio_value = 100000.0

            def get_current_size(self, symbol: str) -> float:
                return self.positions.get(symbol, 0.0)

            def size_up(self, symbol: str, target_pct: float):
                current_pct = self.get_current_size(symbol)

                class SizeCalculation:
                    def __init__(self, symbol, target_pct, current_pct):
                        self.symbol = symbol
                        self.target_size_pct = target_pct
                        self.size_change_pct = target_pct - current_pct
                        self.needs_trade = abs(self.size_change_pct) > 0.005
                        self.shares_to_trade = abs(self.size_change_pct * 100000 / 150)
                        self.trade_dollar_amount = abs(self.size_change_pct * 100000)

                return SizeCalculation(symbol, target_pct, current_pct)

            def update_position(self, symbol: str, new_pct: float):
                self.positions[symbol] = new_pct

        return MockPositionSizer()

    def _create_mock_sentiment_analyzer(self):
        """Create enhanced mock sentiment analyzer with dynamic thresholds"""
        class EnhancedMockSentimentAnalyzer:
            def __init__(self):
                # Dynamic sentiment thresholds based on market volatility
                self.base_thresholds = {
                    'strong_positive': 0.4,
                    'positive': 0.1,
                    'neutral': 0.05,
                    'negative': -0.1,
                    'strong_negative': -0.4
                }

                # Market volatility affects sentiment interpretation
                self.market_volatility = random.uniform(0.15, 0.35)
                self.sentiment_history = []  # Track recent sentiment for trend analysis

            def analyze_text(self, text: str) -> Dict[str, Any]:
                text_lower = text.lower()

                # Enhanced keyword analysis
                strong_positive_words = ['breakthrough', 'surge', 'exceptional', 'outstanding', 'revolutionary']
                positive_words = ['strong', 'growth', 'profit', 'optimistic', 'bullish', 'momentum']
                neutral_words = ['steady', 'maintain', 'continue', 'update', 'report']
                negative_words = ['challenges', 'decline', 'concerns', 'loss', 'bearish', 'uncertainty']
                strong_negative_words = ['crisis', 'collapse', 'disaster', 'plummet', 'catastrophic']

                # Count sentiment indicators
                strong_pos_count = sum(1 for word in strong_positive_words if word in text_lower)
                pos_count = sum(1 for word in positive_words if word in text_lower)
                neutral_count = sum(1 for word in neutral_words if word in text_lower)
                neg_count = sum(1 for word in negative_words if word in text_lower)
                strong_neg_count = sum(1 for word in strong_negative_words if word in text_lower)

                # Calculate weighted sentiment score
                raw_score = (
                    strong_pos_count * 0.8 +
                    pos_count * 0.3 +
                    neutral_count * 0.0 +
                    neg_count * -0.3 +
                    strong_neg_count * -0.8
                ) / max(1, len(text_lower.split()))

                # Adjust thresholds based on market volatility
                volatility_factor = 1.0 + (self.market_volatility - 0.20) * 2  # Scale volatility impact
                adjusted_thresholds = {
                    k: v * volatility_factor for k, v in self.base_thresholds.items()
                }

                # Determine sentiment category and score
                if raw_score >= adjusted_thresholds['strong_positive']:
                    sentiment = 'very_positive'
                    score = random.uniform(0.6, 0.9)
                elif raw_score >= adjusted_thresholds['positive']:
                    sentiment = 'positive'
                    score = random.uniform(0.2, 0.6)
                elif raw_score <= adjusted_thresholds['strong_negative']:
                    sentiment = 'very_negative'
                    score = random.uniform(-0.9, -0.6)
                elif raw_score <= adjusted_thresholds['negative']:
                    sentiment = 'negative'
                    score = random.uniform(-0.6, -0.2)
                else:
                    sentiment = 'neutral'
                    score = random.uniform(-0.1, 0.1)

                # Add noise based on market volatility
                volatility_noise = random.uniform(-0.1, 0.1) * self.market_volatility
                score += volatility_noise
                score = max(-1.0, min(1.0, score))  # Clamp to valid range

                # Calculate confidence based on text length and clarity
                text_length_factor = min(1.0, len(text_lower.split()) / 20)  # More text = higher confidence
                keyword_density = (strong_pos_count + pos_count + neg_count + strong_neg_count) / max(1, len(text_lower.split()))
                confidence = 0.5 + (text_length_factor * 0.3) + (keyword_density * 0.2)
                confidence = max(0.3, min(0.95, confidence))

                # Store in history for trend analysis
                sentiment_data = {
                    'sentiment': sentiment,
                    'score': score,
                    'confidence': confidence,
                    'volatility': self.market_volatility,
                    'timestamp': datetime.now()
                }

                self.sentiment_history.append(sentiment_data)
                if len(self.sentiment_history) > 50:  # Keep last 50 analyses
                    self.sentiment_history.pop(0)

                # Update market volatility gradually
                self.market_volatility *= random.uniform(0.98, 1.02)
                self.market_volatility = max(0.10, min(0.50, self.market_volatility))

                return sentiment_data

        return EnhancedMockSentimentAnalyzer()

    def _create_mock_news_collector(self):
        """Create mock news collector"""
        class MockNewsCollector:
            def get_symbol_news(self, symbol: str, limit: int = 5) -> List[Dict[str, Any]]:
                articles = []
                news_types = ["earnings", "merger", "expansion", "innovation", "partnership"]
                sentiments = ["positive", "neutral", "negative"]

                for i in range(limit):
                    news_type = random.choice(news_types)
                    sentiment_hint = random.choice(sentiments)

                    if sentiment_hint == "positive":
                        title = f"{symbol} Reports Strong {news_type.title()} Performance"
                        desc = f"Analysts are optimistic about {symbol}'s recent developments."
                    elif sentiment_hint == "negative":
                        title = f"{symbol} Faces Challenges in {news_type.title()}"
                        desc = f"Market concerns about {symbol}'s performance continue."
                    else:
                        title = f"{symbol} Updates on {news_type.title()}"
                        desc = f"Standard business update regarding {symbol}."

                    articles.append({
                        'title': title,
                        'description': desc,
                        'url': f"https://mock.news/{symbol}/{i+1}",
                        'published_at': datetime.now().isoformat(),
                        'source': 'MockNews',
                        'sentiment_hint': sentiment_hint
                    })
                return articles

        return MockNewsCollector()

    def start_trading_system(self):
        """Start the unified trading system"""
        self.running = True
        self.logger.info("=" * 60)
        self.logger.info("Starting Unified Friren Trading System")
        self.logger.info("=" * 60)
        self.logger.info(f"Mode: {self.mode}")
        self.logger.info(f"Components: {self.components_mode}")
        self.logger.info(f"Symbols: {self.symbols}")
        self.logger.info(f"Cycle Interval: {self.cycle_interval}s")

        try:
            while self.running:
                cycle_start = datetime.now()
                cycle_num = self.status.cycles_completed + 1

                self.logger.info(f"=== Enhanced Trading Cycle {cycle_num} ===")

                # Execute trading cycle
                cycle_results = self._execute_trading_cycle()

                # Update status
                self.status.cycles_completed += 1
                self.status.last_cycle_time = datetime.now()
                self.status.successful_trades += cycle_results.get('successful_trades', 0)
                self.status.failed_trades += cycle_results.get('failed_trades', 0)
                self.status.position_additions += cycle_results.get('position_additions', 0)

                # Log cycle summary
                cycle_duration = (datetime.now() - cycle_start).total_seconds()
                self._log_cycle_summary(cycle_results, cycle_duration)

                # Wait for next cycle
                if self.running:
                    self.logger.info(f"Waiting {self.cycle_interval}s for next cycle...")
                    time.sleep(self.cycle_interval)

        except KeyboardInterrupt:
            self.logger.info("Trading system stopped by user")
        except Exception as e:
            self.logger.error(f"Trading system error: {e}")
            self.status.errors.append(f"System error: {e}")
        finally:
            self.stop_trading_system()

    def _execute_trading_cycle(self) -> Dict[str, Any]:
        """Execute a complete trading cycle"""
        cycle_results = {
            'decisions_made': 0,
            'successful_trades': 0,
            'failed_trades': 0,
            'position_additions': 0,
            'decisions': []
        }

        for symbol in self.symbols:
            try:
                # 1. Perform market analysis
                analysis = self._perform_market_analysis(symbol)

                # 2. Generate base recommendation
                base_recommendation = self._generate_base_recommendation(analysis)

                # 3. Apply position-aware logic
                decision = self.position_analyzer.analyze_position_aware_decision(
                    symbol, base_recommendation, analysis.confidence, analysis.sentiment_score
                )

                # 4. Log analysis
                self.logger.info(f"Analysis: Current position: {decision.current_position_pct:.1%}, "
                               f"Recommendation: {base_recommendation}, Sentiment: {analysis.sentiment_score:.2f}")

                # 5. Execute if actionable
                if decision.final_action in ['BUY', 'BUY_MORE', 'SELL']:
                    execution_result = self._execute_enhanced_trade(decision)
                    if execution_result:
                        cycle_results['successful_trades'] += 1
                        if decision.is_position_addition:
                            cycle_results['position_additions'] += 1
                    else:
                        cycle_results['failed_trades'] += 1

                    cycle_results['decisions_made'] += 1

                # 6. Log decision
                self.logger.info(f"{symbol}: {decision.final_action} "
                               f"(confidence: {decision.confidence:.2f}, sentiment: {decision.sentiment_score:.2f})")
                self.logger.info(f"   {decision.reasoning}")

                cycle_results['decisions'].append(decision)

            except Exception as e:
                self.logger.error(f"Error processing {symbol}: {e}")
                self.status.errors.append(f"{symbol} error: {e}")

        # Process positive sentiment queue every few cycles
        if self.status.cycles_completed % 3 == 0:  # Every 3rd cycle
            self._process_positive_sentiment_queue()

        return cycle_results

    def _process_sentiment_for_queue(self, symbol: str, processed_news) -> None:
        """Process news sentiment to identify candidates for watchlist addition"""
        try:
            # Only queue symbols not currently in watchlist and with strong positive sentiment
            if (symbol not in self.symbols and
                symbol not in self.rejected_candidates and
                processed_news.overall_sentiment_score > 0.4 and  # Strong positive sentiment
                processed_news.sentiment_confidence > 0.7 and     # High confidence
                processed_news.news_volume >= 2):                 # Minimum news volume

                # Extract key information for decision making
                key_articles = [article.title for article in processed_news.key_articles[:3]]

                candidate = PositiveSentimentCandidate(
                    symbol=symbol,
                    sentiment_score=processed_news.overall_sentiment_score,
                    confidence=processed_news.sentiment_confidence,
                    news_count=processed_news.news_volume,
                    key_articles=key_articles,
                    market_events=processed_news.market_events,
                    discovery_time=datetime.now(),
                    source="sentiment_discovery",
                    priority_score=self._calculate_candidate_priority(processed_news)
                )

                try:
                    self.positive_sentiment_queue.put_nowait(candidate)
                    self.logger.info(f"Queued positive sentiment candidate: {symbol} "
                                   f"(sentiment: {processed_news.overall_sentiment_score:.2f}, "
                                   f"confidence: {processed_news.sentiment_confidence:.2f})")
                except queue.Full:
                    self.logger.warning("Positive sentiment queue is full, oldest candidates may be lost")

        except Exception as e:
            self.logger.error(f"Error processing sentiment for queue: {e}")

    def _calculate_candidate_priority(self, processed_news) -> float:
        """Calculate priority score for candidate ranking"""
        priority = 0.0

        # Sentiment strength (0-40 points)
        priority += processed_news.overall_sentiment_score * 40

        # Confidence (0-30 points)
        priority += processed_news.sentiment_confidence * 30

        # News volume (0-20 points)
        priority += min(processed_news.news_volume / 10.0, 1.0) * 20

        # Market events bonus (0-10 points)
        if processed_news.market_events:
            priority += len(processed_news.market_events) * 2

        # Data quality (0-10 points)
        priority += processed_news.data_quality_score * 10

        return priority

    def _process_positive_sentiment_queue(self) -> None:
        """Process queued positive sentiment candidates for watchlist evaluation"""
        # Only process queue every few cycles to avoid overwhelming the system
        if not hasattr(self, 'positive_sentiment_queue') or self.positive_sentiment_queue.empty():
            return

        candidates_processed = 0
        max_candidates_per_cycle = 3  # Limit processing to avoid cycle delays

        self.logger.info("Processing positive sentiment queue for watchlist candidates...")

        while not self.positive_sentiment_queue.empty() and candidates_processed < max_candidates_per_cycle:
            try:
                candidate = self.positive_sentiment_queue.get_nowait()
                decision = self._evaluate_watchlist_candidate(candidate)

                self.watchlist_decisions.append(decision)
                candidates_processed += 1

                if decision.decision == 'ADD_TO_WATCHLIST':
                    self._add_to_watchlist(candidate, decision)
                elif decision.decision == 'REJECT':
                    self.rejected_candidates.add(candidate.symbol)

                self.logger.info(f"Watchlist decision for {candidate.symbol}: {decision.decision}")
                self.logger.info(f"   Reasoning: {decision.reasoning}")

            except queue.Empty:
                break
            except Exception as e:
                self.logger.error(f"Error processing candidate from queue: {e}")

        if candidates_processed > 0:
            self.logger.info(f"Processed {candidates_processed} sentiment candidates")

    def _evaluate_watchlist_candidate(self, candidate: PositiveSentimentCandidate) -> WatchlistDecision:
        """Evaluate whether a positive sentiment candidate should be added to watchlist"""
        reasoning_parts = []
        score = 0

        # Sentiment analysis (40% weight)
        if candidate.sentiment_score > 0.6:
            score += 40
            reasoning_parts.append(f"Very positive sentiment ({candidate.sentiment_score:.2f})")
        elif candidate.sentiment_score > 0.4:
            score += 25
            reasoning_parts.append(f"Positive sentiment ({candidate.sentiment_score:.2f})")

        # Confidence analysis (30% weight)
        if candidate.confidence > 0.8:
            score += 30
            reasoning_parts.append(f"High confidence ({candidate.confidence:.2f})")
        elif candidate.confidence > 0.7:
            score += 20
            reasoning_parts.append(f"Good confidence ({candidate.confidence:.2f})")

        # News volume analysis (20% weight)
        if candidate.news_count >= 5:
            score += 20
            reasoning_parts.append(f"Strong news volume ({candidate.news_count} articles)")
        elif candidate.news_count >= 3:
            score += 12
            reasoning_parts.append(f"Moderate news volume ({candidate.news_count} articles)")

        # Market events bonus (10% weight)
        if candidate.market_events:
            score += 10
            reasoning_parts.append(f"Market events: {', '.join(candidate.market_events)}")

        # Decision logic
        if score >= 75:
            decision = 'ADD_TO_WATCHLIST'
            priority = 1  # High priority
        elif score >= 50:
            decision = 'MONITOR'
            priority = 3  # Medium priority
        else:
            decision = 'REJECT'
            priority = 5  # Low priority
            reasoning_parts.append("Insufficient positive indicators")

        reasoning = "; ".join(reasoning_parts)
        confidence = min(score / 100.0, 1.0)

        return WatchlistDecision(
            symbol=candidate.symbol,
            decision=decision,
            reasoning=reasoning,
            confidence=confidence,
            priority=priority,
            next_review_time=datetime.now() + timedelta(hours=24) if decision == 'MONITOR' else None
        )

    def _add_to_watchlist(self, candidate: PositiveSentimentCandidate, decision: WatchlistDecision) -> None:
        """Add approved candidate to watchlist"""
        try:
            # Add to current symbols list for immediate monitoring
            if candidate.symbol not in self.symbols:
                self.symbols.append(candidate.symbol)
                self.logger.info(f"Added {candidate.symbol} to active watchlist")

                # Try to add to portfolio manager's watchlist if available
                try:
                    # Check if we have portfolio manager components
                    if hasattr(self, 'portfolio_manager') and self.portfolio_manager:
                        # Add to portfolio manager's watchlist
                        watchlist_entry = {
                            'symbol': candidate.symbol,
                            'discovery_reason': 'positive_sentiment',
                            'sentiment_score': candidate.sentiment_score,
                            'confidence': candidate.confidence,
                            'priority': decision.priority,
                            'key_articles': candidate.key_articles,
                            'added_time': datetime.now()
                        }

                        # If portfolio manager has watchlist functionality
                        if hasattr(self.portfolio_manager, 'add_to_watchlist'):
                            self.portfolio_manager.add_to_watchlist(candidate.symbol, watchlist_entry)
                            self.logger.info(f"{candidate.symbol} added to portfolio manager watchlist")

                        # If we have database manager, add to database
                        if hasattr(self, 'db_manager') and self.db_manager:
                            try:
                                # This would add to the database watchlist table
                                self.db_manager.add_watchlist_symbol(candidate.symbol, watchlist_entry)
                                self.logger.info(f"{candidate.symbol} added to database watchlist")
                            except Exception as e:
                                self.logger.warning(f"Could not add {candidate.symbol} to database: {e}")

                except Exception as e:
                    self.logger.warning(f"Could not integrate {candidate.symbol} with portfolio manager: {e}")

        except Exception as e:
            self.logger.error(f"Error adding {candidate.symbol} to watchlist: {e}")

    def _perform_market_analysis(self, symbol: str) -> MarketAnalysis:
        """Perform comprehensive market analysis for symbol"""
        try:
            # Check if we have real news collector or mock
            if hasattr(self.news_collector, 'collect_symbol_news'):
                # Real EnhancedNewsCollector - returns ProcessedNewsData
                processed_news = self.news_collector.collect_symbol_news(symbol, hours_back=6, max_articles_per_source=5)

                # Extract data from ProcessedNewsData
                sentiment_score = processed_news.overall_sentiment_score
                confidence = processed_news.sentiment_confidence
                news_count = processed_news.news_volume

                # Convert sentiment score to label
                if sentiment_score > 0.3:
                    sentiment_label = 'positive'
                elif sentiment_score < -0.3:
                    sentiment_label = 'negative'
                else:
                    sentiment_label = 'neutral'

                self.logger.info(f"Real news collected for {symbol}: {news_count} articles, sentiment: {sentiment_score:.2f}")

                # Check for positive sentiment candidates for watchlist expansion
                self._process_sentiment_for_queue(symbol, processed_news)

            else:
                # Mock news collector - returns list of articles
                news_articles = self.news_collector.get_symbol_news(symbol, limit=5)

                # Analyze sentiment
                combined_text = " ".join([
                    article.get('title', '') + " " + article.get('description', '')
                    for article in news_articles
                ])

                if combined_text.strip():
                    sentiment_result = self.sentiment_analyzer.analyze_text(combined_text)
                    sentiment_score = sentiment_result.get('score', 0.0)
                    sentiment_label = sentiment_result.get('sentiment', 'neutral')
                    confidence = sentiment_result.get('confidence', 0.5)
                else:
                    # Default values when no news
                    sentiment_score = random.uniform(-0.1, 0.1)
                    sentiment_label = 'neutral'
                    confidence = random.uniform(0.4, 0.6)

                news_count = len(news_articles)

            return MarketAnalysis(
                symbol=symbol,
                sentiment_score=sentiment_score,
                sentiment_label=sentiment_label,
                confidence=confidence,
                news_count=news_count,
                analysis_time=datetime.now()
            )

        except Exception as e:
            self.logger.error(f"Market analysis error for {symbol}: {e}")
            # Return neutral analysis on error
            return MarketAnalysis(
                symbol=symbol,
                sentiment_score=0.0,
                sentiment_label='neutral',
                confidence=0.5,
                news_count=0,
                analysis_time=datetime.now()
            )

    def _generate_base_recommendation(self, analysis: MarketAnalysis) -> str:
        """Generate base trading recommendation"""
        # Simple recommendation logic based on sentiment and confidence
        if analysis.sentiment_score > 0.3 and analysis.confidence > 0.7:
            return 'BUY'
        elif analysis.sentiment_score < -0.3 and analysis.confidence > 0.7:
            return 'SELL'
        else:
            return 'HOLD'

    def _execute_enhanced_trade(self, decision: EnhancedTradingDecision) -> bool:
        """Enhanced trade execution with detailed logging and timestamp tracking"""
        trade_timestamp = datetime.now()

        try:
            # Create detailed risk validation request
            class RiskValidationRequest:
                def __init__(self, symbol: str, side: str, quantity: float, strategy: str, confidence: float):
                    self.symbol = symbol
                    self.side = side
                    self.quantity = quantity
                    self.strategy = strategy
                    self.confidence = confidence
                    self.timestamp = trade_timestamp

            # Determine trade side and quantity
            if decision.final_action in ['BUY', 'BUY_MORE']:
                side = 'buy'
                position_change = decision.target_position_pct - decision.current_position_pct
            elif decision.final_action in ['SELL', 'PARTIAL_SELL', 'REBALANCE_TRIM']:
                side = 'sell'
                position_change = decision.current_position_pct - decision.target_position_pct
            else:
                return False  # No trade needed

            # Calculate trade details
            dollar_amount = abs(position_change) * self.position_sizer.portfolio_value
            estimated_shares = dollar_amount / 150  # Rough price estimate

            # Create risk validation request
            risk_request = RiskValidationRequest(
                decision.symbol, side, estimated_shares,
                decision.strategy_name, decision.confidence
            )

            # Validate with risk manager
            risk_validation = self.risk_manager.validate_risk_comprehensive(risk_request)

            if risk_validation.approved:
                # Execute trade through execution orchestrator
                execution_result = self.execution_orchestrator.execute_approved_decision(
                    risk_validation, decision.strategy_name, decision.confidence
                )

                if execution_result.success:
                    # Log successful trade with full details
                    execution_time = datetime.now()
                    execution_duration = (execution_time - trade_timestamp).total_seconds()

                    self.logger.info(f"Trade executed successfully: {execution_result.summary}")
                    self.logger.info(f"{decision.symbol}: {decision.final_action} (confidence: {decision.confidence:.2f}, sentiment: {decision.sentiment_score:.2f})")
                    self.logger.info(f"   Trade Details: {side.upper()} ${dollar_amount:,.0f} ({abs(position_change):.1%} position)")
                    self.logger.info(f"   Execution Time: {execution_time.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}")
                    self.logger.info(f"   Processing Duration: {execution_duration:.3f}s")
                    self.logger.info(f"   Strategy: {decision.strategy_name}")
                    self.logger.info(f"   Reasoning: {decision.reasoning}")

                    # Update strategy performance tracking
                    if not hasattr(self.position_analyzer, 'strategy_performance'):
                        self.position_analyzer.strategy_performance = {}

                    strategy = decision.strategy_name
                    if strategy not in self.position_analyzer.strategy_performance:
                        self.position_analyzer.strategy_performance[strategy] = {
                            'trades': 0, 'total_value': 0, 'success_rate': 100
                        }

                    perf = self.position_analyzer.strategy_performance[strategy]
                    perf['trades'] += 1
                    perf['total_value'] += dollar_amount
                    # Success rate calculation would need actual P&L tracking

                    return True
                else:
                    self.logger.warning(f"Trade execution failed for {decision.symbol}: {execution_result.summary}")
                    return False
            else:
                # Log risk rejection with details
                self.logger.warning(f"Trade rejected by risk manager for {decision.symbol}")
                self.logger.warning(f"   Rejection Reason: {risk_validation.rejection_reason}")
                self.logger.warning(f"   Attempted Trade: {side.upper()} ${dollar_amount:,.0f}")
                self.logger.warning(f"   Risk Assessment Time: {trade_timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}")
                return False

        except Exception as e:
            error_time = datetime.now()
            self.logger.error(f"Trade execution error for {decision.symbol}: {str(e)}")
            self.logger.error(f"   Error Time: {error_time.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}")
            self.logger.error(f"   Decision Details: {decision.final_action} from {decision.current_position_pct:.1%} to {decision.target_position_pct:.1%}")
            return False

    def _log_cycle_summary(self, cycle_results: Dict[str, Any], cycle_duration: float):
        """Enhanced cycle summary logging with detailed analytics"""
        cycle_num = self.status.cycles_completed

        # Enhanced logging with more detailed information
        self.logger.info(f"Enhanced Cycle {cycle_num} Summary:")
        self.logger.info(f"   Duration: {cycle_duration:.1f}s")
        self.logger.info(f"   Decisions: {cycle_results.get('decisions_made', 0)}")
        self.logger.info(f"   Successful trades: {cycle_results.get('successful_trades', 0)}")
        self.logger.info(f"   Failed trades: {cycle_results.get('failed_trades', 0)}")
        self.logger.info(f"   Position additions: {cycle_results.get('position_additions', 0)}")

        # Portfolio status with detailed breakdown
        self.logger.info(f"   Current Portfolio Positions:")
        total_invested = 0
        position_count = 0

        for symbol, pct in self.position_sizer.positions.items():
            if pct > 0.001:  # Only show meaningful positions
                dollar_amount = pct * self.position_sizer.portfolio_value
                total_invested += dollar_amount
                position_count += 1
                self.logger.info(f"      {symbol}: {pct:.1%} (${dollar_amount:,.0f})")

        if position_count == 0:
            self.logger.info(f"      No active positions")
        else:
            portfolio_allocation = total_invested / self.position_sizer.portfolio_value
            self.logger.info(f"   Total Invested: ${total_invested:,.0f} ({portfolio_allocation:.1%} of portfolio)")

        # Strategy performance breakdown
        if hasattr(self.position_analyzer, 'strategy_performance') and self.position_analyzer.strategy_performance:
            self.logger.info(f"   Strategy Performance:")
            for strategy, performance in self.position_analyzer.strategy_performance.items():
                trades = performance.get('trades', 0)
                total_value = performance.get('total_value', 0)
                success_rate = performance.get('success_rate', 100)
                self.logger.info(f"      {strategy}: {success_rate:.0f}% ({trades} trades, ${total_value:,.0f})")

        # Risk management insights
        if hasattr(self.risk_manager, 'current_profile'):
            volatility = getattr(self.risk_manager, 'market_volatility', 0.25)
            profile = getattr(self.risk_manager, 'current_profile', 'moderate')
            trades_today = getattr(self.risk_manager, 'trade_count_today', 0)
            self.logger.info(f"   Risk Management: {profile.title()} profile, {volatility:.1%} volatility, {trades_today} trades today")

        # Portfolio health metrics
        if len(self.position_analyzer.portfolio_history) > 1:
            current_value = self.position_analyzer.portfolio_history[-1]['total_value']
            previous_value = self.position_analyzer.portfolio_history[-2]['total_value']
            value_change = current_value - previous_value
            change_pct = (value_change / previous_value) * 100 if previous_value > 0 else 0

            self.logger.info(f"   Portfolio Value Change: ${value_change:+,.0f} ({change_pct:+.2f}%) this cycle")

        # Sentiment discovery queue status
        queue_size = self.positive_sentiment_queue.qsize()
        if queue_size > 0:
            self.logger.info(f"   Positive Sentiment Queue: {queue_size} candidates pending")

        if self.watchlist_decisions:
            recent_additions = [d for d in self.watchlist_decisions
                              if d.decision == 'ADD_TO_WATCHLIST' and
                              (datetime.now() - (d.next_review_time or datetime.now())).days < 1]
            if recent_additions:
                added_symbols = [d.symbol for d in recent_additions]
                self.logger.info(f"   Recent Watchlist Additions: {', '.join(added_symbols)}")

        rejected_count = len(self.rejected_candidates)
        if rejected_count > 0:
            self.logger.info(f"   Rejected Candidates: {rejected_count} symbols")

    def stop_trading_system(self):
        """Stop the trading system gracefully"""
        self.running = False
        self.logger.info("Trading system shutdown complete")

    def _signal_handler(self, signum, frame):
        """Handle system signals"""
        self.logger.info(f"Signal {signum} received, stopping trading system...")
        self.stop_trading_system()

# Enhanced Position-Aware Analyzer (integrated from simplified_real_paper_trading.py)
class EnhancedPositionAwareAnalyzer:
    """Enhanced position-aware analysis with rebalancing and partial exits"""

    def __init__(self, position_sizer, logger: logging.Logger):
        self.position_sizer = position_sizer
        self.logger = logger
        self.portfolio_history = []  # Track portfolio value changes
        self.last_rebalance = datetime.now() - timedelta(days=7)  # Force initial rebalance consideration

        # Enhanced position management settings
        self.rebalance_threshold = 0.05  # 5% deviation triggers rebalancing
        self.partial_exit_threshold = -0.08  # 8% loss triggers partial exit
        self.max_position_age_days = 30  # Consider rebalancing after 30 days
        self.portfolio_target_allocation = 0.75  # Target 75% invested

        # Portfolio performance tracking
        self.position_performance = {}  # Track individual position performance
        self.strategy_performance = {}  # Track strategy-level performance

    def should_rebalance_portfolio(self) -> bool:
        """Determine if portfolio needs rebalancing"""
        days_since_rebalance = (datetime.now() - self.last_rebalance).days

        # Rebalance if:
        # 1. Haven't rebalanced in 7+ days
        # 2. Portfolio allocation drifted significantly
        # 3. Individual positions are far from targets

        if days_since_rebalance >= 7:
            return True

        # Check portfolio drift
        total_allocation = sum(self.position_sizer.positions.values())
        target_drift = abs(total_allocation - self.portfolio_target_allocation)

        if target_drift > self.rebalance_threshold:
            return True

        # Check individual position drift
        for symbol, current_pct in self.position_sizer.positions.items():
            if current_pct > 0.15:  # Any position over 15% triggers rebalance
                return True

        return False

    def calculate_rebalanced_position_size(self, symbol: str, confidence: float, sentiment_score: float) -> float:
        """Calculate position size considering portfolio rebalancing"""
        base_size = self.calculate_initial_position_size(confidence, sentiment_score)

        # Adjust for current portfolio allocation
        current_total = sum(self.position_sizer.positions.values())
        available_allocation = self.portfolio_target_allocation - current_total

        # If portfolio is over-allocated, be more conservative
        if current_total > self.portfolio_target_allocation:
            base_size *= 0.7  # Reduce new position sizes

        # If under-allocated, can be more aggressive
        elif current_total < self.portfolio_target_allocation * 0.5:
            base_size *= 1.2  # Increase position sizes when under-invested

        # Ensure we don't exceed available allocation
        if available_allocation > 0:
            base_size = min(base_size, available_allocation)
        else:
            base_size = min(base_size, 0.03)  # Maximum 3% if over-allocated

        return max(0.02, min(0.15, base_size))  # Keep within 2-15% range

    def check_partial_exit_signals(self, symbol: str, current_pct: float, sentiment_score: float) -> Tuple[bool, float]:
        """Check if position should be partially exited"""
        # Partial exit triggers:
        # 1. Strong negative sentiment shift
        # 2. Position aging with poor performance
        # 3. Portfolio rebalancing needs

        should_partial_exit = False
        exit_percentage = 0.0

        # Strong negative sentiment
        if sentiment_score < -0.6:
            should_partial_exit = True
            exit_percentage = 0.3  # Exit 30% on strong negative sentiment

        # Very strong negative sentiment
        elif sentiment_score < -0.8:
            should_partial_exit = True
            exit_percentage = 0.5  # Exit 50% on very strong negative sentiment

        # Position too large for portfolio balance
        elif current_pct > 0.18:
            should_partial_exit = True
            exit_percentage = (current_pct - 0.15) / current_pct  # Trim to 15%

        # Portfolio rebalancing if over-allocated
        elif sum(self.position_sizer.positions.values()) > 0.85:
            should_partial_exit = True
            exit_percentage = 0.2  # Modest 20% reduction for rebalancing

        return should_partial_exit, exit_percentage

    def calculate_initial_position_size(self, confidence: float, sentiment_score: float) -> float:
        """Enhanced position sizing with market conditions"""
        # Base position size (5-12%)
        base_size = 0.05 + (confidence * 0.07)  # 5% + up to 7% based on confidence

        # Sentiment adjustment (stronger impact)
        if sentiment_score > 0.5:
            base_size *= 1.3  # 30% larger for very positive sentiment
        elif sentiment_score > 0.2:
            base_size *= 1.1  # 10% larger for positive sentiment
        elif sentiment_score < -0.5:
            base_size *= 0.6  # 40% smaller for very negative sentiment
        elif sentiment_score < -0.2:
            base_size *= 0.8  # 20% smaller for negative sentiment

        return max(0.02, min(0.15, base_size))

    def calculate_additional_size(self, current_size: float, sentiment_score: float) -> float:
        """Calculate additional position size for existing positions"""
        # More conservative additional sizing
        if sentiment_score > 0.6:
            additional = 0.04  # 4% addition for very strong sentiment
        elif sentiment_score > 0.3:
            additional = 0.02  # 2% addition for strong sentiment
        else:
            additional = 0.0

        # Ensure total doesn't exceed 18%
        max_total = 0.18
        return min(additional, max_total - current_size)

    def analyze_position_aware_decision(self, symbol: str, base_recommendation: str,
                                      confidence: float, sentiment_score: float) -> EnhancedTradingDecision:
        """Enhanced position-aware decision making with rebalancing and partial exits"""
        current_pct = self.position_sizer.get_current_size(symbol)

        # Track portfolio value for performance monitoring
        current_portfolio_value = sum(
            pct * 100000 for pct in self.position_sizer.positions.values()
        )
        self.portfolio_history.append({
            'timestamp': datetime.now(),
            'total_value': current_portfolio_value,
            'allocation': sum(self.position_sizer.positions.values())
        })

        # Keep only last 100 portfolio snapshots
        if len(self.portfolio_history) > 100:
            self.portfolio_history.pop(0)

        # Check for partial exit signals first
        should_partial_exit, exit_percentage = self.check_partial_exit_signals(
            symbol, current_pct, sentiment_score
        )

        if should_partial_exit and current_pct > 0:
            # Execute partial exit
            new_pct = current_pct * (1 - exit_percentage)
            self.position_sizer.update_position(symbol, new_pct)

            return EnhancedTradingDecision(
                symbol=symbol,
                base_recommendation=base_recommendation,
                final_action='PARTIAL_SELL',
                confidence=confidence,
                sentiment_score=sentiment_score,
                strategy_name=self._determine_strategy('PARTIAL_SELL', sentiment_score),
                reasoning=f"Partial exit: {exit_percentage:.1%} due to sentiment/rebalancing",
                current_position_pct=current_pct,
                target_position_pct=new_pct,
                is_position_addition=False
            )

        # Existing position logic with rebalancing considerations
        if current_pct > 0.001:  # Has position
            return self._handle_existing_position_enhanced(
                symbol, base_recommendation, current_pct, sentiment_score, confidence
            )
        else:  # No position
            return self._handle_new_position_enhanced(
                base_recommendation, sentiment_score, confidence, symbol
            )

    def _handle_existing_position_enhanced(self, symbol: str, recommendation: str, current_pct: float,
                                         sentiment_score: float, confidence: float) -> EnhancedTradingDecision:
        """Enhanced existing position handling with rebalancing"""
        # Check if rebalancing is needed
        needs_rebalance = self.should_rebalance_portfolio()

        if recommendation == "BUY":
            # Consider adding to position
            max_additional = self.calculate_additional_size(current_pct, sentiment_score)

            if max_additional > 0.01:  # Only if meaningful addition
                new_pct = current_pct + max_additional
                action = "BUY_MORE"
                reasoning = f"Adding {max_additional:.1%} to position (sentiment: {sentiment_score:.2f})"
            else:
                new_pct = current_pct
                action = "CONTINUE_HOLDING"
                reasoning = f"Position at target size, continuing to hold"

        elif recommendation == "SELL":
            if sentiment_score < -0.4 or confidence > 0.8:
                # Strong sell signal - exit completely
                new_pct = 0.0
                action = "SELL"
                reasoning = f"Strong sell signal (sentiment: {sentiment_score:.2f}, confidence: {confidence:.2f})"
            else:
                # Weak sell - just hold
                new_pct = current_pct
                action = "CONTINUE_HOLDING"
                reasoning = f"Weak sell signal, maintaining position"
        else:  # HOLD
            if needs_rebalance and current_pct > 0.12:
                # Rebalance large positions
                new_pct = 0.10  # Trim to 10%
                action = "REBALANCE_TRIM"
                reasoning = f"Portfolio rebalancing: trimming from {current_pct:.1%} to {new_pct:.1%}"
            else:
                new_pct = current_pct
                action = "CONTINUE_HOLDING"
                reasoning = f"Holding current {current_pct:.1%} position"

        # Update position
        self.position_sizer.update_position(symbol, new_pct)

        return EnhancedTradingDecision(
            symbol=symbol,
            base_recommendation=recommendation,
            final_action=action,
            confidence=confidence,
            sentiment_score=sentiment_score,
            strategy_name=self._determine_strategy(action, sentiment_score),
            reasoning=reasoning,
            current_position_pct=current_pct,
            target_position_pct=new_pct,
            is_position_addition=(action == "BUY_MORE")
        )

    def _handle_new_position_enhanced(self, recommendation: str, sentiment_score: float,
                                    confidence: float, symbol: str) -> EnhancedTradingDecision:
        """Enhanced new position handling with portfolio allocation consideration"""
        if recommendation == "BUY":
            target_pct = self.calculate_rebalanced_position_size(symbol, confidence, sentiment_score)

            if target_pct > 0.015:  # Only open if meaningful size (>1.5%)
                action = "BUY"
                reasoning = f"Opening {target_pct:.1%} position (confidence: {confidence:.2f}, sentiment: {sentiment_score:.2f})"
                self.position_sizer.update_position(symbol, target_pct)
            else:
                target_pct = 0.0
                action = "HOLD"
                reasoning = f"Position too small or portfolio over-allocated"
        else:
            target_pct = 0.0
            action = "HOLD"
            reasoning = f"No position, recommendation: {recommendation}"

        return EnhancedTradingDecision(
            symbol=symbol,
            base_recommendation=recommendation,
            final_action=action,
            confidence=confidence,
            sentiment_score=sentiment_score,
            strategy_name=self._determine_strategy(action, sentiment_score),
            reasoning=reasoning,
            current_position_pct=0.0,
            target_position_pct=target_pct,
            is_position_addition=False
        )

    def _determine_strategy(self, action: str, sentiment_score: float) -> str:
        """Enhanced strategy determination"""
        if action in ["BUY", "BUY_MORE"]:
            if sentiment_score > 0.5:
                return "sentiment_momentum_strategy"
            elif sentiment_score > 0.2:
                return "value_opportunity_strategy"
            else:
                return "technical_momentum_strategy"
        elif action in ["SELL", "PARTIAL_SELL"]:
            return "risk_management_strategy"
        elif action == "REBALANCE_TRIM":
            return "portfolio_rebalancing_strategy"
        else:
            return "hold_strategy"

def create_unified_orchestrator(symbols: List[str] = None,
                               cycle_interval: int = 60,
                               mode: str = "paper_trading") -> UnifiedOrchestrator:
    """Factory function to create the unified orchestrator"""
    return UnifiedOrchestrator(symbols, cycle_interval, mode)

if __name__ == "__main__":
    # Test the unified orchestrator
    orchestrator = create_unified_orchestrator(
        symbols=['AAPL', 'MSFT', 'GOOGL'],
        cycle_interval=30,
        mode="paper_trading"
    )
    orchestrator.start_trading_system()
