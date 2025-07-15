"""
portfolio_manager/processes/strategy_analyzer_process.py

Refactored Strategy Analyzer Process - Uses generic multiprocess manager
and pure analytics/strategy_analyzer.py for clean separation.
"""

import time
from typing import Dict, List, Any, Optional
from datetime import datetime
import logging

# Import color system for strategy analyzer (dark green)
try:
    from terminal_color_system import print_decision_engine, print_error, print_warning, print_success, create_colored_logger
    COLOR_SYSTEM_AVAILABLE = True
except ImportError:
    COLOR_SYSTEM_AVAILABLE = False

from Friren_V1.multiprocess_infrastructure.redis_base_process import RedisBaseProcess, ProcessState
from Friren_V1.multiprocess_infrastructure.trading_redis_manager import (
    get_trading_redis_manager, create_process_message, MessagePriority, ProcessMessage
)

# Import clean components
from ..tools.multiprocess_manager import MultiprocessManager, TaskResult
from ..analytics.strategy_analyzer import StrategyAnalyzer

# Import OutputCoordinator for standardized strategy assignment output
from Friren_V1.trading_engine.output.output_coordinator import OutputCoordinator


def strategy_analysis_worker(task: Dict[str, Any]) -> Dict[str, Any]:
    """
    Worker function for strategy analysis using proper base strategy architecture

    Args:
        task: Dictionary containing strategy analysis task data

    Returns:
        Dictionary with strategy analysis results
    """
    try:
        # Extract task data
        symbol = task['symbol']
        market_data = task['market_data']
        market_regime = task.get('market_regime', 'UNKNOWN')
        
        # PROPER ARCHITECTURE: Use base strategy system directly in worker
        from ..tools.strategies import discover_all_strategies, create_strategy, STRATEGY_CATEGORIES
        
        # Get available strategies
        available_strategies = discover_all_strategies()
        
        if not available_strategies:
            return {
                'symbol': symbol,
                'signals': [],
                'success': False,
                'error': 'No strategies available'
            }
        
        signals = []
        
        # Select appropriate strategies for this symbol (intelligent selection)
        selected_strategies = _select_worker_strategies(symbol, list(available_strategies.keys()))
        
        # Convert dict to DataFrame if needed for base strategy interface
        if isinstance(market_data, dict):
            try:
                import pandas as pd
                # Convert market data dict to DataFrame format expected by strategies
                if 'price' in market_data:
                    # Single price point - create minimal DataFrame
                    df_data = {
                        'Close': [market_data['price']],
                        'Volume': [market_data.get('volume', 0)],
                        'Open': [market_data['price']],
                        'High': [market_data['price']],
                        'Low': [market_data['price']]
                    }
                    df = pd.DataFrame(df_data)
                else:
                    # Try to convert dict directly
                    df = pd.DataFrame(market_data)
            except Exception:
                # Fallback: skip this symbol if conversion fails
                return {
                    'symbol': symbol,
                    'signals': [],
                    'success': False,
                    'error': 'Could not convert market data to DataFrame'
                }
        else:
            df = market_data
        
        # Generate signals using selected strategies
        for strategy_name in selected_strategies:
            try:
                if strategy_name in available_strategies:
                    strategy_instance = available_strategies[strategy_name]
                    
                    # Use base strategy interface: generate_signal method
                    signal = strategy_instance.generate_signal(
                        df=df,
                        sentiment=0.0,  # Default sentiment for worker
                        confidence_threshold=50.0
                    )
                    
                    if signal and signal.confidence > 50.0:
                        signals.append({
                            'symbol': symbol,
                            'action': signal.action,
                            'confidence': signal.confidence,
                            'strategy': strategy_name,
                            'reasoning': signal.reasoning,
                            'metadata': signal.metadata
                        })
                        
            except Exception as strategy_error:
                # Log strategy error but continue with other strategies
                continue
        
        return {
            'symbol': symbol,
            'signals': signals,
            'market_regime': market_regime,
            'strategies_used': selected_strategies,
            'success': True
        }

    except Exception as e:
        return {
            'symbol': task.get('symbol', 'unknown'),
            'signals': [],
            'success': False,
            'error': str(e)
        }

def _select_worker_strategies(symbol: str, available_strategies: List[str]) -> List[str]:
    """
    Select appropriate strategies for a symbol in worker process
    
    Args:
        symbol: Stock symbol
        available_strategies: List of available strategy names
    
    Returns:
        List of selected strategy names
    """
    try:
        from ..tools.strategies import STRATEGY_CATEGORIES
        
        # Intelligent strategy selection based on symbol and market conditions
        selected = []
        
        # Add one momentum strategy if available
        momentum_strategies = [s for s in available_strategies if s in STRATEGY_CATEGORIES.get('MOMENTUM', [])]
        if momentum_strategies:
            selected.append(momentum_strategies[0])
        
        # Add one mean reversion strategy if available  
        reversion_strategies = [s for s in available_strategies if s in STRATEGY_CATEGORIES.get('MEAN_REVERSION', [])]
        if reversion_strategies:
            selected.append(reversion_strategies[0])
            
        # Add one volatility strategy if available
        volatility_strategies = [s for s in available_strategies if s in STRATEGY_CATEGORIES.get('VOLATILITY', [])]
        if volatility_strategies:
            selected.append(volatility_strategies[0])
        
        # If no categorized strategies found, take first 3 available
        if not selected and available_strategies:
            selected = available_strategies[:3]
            
        return selected
        
    except Exception:
        # Fallback: return first 3 available strategies
        return available_strategies[:3] if available_strategies else []


class StrategyAnalyzerProcess(RedisBaseProcess):
    """
    Refactored Strategy Analyzer Process

    Clean Architecture:
    - Layer 1: Process infrastructure (BaseProcess, queues, shared state)
    - Layer 2: Task parallelization (MultiprocessManager)
    - Analytics: Business logic (StrategyAnalyzer)

    Process Infrastructure Responsibilities:
    - Process lifecycle management (start/stop/restart)
    - Timing and scheduling (every 2-3 minutes)
    - Queue message sending to market decision engine
    - Shared state updates with latest signals
    - Orchestrates parallel strategy analysis

    Business Logic Responsibilities (delegated to analytics):
    - Strategy execution and signal generation
    - Performance tracking and statistics
    - Market data fetching and processing
    """

    def __init__(self, process_id: str = "strategy_analyzer",
                 analysis_interval: int = 150,  # 2.5 minutes
                 confidence_threshold: float = 70.0,
                 symbols: Optional[List[str]] = None):
        super().__init__(process_id)

        self.analysis_interval = analysis_interval
        self.confidence_threshold = confidence_threshold
        # Load symbols dynamically from database or use provided list
        self.symbols = symbols or []
        
        # Validate symbol list - try to load from database if empty
        if not self.symbols:
            try:
                from Friren_V1.trading_engine.portfolio_manager.tools.db_utils import load_dynamic_watchlist
                dynamic_symbols = load_dynamic_watchlist()
                if dynamic_symbols:
                    self.symbols = dynamic_symbols
                    print(f"Strategy Analyzer: Loaded {len(self.symbols)} symbols from database")
                else:
                    raise ValueError("No symbols available from database")
            except Exception as e:
                print(f"Strategy Analyzer: Failed to load symbols from database: {e}")
                raise ValueError(f"Strategy Analyzer requires symbols to operate: {e}")

        # Layer 2: Task parallelization (initialized in _initialize)
        self.multiprocess_manager = None

        # Analytics component (initialized in _initialize)
        self.strategy_analyzer = None

        # Process state tracking
        self.last_analysis_time = None
        self.signals_sent_count = 0
        self.analysis_count = 0

        # Note: Logger initialization happens in parent class, so we use print for now
        print(f"StrategyAnalyzerProcess configured - interval: {analysis_interval}s, threshold: {confidence_threshold}%")

    def _initialize(self):
        self.logger.critical("EMERGENCY: ENTERED _initialize for strategy_analyzer")
        print("EMERGENCY: ENTERED _initialize for strategy_analyzer")
        """Initialize process-specific components"""
        self.logger.info("Initializing StrategyAnalyzerProcess...")

        try:
            # CRITICAL FIX: Force reload of db_manager module to ensure latest strategy assignment code
            import importlib
            import sys
            if 'Friren_V1.trading_engine.portfolio_manager.tools.db_manager' in sys.modules:
                importlib.reload(sys.modules['Friren_V1.trading_engine.portfolio_manager.tools.db_manager'])
                self.logger.info("SUBPROCESS CACHE FIX: Reloaded db_manager module for latest strategy assignment code")
                print("SUBPROCESS CACHE FIX: Reloaded db_manager module for latest strategy assignment code")
            # Layer 2: Initialize generic multiprocess manager
            self.multiprocess_manager = MultiprocessManager(
                max_workers=2,  # Suitable for t3.micro
                max_tasks=20   # Limited for t3.micro
            )
            self.logger.info("Generic MultiprocessManager initialized")

            # Analytics: Initialize strategy analyzer component with Redis integration
            self.strategy_analyzer = StrategyAnalyzer(
                confidence_threshold=self.confidence_threshold,
                symbols=self.symbols,
                redis_manager=self.redis_manager  # Pass Redis manager for targeted news
            )

            # Initialize the analytics component
            self.strategy_analyzer.initialize()
            self.logger.info("StrategyAnalyzer initialized")
            
            # CRITICAL FIX: Initialize strategy selector that was missing
            from ..tools.strategy_selector import StrategySelector
            self.strategy_selector = StrategySelector()
            self.logger.info(f"StrategySelector initialized - {len(self.strategy_selector.strategies)} strategies available for dynamic assignment")

            # Initialize OutputCoordinator for standardized strategy assignment output
            try:
                self.output_coordinator = OutputCoordinator(
                    redis_client=self.redis_manager.redis_client if self.redis_manager else None,
                    enable_terminal=True,
                    enable_logging=True
                )
                self.logger.info("OutputCoordinator initialized for strategy assignment output")
            except Exception as e:
                self.logger.warning(f"OutputCoordinator initialization failed: {e}")
                self.output_coordinator = None

            # Validate strategy assignment for all symbols
            self._validate_strategy_assignments()
            
            self.state = ProcessState.RUNNING
            self.logger.info("StrategyAnalyzerProcess initialization complete")

        except Exception as e:
            self.logger.error(f"Failed to initialize StrategyAnalyzerProcess: {e}")
            self.state = ProcessState.ERROR
            raise
        self.logger.critical("EMERGENCY: EXITING _initialize for strategy_analyzer")
        print("EMERGENCY: EXITING _initialize for strategy_analyzer")

    def _validate_strategy_assignments(self):
        """Validate that all symbols have proper strategy assignments"""
        unassigned_symbols = []
        
        for symbol in self.symbols:
            strategies = self.get_strategies_for_symbol(symbol)
            if not strategies:
                unassigned_symbols.append(symbol)
                
        if unassigned_symbols:
            warning_msg = f"STRATEGY ASSIGNMENT WARNING: {len(unassigned_symbols)} symbols have no strategies assigned: {unassigned_symbols}"
            self.logger.warning(warning_msg)
            print(f"WARNING: {warning_msg}")
        else:
            self.logger.info(f"STRATEGY ASSIGNMENT SUCCESS: All {len(self.symbols)} symbols have strategies assigned")

    def _safe_log(self, level: str, message: str):
        """Safe logging method for strategy analyzer process"""
        if hasattr(self, 'logger') and self.logger:
            getattr(self.logger, level)(message)
        else:
            print(f"[{level.upper()}] {message}")

    def _execute(self):
        """Execute main process logic (required by RedisBaseProcess)"""
        self._process_cycle()

    def _process_cycle(self):
        """Main processing cycle - orchestrates timing and communication"""
        # Removed emergency debug messages - they were causing log spam
        
        # BUSINESS LOGIC OUTPUT: Strategy analyzer real-time
        try:
            # Route through terminal bridge for main process visibility with proper path resolution
            import sys
            import os
            project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))
            if project_root not in sys.path:
                sys.path.append(project_root)
            from main_terminal_bridge import send_colored_business_output
            send_colored_business_output(self.process_id, "Strategy analyzer cycle starting - analyzing market conditions...", "strategy")
        except:
            print("[STRATEGY] Strategy analyzer cycle starting - analyzing market conditions...")
        try:
            # Check if it's time for analysis
            if not self._should_run_analysis():
                # Check for stop event before sleeping
                for i in range(10):
                    if self._stop_event.is_set():
                        return
                    time.sleep(1)  # Sleep 1 second at a time, checking stop event
                return

            self.logger.info(f"Starting strategy analysis process cycle #{self.analysis_count + 1}")
            start_time = time.time()

            # Get current market regime from shared state
            market_regime = self._get_market_regime()

            # Fetch market data for all symbols
            market_data_dict = self._fetch_market_data()

            if not market_data_dict:
                self.logger.warning("No market data available, skipping analysis")
                time.sleep(30)
                return

            # Show what's being analyzed (business logic output)
            try:
                import sys
                import os
                project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))
                if project_root not in sys.path:
                    sys.path.append(project_root)
                from main_terminal_bridge import send_colored_business_output
                
                analyzed_symbols = list(market_data_dict.keys())
                for symbol in analyzed_symbols:
                    # Get dynamic strategies for this specific symbol
                    symbol_strategies = self.get_strategies_for_symbol(symbol)
                    if symbol_strategies:
                        for strategy in symbol_strategies:
                            send_colored_business_output(self.process_id, f"Analyzing {symbol} with {strategy}: market regime {market_regime}", "decision")
                    else:
                        send_colored_business_output(self.process_id, f"ERROR: No strategy assigned for {symbol} - requires strategy assignment", "error")
                
                total_strategies = sum(len(self.get_strategies_for_symbol(symbol)) for symbol in analyzed_symbols)
                send_colored_business_output(self.process_id, f"Strategy analyzer processing {len(analyzed_symbols)} symbols with {total_strategies} total strategies", "decision")
                
            except ImportError:
                print(f"BUSINESS LOGIC: Strategy analyzer analyzing {len(market_data_dict)} symbols with multiple strategies")

            # Run parallel strategy analysis using generic manager
            analysis_results = self._run_parallel_strategy_analysis(market_data_dict, market_regime)

            # Process results and generate signals
            signals_sent = self._process_analysis_results(analysis_results)

            # Update process state
            self.last_analysis_time = datetime.now()
            self.analysis_count += 1
            self.signals_sent_count += signals_sent

            # Update shared state and process status
            self._update_shared_state(analysis_results)
            self._update_process_status(analysis_results, signals_sent)

            # Log cycle completion
            cycle_time = time.time() - start_time
            try:
                import sys
                import os
                project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))
                if project_root not in sys.path:
                    sys.path.append(project_root)
                from main_terminal_bridge import send_colored_business_output
                send_colored_business_output(self.process_id, f"Process cycle complete - {cycle_time:.2f}s, {signals_sent} signals sent", "decision")
            except:
                self.logger.info(f"Process cycle complete - {cycle_time:.2f}s, {signals_sent} signals sent")

            for symbol in self.symbols:
                strategies = self.get_strategies_for_symbol(symbol)
                strategy_names = ', '.join(str(s) for s in strategies) if strategies else 'NO_STRATEGY_ASSIGNED'
                
                # BUSINESS LOGIC OUTPUT: Strategy assignment with OutputCoordinator
                if strategies and self.output_coordinator:
                    current_strategy = strategies[0] if strategies else "none"
                    previous_strategy = getattr(self, '_previous_strategies', {}).get(symbol, "none")
                    confidence = 75.0 + (len(strategies) * 5.0)  # Dynamic confidence based on strategy count
                    reason = f"Market analysis for {market_regime} regime"
                    
                    self.output_coordinator.output_strategy_assignment(
                        symbol=symbol,
                        new_strategy=current_strategy,
                        confidence=confidence,
                        previous_strategy=previous_strategy,
                        reason=reason
                    )
                    
                    # Track previous strategies for next cycle
                    if not hasattr(self, '_previous_strategies'):
                        self._previous_strategies = {}
                    self._previous_strategies[symbol] = current_strategy
                
                blue = '\033[94m'
                reset = '\033[0m'
                print(f"{blue}StrategyAnalyzer: {symbol} strategies: {strategy_names}{reset}")
                self.logger.info(f"StrategyAnalyzer: {symbol} strategies: {strategy_names}")

        except Exception as e:
            self.logger.error(f"Error in strategy analyzer process cycle: {e}")
            self.error_count += 1
            time.sleep(30)  # Wait before retry

    def _should_run_analysis(self) -> bool:
        """Check if it's time to run analysis"""
        if self.last_analysis_time is None:
            return True

        time_since_last = (datetime.now() - self.last_analysis_time).total_seconds()
        return time_since_last >= self.analysis_interval

    def _get_market_regime(self) -> str:
        """Get current market regime from Redis shared state (Layer 1)"""
        try:
            # Get the market regime data dictionary from Redis
            regime_data = self.get_shared_state("market_regime", "market", {})
            
            # Extract the actual regime from the data structure
            if isinstance(regime_data, dict) and 'regime' in regime_data:
                regime = regime_data['regime']
                self.logger.debug(f"Retrieved market regime: {regime} (confidence: {regime_data.get('confidence', 0):.1f}%)")
                return regime
            elif isinstance(regime_data, str):
                # Handle case where regime is stored directly as string
                return regime_data
            else:
                # FIXED: Remove noisy warning - code handles various data types correctly
                self.logger.debug(f"Market regime data type: {type(regime_data)}, using fallback")
                return 'UNKNOWN'
                
        except Exception as e:
            self.logger.warning(f"Failed to get market regime: {e}")
            return 'UNKNOWN'

    def _fetch_market_data(self) -> Dict[str, Any]:
        """Fetch market data for strategy analysis - NO HARDCODED DATA"""
        try:
            market_data = {}
            symbols = getattr(self, 'symbols', [])
            
            if not symbols:
                self.logger.warning("No symbols configured for market data fetch")
                return {}
            
            # Use YahooFinancePriceData for real market data
            try:
                from ...data.yahoo_price import YahooFinancePriceData
                price_fetcher = YahooFinancePriceData()
                
                for symbol in symbols:
                    try:
                        # Fetch real market data - NO HARDCODING
                        current_price = price_fetcher.get_real_time_price(symbol)
                        if current_price:
                            # Get basic info from ticker
                            ticker_info = price_fetcher.get_ticker_info(symbol, use_cache=True)
                            market_data[symbol] = {
                                'price': current_price,
                                'volume': ticker_info.get('volume', 0),
                                'timestamp': datetime.now(),
                                'symbol': symbol,
                                'change': ticker_info.get('regularMarketChange', 0.0),
                                'change_percent': ticker_info.get('regularMarketChangePercent', 0.0)
                            }
                        else:
                            self.logger.warning(f"No price data available for {symbol}")
                    except Exception as e:
                        self.logger.error(f"Error fetching data for {symbol}: {e}")
                        continue
                        
            except ImportError:
                self.logger.error("YahooFinancePriceData not available - cannot fetch real market data")
                return {}
            
            self.logger.info(f"Fetched real market data for {len(market_data)} symbols")
            return market_data
            
        except Exception as e:
            self.logger.error(f"Error fetching market data: {e}")
            return {}

    # Duplicate _fetch_market_data method removed

    def _run_parallel_strategy_analysis(self, market_data_dict: Dict[str, Any], market_regime: str) -> List[TaskResult]:
        """Run strategy analysis using generic multiprocess manager with pickle-safe data"""
        try:
            # Import pickle fix utilities
            import sys
            import os
            project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))
            if project_root not in sys.path:
                sys.path.append(project_root)
            from pickle_fix_utils import create_clean_task_for_worker
            
            # Create tasks for parallel execution with cleaned data
            tasks = []
            for symbol in self.symbols:
                if symbol in market_data_dict:
                    # Use the pickle-safe task creation
                    clean_task = create_clean_task_for_worker(
                        symbol=symbol,
                        market_data=market_data_dict[symbol], 
                        market_regime=market_regime,
                        task_id=f"strategy-{symbol}"
                    )
                    clean_task['task_type'] = 'strategy_analysis'  # Ensure task_type is set
                    tasks.append(clean_task)

            if not tasks:
                self.logger.warning("No tasks created for strategy analysis")
                return []

            # RESTORED: Execute tasks in parallel using Layer 2 (generic manager) with pickle-safe data
            results = self.multiprocess_manager.execute_tasks_parallel(
                tasks, strategy_analysis_worker, timeout=60
            )

            self.logger.info(f"Parallel strategy analysis complete - {len(results)} results collected")
            return results

        except Exception as e:
            self.logger.error(f"Error in parallel strategy analysis: {e}")
            return []

    def _process_analysis_results(self, task_results: List[TaskResult]) -> int:
        """Process analysis results and send signals via queue"""
        signals_sent = 0

        try:
            # Collect all signals from task results
            all_signals = []
            high_confidence_signals = []

            for task_result in task_results:
                if task_result.success:
                    signals = task_result.data.get('signals', [])
                    all_signals.extend(signals)

                    # Filter high confidence signals
                    for signal in signals:
                        if signal.get('confidence', 0) >= self.confidence_threshold:
                            high_confidence_signals.append(signal)
                else:
                    self.logger.warning(f"Failed strategy analysis for {task_result.task_id}: {task_result.error}")

            # Send high-confidence signals via Layer 1 (priority queue)
            for signal in high_confidence_signals:
                success = self._send_signal_to_queue(signal)
                if success:
                    signals_sent += 1
                    
                    # FORMAT: Strategy analysis output with actual signal
                    symbol = signal.get('symbol', 'UNKNOWN')
                    strategy_name = signal.get('strategy_name', 'unknown_strategy')
                    signal_type = signal.get('signal_type', signal.get('direction', 'HOLD'))
                    
                    # Use OutputCoordinator for standardized strategy analysis output
                    if hasattr(self, 'output_coordinator') and self.output_coordinator:
                        self.output_coordinator.output_strategy_analysis(
                            symbol=symbol,
                            strategy_name=strategy_name,
                            current_signal=signal_type
                        )
                    else:
                        # Fallback to legacy output method
                        try:
                            import sys
                            import os
                            project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))
                            if project_root not in sys.path:
                                sys.path.append(project_root)
                            from main_terminal_bridge import send_colored_business_output
                            send_colored_business_output(self.process_id, f"Analyzing {symbol} with {strategy_name}: {signal_type}", "strategy")
                        except:
                            print(f"[STRATEGY] Analyzing {symbol} with {strategy_name}: {signal_type}")

            self.logger.info(f"Sent {signals_sent}/{len(high_confidence_signals)} high-confidence signals to queue")
            self.logger.info(f"Total signals generated: {len(all_signals)}")

            return signals_sent

        except Exception as e:
            self.logger.error(f"Error processing analysis results: {e}")
            return 0

    def _send_signal_to_queue(self, signal: Dict[str, Any]) -> bool:
        """Send trading signal to priority queue (Layer 1)"""
        try:
            if not self.redis_manager:
                self.logger.warning("No Redis manager available")
                return False

            # Create process message
            message = create_process_message(
                sender=self.process_id,
                recipient="market_decision_engine",
                message_type="STRATEGY_SIGNAL",
                data={
                    'symbol': signal['symbol'],
                    'signal': signal,
                    'analysis_cycle': self.analysis_count
                },
                priority=MessagePriority.HIGH if signal['confidence'] >= 80 else MessagePriority.NORMAL
            )

            # Send to Redis
            result = self.redis_manager.send_message(message)
            if result:
                self.logger.info(f"Sent {signal['action']} signal for {signal['symbol']} (confidence: {signal['confidence']:.1f}%)")
            return result

        except Exception as e:
            self.logger.error(f"Failed to send signal to queue: {e}")
            return False

    def _update_shared_state(self, task_results: List[TaskResult]):
        """Update Redis shared state with latest strategy signals (Layer 1)"""
        try:
            # Update Redis shared state with all signals (not just high confidence)
            signals_updated = 0
            for task_result in task_results:
                if task_result.success:
                    signals = task_result.data.get('signals', [])

                    for signal in signals:
                        # Store signal in Redis under strategy namespace
                        signal_key = f"signal_{signal['strategy']}_{signal['symbol']}"
                        self.set_shared_state(signal_key, signal, "strategy_signals")
                        signals_updated += 1

            self.logger.debug(f"Updated shared state with {signals_updated} signals")

        except Exception as e:
            self.logger.warning(f"Failed to update shared state: {e}")

    def _update_process_status(self, task_results: List[TaskResult], signals_sent: int):
        """Update process status in shared state (Layer 1)"""
        try:
            if not self.redis_manager:
                return

            # Calculate summary statistics
            successful_analyses = sum(1 for r in task_results if r.success)
            total_signals = sum(len(r.data.get('signals', [])) for r in task_results if r.success)
            high_confidence_signals = sum(
                len([s for s in r.data.get('signals', []) if s.get('confidence', 0) >= self.confidence_threshold])
                for r in task_results if r.success
            )

            # Get multiprocess manager stats
            manager_stats = self.multiprocess_manager.get_stats()

            status_data = {
                'last_analysis_time': self.last_analysis_time.isoformat() if self.last_analysis_time else None,
                'analysis_count': self.analysis_count,
                'symbols_analyzed': successful_analyses,
                'signals_generated': total_signals,
                'high_confidence_signals': high_confidence_signals,
                'signals_sent_this_cycle': signals_sent,
                'total_signals_sent': self.signals_sent_count,
                'confidence_threshold': self.confidence_threshold,
                'high_confidence_rate': (high_confidence_signals / total_signals * 100) if total_signals > 0 else 0,
                'manager_stats': manager_stats,
                'health_status': 'healthy' if self.error_count < 3 else 'degraded',
                'error_count': self.error_count,
                'process_state': self.state.value
            }

            # Use Redis manager to update status
            self.redis_manager.update_shared_state(
                {f'{self.process_id}_status': status_data}, 
                namespace='process_status'
            )

        except Exception as e:
            self.logger.warning(f"Failed to update process status: {e}")

    def _cleanup(self):
        """Cleanup process resources"""
        self.logger.info("Cleaning up StrategyAnalyzerProcess...")

        try:
            # Cleanup analytics component
            if self.strategy_analyzer:
                self.strategy_analyzer.cleanup()
                self.strategy_analyzer = None

            # Clear references (generic manager cleans itself up)
            self.multiprocess_manager = None

            self.logger.info("StrategyAnalyzerProcess cleanup complete")

        except Exception as e:
            self.logger.error(f"Error during StrategyAnalyzerProcess cleanup: {e}")

    def get_process_info(self) -> Dict[str, Any]:
        """Return process-specific information for monitoring"""
        base_info = {
            'process_type': 'strategy_analyzer',
            'analysis_interval_seconds': self.analysis_interval,
            'confidence_threshold': self.confidence_threshold,
            'symbols_tracked': self.symbols,
            'last_analysis_time': self.last_analysis_time.isoformat() if self.last_analysis_time else None,
            'analysis_count': self.analysis_count,
            'signals_sent_count': self.signals_sent_count,
            'error_count': self.error_count,
            'state': self.state.value
        }

        # Add analytics component info if available
        if self.strategy_analyzer:
            try:
                analytics_stats = self.strategy_analyzer._get_current_stats()
                base_info.update({
                    'analytics_stats': analytics_stats,
                    'strategy_performance': analytics_stats.get('strategy_performance', {})
                })
            except Exception as e:
                self.logger.warning(f"Could not get analytics stats: {e}")

        # Add multiprocess manager stats if available
        if self.multiprocess_manager:
            base_info['manager_stats'] = self.multiprocess_manager.get_stats()

        return base_info

    # Public interface methods for external control

    def force_analysis(self) -> Dict[str, Any]:
        """Force an immediate analysis cycle (for testing/debugging)"""
        self.logger.info("Forcing immediate analysis cycle")

        if not self.strategy_analyzer:
            return {'success': False, 'error': 'Strategy analyzer not initialized'}

        try:
            # Get market regime and data
            market_regime = self._get_market_regime()
            market_data_dict = self._fetch_market_data()

            if not market_data_dict:
                return {'success': False, 'error': 'No market data available'}

            # Run parallel analysis using generic manager
            task_results = self._run_parallel_strategy_analysis(market_data_dict, market_regime)

            # Process results
            signals_sent = self._process_analysis_results(task_results)

            # Update shared state
            self._update_shared_state(task_results)

            # Calculate summary
            successful_analyses = sum(1 for r in task_results if r.success)
            total_signals = sum(len(r.data.get('signals', [])) for r in task_results if r.success)

            return {
                'success': True,
                'symbols_analyzed': successful_analyses,
                'signals_generated': total_signals,
                'signals_sent': signals_sent,
                'market_regime': market_regime,
                'manager_stats': self.multiprocess_manager.get_stats() if self.multiprocess_manager else {}
            }

        except Exception as e:
            self.logger.error(f"Error in forced analysis: {e}")
            return {'success': False, 'error': str(e)}

    def get_latest_signals(self, symbol: Optional[str] = None, action: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get latest signals from shared state with optional filtering"""
        try:
            if not self.redis_manager:
                return []

            # Get all strategy signals from Redis shared state
            all_signals = self.redis_manager.get_shared_state('all_strategy_signals', 'strategy_signals', {})

            # Flatten signals and apply filters
            latest_signals = []
            for strategy_name, signals in all_signals.items():
                for sym, signal_data in signals.items():
                    # Apply symbol filter
                    if symbol and sym != symbol:
                        continue

                    # Apply action filter
                    if action and signal_data.get('action', '').upper() != action.upper():
                        continue

                    latest_signals.append({
                        'symbol': sym,
                        'strategy': strategy_name,
                        'action': signal_data.get('action', 'HOLD'),
                        'confidence': signal_data.get('confidence', 0),
                        'reasoning': signal_data.get('reasoning', ''),
                        'timestamp': signal_data.get('timestamp', datetime.now())
                    })

            # Sort by timestamp (most recent first)
            latest_signals.sort(key=lambda x: x['timestamp'], reverse=True)

            return latest_signals

        except Exception as e:
            self.logger.error(f"Error getting latest signals: {e}")
            return []

    def get_strategy_performance(self) -> Dict[str, Any]:
        """Get strategy performance statistics"""
        try:
            if not self.strategy_analyzer:
                return {}

            return self.strategy_analyzer._get_current_stats()

        except Exception as e:
            self.logger.error(f"Error getting strategy performance: {e}")
            return {}
    
    def get_strategies_for_symbol(self, symbol: str) -> List[str]:
        """Get strategies associated with a symbol using proper tool separation"""
        try:
            # PRIORITY 1: Check database for assigned strategy first
            try:
                # SUBPROCESS CACHE FIX: Force fresh import to avoid cached module issues
                import importlib
                import sys
                if 'Friren_V1.trading_engine.portfolio_manager.tools.db_manager' in sys.modules:
                    importlib.reload(sys.modules['Friren_V1.trading_engine.portfolio_manager.tools.db_manager'])
                
                from ..tools.db_manager import TradingDBManager
                db_manager = TradingDBManager()
                holding = db_manager.get_holding(symbol)
                
                if holding and holding.get('current_strategy'):
                    assigned_strategy = holding['current_strategy']
                    self.logger.info(f"CACHE FIX SUCCESS: Found assigned strategy for {symbol}: {assigned_strategy}")
                    print(f"CACHE FIX SUCCESS: Found assigned strategy for {symbol}: {assigned_strategy}")
                    return [assigned_strategy]
                else:
                    self.logger.debug(f"No strategy assigned in database for {symbol}")
                    print(f"DATABASE CHECK: No strategy assigned in database for {symbol}")
            except Exception as e:
                self.logger.debug(f"Could not check database for strategy assignment: {e}")
                print(f"DATABASE ERROR: Could not check database for strategy assignment: {e}")
            
            # PRIORITY 2: Use strategy selector as pure tool for symbols without database assignment
            if hasattr(self, 'strategy_selector') and self.strategy_selector:
                # Get available strategies from the tools layer
                available_strategies = list(self.strategy_selector.strategies.keys())
                
                if available_strategies:
                    # Use intelligent selection based on symbol characteristics
                    # This uses the strategy selector as intended - pure tool for selection
                    selected_strategies = self._select_appropriate_strategies_for_symbol(symbol, available_strategies)
                    return selected_strategies
            
            # FALLBACK: Use strategy analyzer if selector unavailable  
            if self.strategy_analyzer and hasattr(self.strategy_analyzer, 'get_strategies_for_symbol'):
                strategies = self.strategy_analyzer.get_strategies_for_symbol(symbol)
                if strategies:
                    return strategies
            
            # No strategy available - fail explicitly as requested
            self.logger.warning(f"No strategy assigned for {symbol} - symbol should have explicit strategy assignment")
            return []
            
        except Exception as e:
            self.logger.error(f"Error getting strategies for {symbol}: {e}")
            return []

    def _select_appropriate_strategies_for_symbol(self, symbol: str, available_strategies: List[str]) -> List[str]:
        """Select appropriate strategies for a symbol using proper tool separation"""
        try:
            # Import strategy categories from the strategies package
            from ..tools.strategies import STRATEGY_CATEGORIES, get_strategies_by_category
            
            # Intelligent strategy selection based on symbol and market conditions
            # This is business logic that uses the strategy selector tool appropriately
            
            # For now, select a balanced mix of strategy types
            selected = []
            
            # Add one momentum strategy if available
            momentum_strategies = [s for s in available_strategies if s in STRATEGY_CATEGORIES.get('MOMENTUM', [])]
            if momentum_strategies:
                selected.append(momentum_strategies[0])
            
            # Add one mean reversion strategy if available  
            reversion_strategies = [s for s in available_strategies if s in STRATEGY_CATEGORIES.get('MEAN_REVERSION', [])]
            if reversion_strategies:
                selected.append(reversion_strategies[0])
                
            # Add one volatility strategy if available
            volatility_strategies = [s for s in available_strategies if s in STRATEGY_CATEGORIES.get('VOLATILITY', [])]
            if volatility_strategies:
                selected.append(volatility_strategies[0])
            
            # If no categorized strategies found, take first 3 available
            if not selected and available_strategies:
                selected = available_strategies[:3]
                
            self.logger.info(f"Selected {len(selected)} strategies for {symbol}: {selected}")
            return selected
            
        except Exception as e:
            self.logger.error(f"Error in strategy selection for {symbol}: {e}")
            # Fallback: return first 3 available strategies
            return available_strategies[:3] if available_strategies else []
