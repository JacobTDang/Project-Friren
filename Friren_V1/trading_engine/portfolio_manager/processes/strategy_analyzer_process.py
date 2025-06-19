"""
portfolio_manager/processes/strategy_analyzer_process.py

Refactored Strategy Analyzer Process - Uses generic multiprocess manager
and pure analytics/strategy_analyzer.py for clean separation.
"""

import time
from typing import Dict, List, Any, Optional
from datetime import datetime
import logging

from multiprocess_infrastructure.base_process import BaseProcess, ProcessState
from multiprocess_infrastructure.queue_manager import QueueMessage, MessageType, MessagePriority

# Import clean components
from ..tools.multiprocess_manager import MultiprocessManager, TaskResult
from ..analytics.strategy_analyzer import StrategyAnalyzer


def strategy_analysis_worker(task: Dict[str, Any]) -> Dict[str, Any]:
    """
    Worker function for strategy analysis

    Args:
        task: Dictionary containing strategy analysis task data

    Returns:
        Dictionary with strategy analysis results
    """
    try:
        # Initialize strategy analyzer in worker
        analyzer = StrategyAnalyzer()
        analyzer.initialize()

        # Extract task data
        symbol = task['symbol']
        market_data = task['market_data']
        market_regime = task.get('market_regime', 'UNKNOWN')

        # Get signals for this symbol using the analytics component
        signals = analyzer.get_strategy_signals(symbol, {symbol: market_data}, market_regime)

        return {
            'symbol': symbol,
            'signals': signals,
            'market_regime': market_regime,
            'success': True
        }

    except Exception as e:
        return {
            'symbol': task.get('symbol', 'unknown'),
            'signals': [],
            'success': False,
            'error': str(e)
        }


class StrategyAnalyzerProcess(BaseProcess):
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
        self.symbols = symbols or ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA']  # Reduced for Phase 1

        # Layer 2: Task parallelization (initialized in _initialize)
        self.multiprocess_manager = None

        # Analytics component (initialized in _initialize)
        self.strategy_analyzer = None

        # Process state tracking
        self.last_analysis_time = None
        self.signals_sent_count = 0
        self.analysis_count = 0

        self.logger.info(f"StrategyAnalyzerProcess configured - interval: {analysis_interval}s, threshold: {confidence_threshold}%")

    def _initialize(self):
        """Initialize process-specific components"""
        self.logger.info("Initializing StrategyAnalyzerProcess...")

        try:
            # Layer 2: Initialize generic multiprocess manager
            self.multiprocess_manager = MultiprocessManager(
                max_workers=2,  # Suitable for t3.micro
                max_tasks=20   # Limited for t3.micro
            )
            self.logger.info("Generic MultiprocessManager initialized")

            # Analytics: Initialize strategy analyzer component
            self.strategy_analyzer = StrategyAnalyzer(
                confidence_threshold=self.confidence_threshold,
                symbols=self.symbols
            )

            # Initialize the analytics component
            self.strategy_analyzer.initialize()
            self.logger.info("StrategyAnalyzer initialized")

            self.state = ProcessState.RUNNING
            self.logger.info("StrategyAnalyzerProcess initialization complete")

        except Exception as e:
            self.logger.error(f"Failed to initialize StrategyAnalyzerProcess: {e}")
            self.state = ProcessState.ERROR
            raise

    def _process_cycle(self):
        """Main processing cycle - orchestrates timing and communication"""
        try:
            # Check if it's time for analysis
            if not self._should_run_analysis():
                time.sleep(10)  # Short sleep if not time yet
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
            self.logger.info(f"Process cycle complete - {cycle_time:.2f}s, {signals_sent} signals sent")

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
        """Get current market regime from shared state (Layer 1)"""
        try:
            if self.shared_state:
                return self.shared_state.get_market_regime()
            return 'UNKNOWN'
        except Exception as e:
            self.logger.warning(f"Failed to get market regime: {e}")
            return 'UNKNOWN'

    def _fetch_market_data(self) -> Dict[str, Any]:
        """Fetch market data using analytics component"""
        try:
            # Use the analytics component's data fetching capability
            # This delegates to the existing StrategyAnalyzer which has the data fetcher
            return self.strategy_analyzer._fetch_market_data()

        except Exception as e:
            self.logger.error(f"Error fetching market data: {e}")
            return {}

    def _run_parallel_strategy_analysis(self, market_data_dict: Dict[str, Any], market_regime: str) -> List[TaskResult]:
        """Run strategy analysis using generic multiprocess manager"""
        try:
            # Create tasks for parallel execution
            tasks = []
            for symbol in self.symbols:
                if symbol in market_data_dict:
                    tasks.append({
                        'task_id': f"strategy-{symbol}",
                        'task_type': 'strategy_analysis',
                        'symbol': symbol,
                        'market_data': market_data_dict[symbol],
                        'market_regime': market_regime
                    })

            if not tasks:
                self.logger.warning("No tasks created for strategy analysis")
                return []

            # Execute tasks in parallel using Layer 2 (generic manager)
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

            self.logger.info(f"Sent {signals_sent}/{len(high_confidence_signals)} high-confidence signals to queue")
            self.logger.info(f"Total signals generated: {len(all_signals)}")

            return signals_sent

        except Exception as e:
            self.logger.error(f"Error processing analysis results: {e}")
            return 0

    def _send_signal_to_queue(self, signal: Dict[str, Any]) -> bool:
        """Send trading signal to priority queue (Layer 1)"""
        try:
            if not self.priority_queue:
                self.logger.warning("No priority queue available")
                return False

            # Create queue message
            message = QueueMessage(
                message_type=MessageType.STRATEGY_SIGNAL,
                priority=MessagePriority.HIGH if signal['confidence'] >= 80 else MessagePriority.NORMAL,
                sender_id=self.process_id,
                recipient_id="market_decision_engine",
                payload={
                    'symbol': signal['symbol'],
                    'signal': signal,
                    'analysis_cycle': self.analysis_count
                }
            )

            # Send to queue
            self.priority_queue.put(message, block=False)
            self.logger.info(f"Sent {signal['action']} signal for {signal['symbol']} (confidence: {signal['confidence']:.1f}%)")
            return True

        except Exception as e:
            self.logger.error(f"Failed to send signal to queue: {e}")
            return False

    def _update_shared_state(self, task_results: List[TaskResult]):
        """Update shared state with latest strategy signals (Layer 1)"""
        try:
            if not self.shared_state:
                return

            # Update shared state with all signals (not just high confidence)
            signals_updated = 0
            for task_result in task_results:
                if task_result.success:
                    signals = task_result.data.get('signals', [])

                    for signal in signals:
                        self.shared_state.update_strategy_signal(
                            strategy_name=signal['strategy'],
                            symbol=signal['symbol'],
                            signal_data=signal
                        )
                        signals_updated += 1

            self.logger.debug(f"Updated shared state with {signals_updated} signals")

        except Exception as e:
            self.logger.warning(f"Failed to update shared state: {e}")

    def _update_process_status(self, task_results: List[TaskResult], signals_sent: int):
        """Update process status in shared state (Layer 1)"""
        try:
            if not self.shared_state:
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

            self.shared_state.update_system_status(self.process_id, status_data)

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
            if not self.shared_state:
                return []

            # Get all strategy signals from shared state
            all_signals = self.shared_state.get_all_strategy_signals()

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
