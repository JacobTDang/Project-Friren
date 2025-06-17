"""
portfolio_manager/processes/strategy_analyzer_process.py

Strategy Analyzer Process - Process infrastructure wrapper that uses
the pure analytics/strategy_analyzer.py for business logic.

Handles timing, queues, shared state, and process lifecycle.
"""

import time
from typing import Dict, List, Any, Optional
from datetime import datetime
import logging

from multiprocess_infrastructure.base_process import BaseProcess, ProcessState
from multiprocess_infrastructure.queue_manager import QueueMessage, MessageType, MessagePriority

# Import the pure analytics component
from ..analytics.strategy_analyzer import StrategyAnalyzer


class StrategyAnalyzerProcess(BaseProcess):
    """
    Strategy Analyzer Process Wrapper

    Process Infrastructure Responsibilities:
    - Process lifecycle management (start/stop/restart)
    - Timing and scheduling (every 2-3 minutes)
    - Queue message sending to market decision engine
    - Shared state updates with latest signals
    - Health monitoring and error handling

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
        self.symbols = symbols or ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA', 'NVDA', 'META', 'NFLX']

        # Analytics component (initialized in _initialize)
        self.strategy_analyzer = None

        # Process state tracking
        self.last_analysis_time = None
        self.signals_sent_count = 0

        self.logger.info(f"StrategyAnalyzerProcess configured - interval: {analysis_interval}s, threshold: {confidence_threshold}%")

    def _initialize(self):
        """Initialize process-specific components"""
        self.logger.info("Initializing StrategyAnalyzerProcess...")

        try:
            # Initialize the analytics component
            self.strategy_analyzer = StrategyAnalyzer(
                confidence_threshold=self.confidence_threshold,
                symbols=self.symbols
            )

            # Initialize the analytics component
            self.strategy_analyzer.initialize()

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

            self.logger.info(f"Starting strategy analysis process cycle")
            start_time = time.time()

            # Get current market regime from shared state
            market_regime = self._get_market_regime()

            # Run the analysis using analytics component
            analysis_results = self.strategy_analyzer.analyze_all_strategies(market_regime)

            if not analysis_results['success']:
                self.logger.warning(f"Analysis failed: {analysis_results.get('error', 'Unknown error')}")
                time.sleep(30)
                return

            # Process the results through process infrastructure
            signals_sent = self._process_analysis_results(analysis_results)

            # Update process state
            self.last_analysis_time = datetime.now()
            self.signals_sent_count += signals_sent

            # Update shared state and send health info
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
        """Get current market regime from shared state"""
        try:
            if self.shared_state:
                return self.shared_state.get_market_regime()
            return 'UNKNOWN'
        except Exception as e:
            self.logger.warning(f"Failed to get market regime: {e}")
            return 'UNKNOWN'

    def _process_analysis_results(self, analysis_results: Dict[str, Any]) -> int:
        """Process analysis results and send messages via queue"""
        signals_sent = 0

        try:
            high_confidence_signals = analysis_results.get('high_confidence_signals', [])

            for signal in high_confidence_signals:
                success = self._send_signal_to_queue(signal)
                if success:
                    signals_sent += 1

            self.logger.info(f"Sent {signals_sent}/{len(high_confidence_signals)} high-confidence signals to queue")
            return signals_sent

        except Exception as e:
            self.logger.error(f"Error processing analysis results: {e}")
            return 0

    def _send_signal_to_queue(self, signal: Dict[str, Any]) -> bool:
        """Send trading signal to priority queue"""
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
                    'analysis_cycle': signal['analysis_cycle']
                }
            )

            # Send to queue
            self.priority_queue.put(message, block=False)
            self.logger.info(f"Sent {signal['action']} signal for {signal['symbol']} (confidence: {signal['confidence']:.1f}%)")
            return True

        except Exception as e:
            self.logger.error(f"Failed to send signal to queue: {e}")
            return False

    def _update_shared_state(self, analysis_results: Dict[str, Any]):
        """Update shared state with latest strategy signals"""
        try:
            if not self.shared_state:
                return

            signals = analysis_results.get('signals', [])

            # Update shared state with all signals (not just high confidence)
            for signal in signals:
                self.shared_state.update_strategy_signal(
                    strategy_name=signal['strategy'],
                    symbol=signal['symbol'],
                    signal_data=signal
                )

            self.logger.debug(f"Updated shared state with {len(signals)} signals")

        except Exception as e:
            self.logger.warning(f"Failed to update shared state: {e}")

    def _update_process_status(self, analysis_results: Dict[str, Any], signals_sent: int):
        """Update process status in shared state"""
        try:
            if not self.shared_state:
                return

            stats = analysis_results.get('statistics', {})

            status_data = {
                'last_analysis_time': self.last_analysis_time.isoformat() if self.last_analysis_time else None,
                'analysis_count': stats.get('analysis_count', 0),
                'cycle_time_seconds': analysis_results.get('cycle_time_seconds', 0),
                'symbols_analyzed': analysis_results.get('symbols_analyzed', 0),
                'signals_generated': len(analysis_results.get('signals', [])),
                'high_confidence_signals': len(analysis_results.get('high_confidence_signals', [])),
                'signals_sent_this_cycle': signals_sent,
                'total_signals_sent': self.signals_sent_count,
                'strategy_count': stats.get('strategies_count', 0),
                'high_confidence_rate': stats.get('high_confidence_rate', 0),
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
            'signals_sent_count': self.signals_sent_count,
            'error_count': self.error_count,
            'state': self.state.value
        }

        # Add analytics component info if available
        if self.strategy_analyzer:
            analytics_stats = self.strategy_analyzer._get_current_stats()
            base_info.update({
                'analysis_count': analytics_stats['analysis_count'],
                'total_signals': analytics_stats['total_signals'],
                'high_confidence_signals': analytics_stats['high_confidence_signals'],
                'high_confidence_rate': analytics_stats['high_confidence_rate'],
                'strategy_performance': analytics_stats['strategy_performance']
            })

        return base_info

    # Public interface methods for external control

    def force_analysis(self) -> Dict[str, Any]:
        """Force an immediate analysis cycle (for testing/debugging)"""
        self.logger.info("Forcing immediate analysis cycle")

        if not self.strategy_analyzer:
            return {'success': False, 'error': 'Strategy analyzer not initialized'}

        try:
            market_regime = self._get_market_regime()
            results = self.strategy_analyzer.analyze_all_strategies(market_regime)

            if results['success']:
                signals_sent = self._process_analysis_results(results)
                self._update_shared_state(results)
                results['signals_sent'] = signals_sent

            return results

        except Exception as e:
            self.logger.error(f"Error in forced analysis: {e}")
            return {'success': False, 'error': str(e)}

    def get_latest_signals(self, symbol: Optional[str] = None, action: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get latest signals with optional filtering"""
        if not self.strategy_analyzer:
            return []

        try:
            # This would require storing recent signals in the analytics component
            # For now, return empty list
            return []
        except Exception as e:
            self.logger.error(f"Error getting latest signals: {e}")
            return []
