"""
trading_engine/portfolio_manager/processes/strategy_analyzer.py

Strategy Analyzer Process - Wraps existing MultiprocessManager and strategies
in the new multiprocess infrastructure.

Runs all 7 strategies in parallel every 2-3 minutes and sends high-confidence
signals to the priority queue.
"""

import time
import threading
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import logging

from multiprocess_infrastructure.base_process import BaseProcess, ProcessState
from multiprocess_infrastructure.queue_manager import QueueMessage, MessageType, MessagePriority

# Import existing components
from multiprocess_manager import MultiprocessManager
from strategies import discover_all_strategies, AVAILABLE_STRATEGIES
from data.yahoo import StockDataFetcher


class StrategyAnalyzer(BaseProcess):
    """
    Strategy Analysis Process

    Responsibilities:
    - Run all 7 strategies in parallel using existing MultiprocessManager
    - Filter signals by confidence threshold
    - Send high-confidence signals to market decision engine
    - Update shared state with latest strategy signals
    - Monitor strategy performance and health
    """

    def __init__(self, process_id: str = "strategy_analyzer",
                 analysis_interval: int = 150,  # 2.5 minutes
                 confidence_threshold: float = 70.0):
        super().__init__(process_id)

        self.analysis_interval = analysis_interval
        self.confidence_threshold = confidence_threshold

        # Components (initialized in _initialize)
        self.multiprocess_manager = None
        self.strategies = None
        self.data_fetcher = None

        # State tracking
        self.symbols = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA', 'NVDA', 'META', 'NFLX']
        self.last_analysis_time = None
        self.analysis_count = 0
        self.signal_stats = {
            'total_signals': 0,
            'high_confidence_signals': 0,
            'signals_sent': 0,
            'strategy_performance': {}
        }

        self.logger.info(f"StrategyAnalyzer configured - interval: {analysis_interval}s, threshold: {confidence_threshold}%")

    def _initialize(self):
        """Initialize strategy analyzer components"""
        self.logger.info("Initializing StrategyAnalyzer...")

        try:
            # Initialize existing multiprocess manager
            self.multiprocess_manager = MultiprocessManager()
            self.logger.info(f"MultiprocessManager initialized - workers: {self.multiprocess_manager.num_workers}")

            # Discover and initialize all strategies
            self.strategies = discover_all_strategies()
            self.logger.info(f"Loaded {len(self.strategies)} strategies: {list(self.strategies.keys())}")

            # Initialize data fetcher
            self.data_fetcher = StockDataFetcher()
            self.logger.info("StockDataFetcher initialized")

            # Initialize strategy performance tracking
            for strategy_name in self.strategies.keys():
                self.signal_stats['strategy_performance'][strategy_name] = {
                    'signals_generated': 0,
                    'high_confidence_signals': 0,
                    'avg_confidence': 0.0,
                    'last_signal_time': None
                }

            self.state = ProcessState.RUNNING
            self.logger.info("StrategyAnalyzer initialization complete")

        except Exception as e:
            self.logger.error(f"Failed to initialize StrategyAnalyzer: {e}")
            self.state = ProcessState.ERROR
            raise

    def _process_cycle(self):
        """Main processing cycle - run strategy analysis"""
        try:
            # Check if it's time for analysis
            if not self._should_run_analysis():
                time.sleep(10)  # Short sleep if not time yet
                return

            self.logger.info(f"Starting strategy analysis cycle #{self.analysis_count + 1}")
            start_time = time.time()

            # Fetch market data for all symbols
            market_data = self._fetch_market_data()

            if not market_data:
                self.logger.warning("No market data available, skipping analysis")
                time.sleep(30)
                return

            # Run strategy analysis using existing multiprocess manager
            analysis_results = self._run_strategy_analysis(market_data)

            # Process results and generate signals
            signals_generated = self._process_analysis_results(analysis_results)

            # Update statistics
            self.analysis_count += 1
            self.last_analysis_time = datetime.now()

            # Log cycle completion
            cycle_time = time.time() - start_time
            self.logger.info(f"Analysis cycle complete - {cycle_time:.2f}s, {signals_generated} signals generated")

            # Update shared state with timing info
            self._update_process_status(cycle_time, signals_generated)

        except Exception as e:
            self.logger.error(f"Error in strategy analysis cycle: {e}")
            self.error_count += 1
            time.sleep(30)  # Wait before retry

    def _should_run_analysis(self) -> bool:
        """Check if it's time to run analysis"""
        if self.last_analysis_time is None:
            return True

        time_since_last = (datetime.now() - self.last_analysis_time).total_seconds()
        return time_since_last >= self.analysis_interval

    def _fetch_market_data(self) -> Dict[str, Any]:
        """Fetch market data for all symbols"""
        try:
            market_data_dict = {}

            for symbol in self.symbols:
                try:
                    # Get recent data (last 100 days for technical analysis)
                    df = self.data_fetcher.extract_data(symbol, period="100d", interval="1d")

                    if not df.empty:
                        market_data_dict[symbol] = df
                        self.logger.debug(f"Fetched {len(df)} days of data for {symbol}")
                    else:
                        self.logger.warning(f"No data received for {symbol}")

                except Exception as e:
                    self.logger.warning(f"Failed to fetch data for {symbol}: {e}")
                    continue

            self.logger.info(f"Market data fetched for {len(market_data_dict)} symbols")
            return market_data_dict

        except Exception as e:
            self.logger.error(f"Error fetching market data: {e}")
            return {}

    def _run_strategy_analysis(self, market_data_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Run strategy analysis using existing multiprocess manager"""
        try:
            # Use existing multiprocess manager for parallel analysis
            results = self.multiprocess_manager.analyze_symbols_parallel(
                symbols=list(market_data_dict.keys()),
                market_data_dict=market_data_dict
            )

            self.logger.info(f"Multiprocess analysis complete - {results.get('success_rate', 0):.1f}% success")
            return results

        except Exception as e:
            self.logger.error(f"Error in multiprocess analysis: {e}")
            return {'results': {'strategy': {}}}

    def _process_analysis_results(self, analysis_results: Dict[str, Any]) -> int:
        """Process analysis results and generate trading signals"""
        signals_generated = 0

        try:
            strategy_results = analysis_results.get('results', {}).get('strategy', {})

            for symbol, result_data in strategy_results.items():
                if not result_data:
                    continue

                # Extract strategy signal information
                strategy_name = result_data.get('strategy', 'unknown')
                confidence = result_data.get('confidence', 0)
                rsi = result_data.get('rsi', 50)

                # Update strategy performance tracking
                self._update_strategy_stats(strategy_name, confidence)

                # Generate signal based on strategy result
                signal = self._generate_signal_from_result(symbol, result_data)

                if signal:
                    # Update shared state with signal
                    self._update_shared_state_signal(symbol, strategy_name, signal)

                    # Send high-confidence signals to decision engine
                    if confidence >= self.confidence_threshold:
                        success = self._send_signal_to_queue(symbol, signal)
                        if success:
                            signals_generated += 1
                            self.signal_stats['signals_sent'] += 1

                    self.signal_stats['total_signals'] += 1
                    if confidence >= self.confidence_threshold:
                        self.signal_stats['high_confidence_signals'] += 1

            return signals_generated

        except Exception as e:
            self.logger.error(f"Error processing analysis results: {e}")
            return 0

    def _generate_signal_from_result(self, symbol: str, result_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Generate standardized signal from strategy result"""
        try:
            strategy_name = result_data.get('strategy', 'unknown')
            confidence = result_data.get('confidence', 0)
            rsi = result_data.get('rsi', 50)

            # Determine action based on strategy and RSI
            if strategy_name == 'mean_reversion':
                if rsi < 30:
                    action = 'BUY'
                    reasoning = f"RSI oversold at {rsi:.1f}"
                elif rsi > 70:
                    action = 'SELL'
                    reasoning = f"RSI overbought at {rsi:.1f}"
                else:
                    action = 'HOLD'
                    reasoning = f"RSI neutral at {rsi:.1f}"
            elif strategy_name == 'momentum':
                if rsi > 60:
                    action = 'BUY'
                    reasoning = f"Momentum strong, RSI at {rsi:.1f}"
                elif rsi < 40:
                    action = 'SELL'
                    reasoning = f"Momentum weak, RSI at {rsi:.1f}"
                else:
                    action = 'HOLD'
                    reasoning = f"Momentum neutral, RSI at {rsi:.1f}"
            else:
                action = 'HOLD'
                reasoning = f"Strategy {strategy_name} suggests hold"

            # Get current market regime for context
            market_regime = self.shared_state.get_market_regime() if self.shared_state else 'UNKNOWN'

            signal = {
                'action': action,
                'confidence': confidence,
                'reasoning': reasoning,
                'strategy': strategy_name,
                'rsi': rsi,
                'market_regime': market_regime,
                'timestamp': datetime.now(),
                'metadata': {
                    'analysis_cycle': self.analysis_count,
                    'symbol': symbol
                }
            }

            return signal

        except Exception as e:
            self.logger.error(f"Error generating signal for {symbol}: {e}")
            return None

    def _update_shared_state_signal(self, symbol: str, strategy_name: str, signal: Dict[str, Any]):
        """Update shared state with latest signal"""
        try:
            if self.shared_state:
                self.shared_state.update_strategy_signal(strategy_name, symbol, signal)
                self.logger.debug(f"Updated shared state signal for {symbol} from {strategy_name}")
        except Exception as e:
            self.logger.warning(f"Failed to update shared state: {e}")

    def _send_signal_to_queue(self, symbol: str, signal: Dict[str, Any]) -> bool:
        """Send high-confidence signal to priority queue"""
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
                    'symbol': symbol,
                    'signal': signal,
                    'analysis_cycle': self.analysis_count
                }
            )

            # Send to queue
            self.priority_queue.put(message, block=False)
            self.logger.info(f"Sent {signal['action']} signal for {symbol} (confidence: {signal['confidence']:.1f}%)")
            return True

        except Exception as e:
            self.logger.error(f"Failed to send signal to queue: {e}")
            return False

    def _update_strategy_stats(self, strategy_name: str, confidence: float):
        """Update strategy performance statistics"""
        if strategy_name in self.signal_stats['strategy_performance']:
            stats = self.signal_stats['strategy_performance'][strategy_name]
            stats['signals_generated'] += 1

            if confidence >= self.confidence_threshold:
                stats['high_confidence_signals'] += 1

            # Update running average confidence
            current_avg = stats['avg_confidence']
            signal_count = stats['signals_generated']
            stats['avg_confidence'] = (current_avg * (signal_count - 1) + confidence) / signal_count

            stats['last_signal_time'] = datetime.now()

    def _update_process_status(self, cycle_time: float, signals_generated: int):
        """Update process status in shared state"""
        try:
            if self.shared_state:
                status_data = {
                    'last_analysis_time': self.last_analysis_time.isoformat(),
                    'analysis_count': self.analysis_count,
                    'cycle_time_seconds': cycle_time,
                    'signals_generated': signals_generated,
                    'total_signals': self.signal_stats['total_signals'],
                    'high_confidence_signals': self.signal_stats['high_confidence_signals'],
                    'signals_sent': self.signal_stats['signals_sent'],
                    'symbols_analyzed': len(self.symbols),
                    'strategy_count': len(self.strategies) if self.strategies else 0,
                    'health_status': 'healthy' if self.error_count < 3 else 'degraded'
                }

                self.shared_state.update_system_status(self.process_id, status_data)

        except Exception as e:
            self.logger.warning(f"Failed to update process status: {e}")

    def _cleanup(self):
        """Cleanup strategy analyzer resources"""
        self.logger.info("Cleaning up StrategyAnalyzer...")

        try:
            # Shutdown multiprocess manager
            if self.multiprocess_manager:
                self.multiprocess_manager.shutdown()
                self.multiprocess_manager = None

            # Clear strategy references
            if self.strategies:
                self.strategies.clear()
                self.strategies = None

            self.data_fetcher = None

            self.logger.info("StrategyAnalyzer cleanup complete")

        except Exception as e:
            self.logger.error(f"Error during StrategyAnalyzer cleanup: {e}")

    def get_process_info(self) -> Dict[str, Any]:
        """Return process-specific information for monitoring"""
        return {
            'process_type': 'strategy_analyzer',
            'analysis_interval_seconds': self.analysis_interval,
            'confidence_threshold': self.confidence_threshold,
            'symbols_tracked': self.symbols,
            'last_analysis_time': self.last_analysis_time.isoformat() if self.last_analysis_time else None,
            'analysis_count': self.analysis_count,
            'signal_statistics': self.signal_stats,
            'strategy_count': len(self.strategies) if self.strategies else 0,
            'worker_count': self.multiprocess_manager.num_workers if self.multiprocess_manager else 0,
            'error_count': self.error_count,
            'state': self.state.value
        }
