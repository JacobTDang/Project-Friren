"""
Pure strategy analysis logic - separated from process infrastructure.
Handles strategy execution, signal generation, and performance tracking.
"""

import time
from typing import Dict, List, Any, Optional
from datetime import datetime
import logging

# Import existing components
from ..multiprocess_manager import MultiprocessManager
from ..strategies import discover_all_strategies, AVAILABLE_STRATEGIES
from ...data.yahoo_price import StockDataFetcher


class StrategyAnalyzer:
    """
    Pure Strategy Analysis Component

    Responsibilities:
    - Run all strategies in parallel using existing MultiprocessManager
    - Generate standardized trading signals
    - Track strategy performance metrics
    - Filter signals by confidence threshold

    Does NOT handle:
    - Process lifecycle management
    - Queue operations
    - Shared state updates
    - Timing/scheduling
    """

    def __init__(self, confidence_threshold: float = 70.0, symbols: Optional[List[str]] = None):
        self.confidence_threshold = confidence_threshold
        self.symbols = symbols or ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA', 'NVDA', 'META', 'NFLX']
        self.logger = logging.getLogger("strategy_analyzer")

        # Components (initialized in initialize)
        self.multiprocess_manager = None
        self.strategies = None
        self.data_fetcher = None

        # Performance tracking
        self.analysis_count = 0
        self.signal_stats = {
            'total_signals': 0,
            'high_confidence_signals': 0,
            'strategy_performance': {}
        }

        self.logger.info(f"StrategyAnalyzer created - threshold: {confidence_threshold}%, symbols: {len(self.symbols)}")

    def initialize(self):
        """Initialize strategy analyzer components"""
        self.logger.info("Initializing StrategyAnalyzer components...")

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

            self.logger.info("StrategyAnalyzer initialization complete")

        except Exception as e:
            self.logger.error(f"Failed to initialize StrategyAnalyzer: {e}")
            raise

    def analyze_all_strategies(self, market_regime: str = 'UNKNOWN') -> Dict[str, Any]:
        """
        Main analysis method - run all strategies and return results

        Args:
            market_regime: Current market regime for context

        Returns:
            Dict containing analysis results and generated signals
        """
        self.logger.info(f"Starting strategy analysis cycle #{self.analysis_count + 1}")
        start_time = time.time()

        try:
            # Fetch market data for all symbols
            market_data = self._fetch_market_data()

            if not market_data:
                self.logger.warning("No market data available")
                return {'success': False, 'error': 'No market data'}

            # Run strategy analysis using existing multiprocess manager
            analysis_results = self._run_strategy_analysis(market_data)

            # Process results and generate standardized signals
            processed_signals = self._process_analysis_results(analysis_results, market_regime)

            # Update statistics
            self.analysis_count += 1
            cycle_time = time.time() - start_time

            # Return comprehensive results
            return {
                'success': True,
                'analysis_cycle': self.analysis_count,
                'cycle_time_seconds': cycle_time,
                'symbols_analyzed': len(market_data),
                'signals': processed_signals,
                'high_confidence_signals': [s for s in processed_signals if s['confidence'] >= self.confidence_threshold],
                'statistics': self._get_current_stats(),
                'market_regime': market_regime,
                'timestamp': datetime.now()
            }

        except Exception as e:
            self.logger.error(f"Error in strategy analysis: {e}")
            return {'success': False, 'error': str(e)}

    def get_strategy_signals(self, symbol: str, market_data: Dict[str, Any], market_regime: str = 'UNKNOWN') -> List[Dict[str, Any]]:
        """
        Get signals for a specific symbol from all strategies

        Args:
            symbol: Stock symbol
            market_data: Market data dictionary
            market_regime: Current market regime

        Returns:
            List of signals from all applicable strategies
        """
        signals = []

        if symbol not in market_data:
            return signals

        # Run analysis for single symbol
        single_symbol_data = {symbol: market_data[symbol]}
        analysis_results = self._run_strategy_analysis(single_symbol_data)

        # Process results for this symbol
        strategy_results = analysis_results.get('results', {}).get('strategy', {})

        if symbol in strategy_results:
            result_data = strategy_results[symbol]
            signal = self._generate_signal_from_result(symbol, result_data, market_regime)
            if signal:
                signals.append(signal)

        return signals

    def _fetch_market_data(self) -> Dict[str, Any]:
        """Fetch market data for all tracked symbols"""
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

    def _process_analysis_results(self, analysis_results: Dict[str, Any], market_regime: str) -> List[Dict[str, Any]]:
        """Process raw analysis results into standardized signals"""
        signals = []

        try:
            strategy_results = analysis_results.get('results', {}).get('strategy', {})

            for symbol, result_data in strategy_results.items():
                if not result_data:
                    continue

                # Extract strategy signal information
                strategy_name = result_data.get('strategy', 'unknown')
                confidence = result_data.get('confidence', 0)

                # Update strategy performance tracking
                self._update_strategy_stats(strategy_name, confidence)

                # Generate standardized signal
                signal = self._generate_signal_from_result(symbol, result_data, market_regime)

                if signal:
                    signals.append(signal)
                    self.signal_stats['total_signals'] += 1

                    if confidence >= self.confidence_threshold:
                        self.signal_stats['high_confidence_signals'] += 1

            return signals

        except Exception as e:
            self.logger.error(f"Error processing analysis results: {e}")
            return []

    def _generate_signal_from_result(self, symbol: str, result_data: Dict[str, Any], market_regime: str) -> Optional[Dict[str, Any]]:
        """Generate standardized trading signal from strategy result"""
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

            # Create standardized signal
            signal = {
                'symbol': symbol,
                'action': action,
                'confidence': confidence,
                'reasoning': reasoning,
                'strategy': strategy_name,
                'rsi': rsi,
                'market_regime': market_regime,
                'timestamp': datetime.now(),
                'analysis_cycle': self.analysis_count,
                'is_high_confidence': confidence >= self.confidence_threshold,
                'metadata': {
                    'raw_result': result_data
                }
            }

            return signal

        except Exception as e:
            self.logger.error(f"Error generating signal for {symbol}: {e}")
            return None

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

    def _get_current_stats(self) -> Dict[str, Any]:
        """Get current performance statistics"""
        return {
            'analysis_count': self.analysis_count,
            'total_signals': self.signal_stats['total_signals'],
            'high_confidence_signals': self.signal_stats['high_confidence_signals'],
            'high_confidence_rate': (
                self.signal_stats['high_confidence_signals'] / self.signal_stats['total_signals']
                if self.signal_stats['total_signals'] > 0 else 0
            ),
            'strategy_performance': self.signal_stats['strategy_performance'].copy(),
            'symbols_count': len(self.symbols),
            'strategies_count': len(self.strategies) if self.strategies else 0
        }

    def filter_signals_by_confidence(self, signals: List[Dict[str, Any]], min_confidence: Optional[float] = None) -> List[Dict[str, Any]]:
        """Filter signals by confidence threshold"""
        threshold = min_confidence or self.confidence_threshold
        return [signal for signal in signals if signal['confidence'] >= threshold]

    def get_signals_by_action(self, signals: List[Dict[str, Any]], action: str) -> List[Dict[str, Any]]:
        """Filter signals by action type (BUY, SELL, HOLD)"""
        return [signal for signal in signals if signal['action'] == action.upper()]

    def get_signals_by_strategy(self, signals: List[Dict[str, Any]], strategy_name: str) -> List[Dict[str, Any]]:
        """Filter signals by strategy name"""
        return [signal for signal in signals if signal['strategy'] == strategy_name]

    def cleanup(self):
        """Cleanup analyzer resources"""
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
