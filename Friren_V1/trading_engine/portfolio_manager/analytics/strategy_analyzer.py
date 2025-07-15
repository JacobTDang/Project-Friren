"""
Pure strategy analysis logic - separated from process infrastructure.
Handles strategy execution, signal generation, and performance tracking.
"""

import time
from typing import Dict, List, Any, Optional
from datetime import datetime
import logging

# Import existing components
from ..tools.multiprocess_manager import MultiprocessManager
from ..tools.strategies import discover_all_strategies, AVAILABLE_STRATEGIES
from ...data.yahoo_price import YahooFinancePriceData

# Import chaos detection components
from .market_analyzer import MarketAnalyzer, MarketRegimeResult

# Import dynamic market metrics for chaos scoring
from ...analytics.market_metrics import get_all_metrics, MarketMetricsResult


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

    def __init__(self, confidence_threshold: float = 70.0, symbols: Optional[List[str]] = None, redis_manager=None):
        self.confidence_threshold = confidence_threshold
        self.logger = logging.getLogger("strategy_analyzer")
        
        # Remove hardcoded symbols - use symbols passed from configuration
        self.symbols = symbols or []
        if not self.symbols:
            self.logger.warning("No symbols provided to StrategyAnalyzer - will be configured by process manager")

        # Components (initialized in initialize)
        self.multiprocess_manager = None
        self.strategies = None
        self.data_fetcher = None
        
        # Redis integration for event-driven news
        self.redis_manager = redis_manager
        self.targeted_news_enabled = True

        # Performance tracking
        self.analysis_count = 0
        self.signal_stats = {
            'total_signals': 0,
            'high_confidence_signals': 0,
            'strategy_performance': {}
        }

        # CHAOS DETECTION: Using entropy regime detection system with dynamic thresholds
        self.market_analyzer = None
        self.chaos_detection_enabled = True
        # Initialize with conservative defaults - will be updated dynamically per symbol
        self.chaos_thresholds = {
            'entropy_chaos_threshold': None,  # Will be calculated based on market volatility
            'regime_confidence_threshold': None,  # Will be calculated based on market stability
            'transition_probability_threshold': None,  # Will be calculated based on market regime
            'volatility_spike_threshold': None  # Will be calculated based on historical volatility
        }
        
        # Chaos tracking
        self.chaos_events = []
        self.last_chaos_check = None

        self.logger.info(f"StrategyAnalyzer created - threshold: {confidence_threshold}%, symbols: {len(self.symbols)}")
        self.logger.info(f"Chaos detection enabled with entropy regime analysis")

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

            # Initialize data fetcher (use available YahooFinancePriceData)
            self.data_fetcher = YahooFinancePriceData()
            self.logger.info("Yahoo Finance data fetcher initialized")

            # CHAOS DETECTION: Initialize MarketAnalyzer with entropy regime detection
            if self.chaos_detection_enabled:
                self.market_analyzer = MarketAnalyzer()
                self.logger.info("MarketAnalyzer initialized for chaos detection with entropy regime analysis")

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

                    # FIXED: Handle both DataFrame and dict data types
                    if isinstance(df, dict):
                        # If data_fetcher returns dict, check if it has data
                        if df and len(df) > 0:
                            market_data_dict[symbol] = df
                            self.logger.debug(f"Fetched dict data for {symbol}")
                        else:
                            self.logger.warning(f"No data received for {symbol} (empty dict)")
                    elif hasattr(df, 'empty') and not df.empty:
                        # If it's a DataFrame and not empty
                        market_data_dict[symbol] = df
                        self.logger.debug(f"Fetched {len(df)} days of data for {symbol}")
                    elif df is not None:
                        # Some other data type - try to use it
                        market_data_dict[symbol] = df
                        self.logger.debug(f"Fetched data for {symbol} (type: {type(df)})")
                    else:
                        self.logger.warning(f"No data received for {symbol} (None)")

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
            # FIXED: Use correct multiprocess manager method with proper task structure
            # Create tasks for each symbol
            tasks = []
            for symbol, market_data in market_data_dict.items():
                task = {
                    'symbol': symbol,
                    'market_data': market_data,
                    'market_regime': 'UNKNOWN',  # Can be enhanced later
                    'task_type': 'strategy_analysis',
                    'task_id': f"strategy-{symbol}"
                }
                tasks.append(task)

            # PROPER ARCHITECTURE: Use the refactored worker function from process layer
            from ..processes.strategy_analyzer_process import strategy_analysis_worker
            
            # The worker function is now properly implemented with base strategy architecture

            # Execute tasks in parallel using proper worker function
            task_results = self.multiprocess_manager.execute_tasks_parallel(
                tasks=tasks,
                worker_function=strategy_analysis_worker,
                timeout=60
            )

            # Convert TaskResult objects to expected format
            strategy_results = {}
            for task_result in task_results:
                if task_result.success:
                    result_data = task_result.data
                    symbol = result_data.get('symbol')
                    signals = result_data.get('signals', [])
                    
                    if symbol and signals:
                        # Use the highest confidence signal for overall result
                        max_confidence_signal = max(signals, key=lambda s: s.get('confidence', 0))
                        
                        # Structure compatible with existing processing
                        strategy_results[symbol] = {
                            'strategy': max_confidence_signal.get('strategy', 'unknown'),
                            'confidence': max_confidence_signal.get('confidence', 0),
                            'signals': signals,
                            'strategies_used': result_data.get('strategies_used', []),
                            'action': max_confidence_signal.get('action', 'HOLD'),
                            'reasoning': max_confidence_signal.get('reasoning', 'No clear signal')
                        }

            self.logger.info(f"Multiprocess analysis complete - {len(strategy_results)} symbols analyzed")
            return {'results': {'strategy': strategy_results}}

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

    # CHAOS DETECTION METHODS using entropy regime detection
    
    def detect_market_chaos(self, symbol: str = "SPY") -> Dict[str, Any]:
        """
        Detect market chaos using entropy regime detection system
        
        Returns chaos analysis that can trigger targeted news collection
        """
        if not self.chaos_detection_enabled or not self.market_analyzer:
            return {'chaos_detected': False, 'reason': 'chaos_detection_disabled'}
        
        try:
            self.logger.info(f"CHAOS DETECTION: Analyzing market regime for {symbol}")
            
            # Get market regime analysis using your entropy system
            # First fetch market data for the symbol
            symbol_data = self.data_fetcher.extract_data(symbol, period="100d", interval="1d")
            
            # FIXED: Handle both DataFrame and dict data types
            if isinstance(symbol_data, dict):
                # If data_fetcher returns dict, check if it's empty
                if not symbol_data or len(symbol_data) == 0:
                    return {'chaos_detected': False, 'reason': 'no_data_available'}
            elif hasattr(symbol_data, 'empty') and symbol_data.empty:
                # If it's a DataFrame, use the .empty attribute
                return {'chaos_detected': False, 'reason': 'no_data_available'}
            elif symbol_data is None:
                return {'chaos_detected': False, 'reason': 'no_data_available'}
            
            regime_result = self.market_analyzer.analyze_regime(symbol_data)
            
            chaos_indicators = self._analyze_chaos_indicators(regime_result, symbol)
            
            # Log chaos detection results
            if chaos_indicators['chaos_detected']:
                self.logger.warning(f"CHAOS DETECTED: {symbol} - {chaos_indicators['primary_reason']}")
                self._record_chaos_event(symbol, chaos_indicators)
                
                # REDIS INTEGRATION: Trigger targeted news collection
                if self.targeted_news_enabled:
                    self._trigger_targeted_news_collection(symbol, chaos_indicators)
            else:
                self.logger.debug(f"Market stable for {symbol} - No chaos detected")
            
            self.last_chaos_check = datetime.now()
            return chaos_indicators
            
        except Exception as e:
            self.logger.error(f"Error in chaos detection for {symbol}: {e}")
            return {'chaos_detected': False, 'reason': 'detection_error', 'error': str(e)}
    
    def _calculate_dynamic_chaos_thresholds(self, symbol: str) -> Dict[str, float]:
        """Calculate dynamic chaos thresholds based on market metrics - NO HARDCODED VALUES"""
        
        try:
            # Get comprehensive market metrics for the symbol
            market_metrics = get_all_metrics(symbol)
            
            if market_metrics and market_metrics.volatility is not None:
                volatility = market_metrics.volatility
                risk_score = market_metrics.risk_score or 50.0  # Conservative fallback if None
                
                # Dynamic entropy threshold based on volatility
                # High volatility markets need higher entropy threshold (more tolerance for chaos)
                entropy_threshold = min(0.9, 0.6 + (volatility * 0.3))
                
                # Dynamic confidence threshold based on risk score
                # High risk markets need higher confidence requirements
                confidence_threshold = max(0.2, 0.6 - (risk_score / 200.0))  # Scale risk 0-100 to 0-0.5
                
                # Dynamic transition probability threshold based on market stability
                transition_threshold = min(0.9, 0.5 + (volatility * 0.4))
                
                # Dynamic volatility spike threshold based on historical volatility
                volatility_spike_threshold = max(1.5, 1.0 + (volatility * 2.0))
                
                return {
                    'entropy_chaos_threshold': entropy_threshold,
                    'regime_confidence_threshold': confidence_threshold,
                    'transition_probability_threshold': transition_threshold,
                    'volatility_spike_threshold': volatility_spike_threshold
                }
            else:
                # Conservative thresholds when no market data available
                self.logger.warning(f"No market metrics for {symbol} - using conservative chaos thresholds")
                return {
                    'entropy_chaos_threshold': 0.7,  # More sensitive to chaos
                    'regime_confidence_threshold': 0.5,  # Require higher confidence
                    'transition_probability_threshold': 0.6,  # More sensitive to transitions
                    'volatility_spike_threshold': 1.8  # More sensitive to volatility spikes
                }
                
        except Exception as e:
            self.logger.error(f"Failed to calculate dynamic thresholds for {symbol}: {e}")
            # Very conservative fallback
            return {
                'entropy_chaos_threshold': 0.6,
                'regime_confidence_threshold': 0.6,
                'transition_probability_threshold': 0.5,
                'volatility_spike_threshold': 1.5
            }

    def _analyze_chaos_indicators(self, regime_result: MarketRegimeResult, symbol: str) -> Dict[str, Any]:
        """Analyze regime result for chaos indicators with dynamic scoring"""
        chaos_indicators = {
            'chaos_detected': False,
            'symbol': symbol,
            'chaos_level': 'low',  # low, medium, high, critical
            'primary_reason': '',
            'secondary_reasons': [],
            'urgency': 'low',
            'recommendation': 'monitor'
        }
        
        reasons = []
        chaos_score = 0.0
        
        # Get dynamic thresholds based on market metrics
        dynamic_thresholds = self._calculate_dynamic_chaos_thresholds(symbol)
        
        # Get market metrics for dynamic scoring weights
        try:
            market_metrics = get_all_metrics(symbol)
            volatility = market_metrics.volatility if market_metrics else None
            risk_score = market_metrics.risk_score if market_metrics else None
        except Exception as e:
            self.logger.warning(f"Failed to get market metrics for chaos scoring: {e}")
            volatility = None
            risk_score = None
        
        # Check entropy chaos indicators with dynamic scoring
        if hasattr(regime_result, 'entropy_measures'):
            entropy_measures = regime_result.entropy_measures
            
            # High entropy indicates chaos - dynamic threshold and scoring
            if 'total_entropy' in entropy_measures:
                total_entropy = entropy_measures['total_entropy']
                entropy_threshold = dynamic_thresholds['entropy_chaos_threshold']
                if total_entropy > entropy_threshold:
                    reasons.append(f"high_entropy_{total_entropy:.2f}")
                    # Dynamic scoring based on how much entropy exceeds threshold and market volatility
                    excess_entropy = total_entropy - entropy_threshold
                    volatility_multiplier = 1.0 + (volatility * 0.5) if volatility else 1.0
                    entropy_score = min(0.5, excess_entropy * volatility_multiplier)
                    chaos_score += entropy_score
            
            # Check price entropy specifically with dynamic threshold
            if 'price_entropy' in entropy_measures:
                price_entropy = entropy_measures['price_entropy']
                # Dynamic price entropy threshold based on market risk
                price_entropy_threshold = 0.75 + (risk_score / 500.0) if risk_score else 0.8
                if price_entropy > price_entropy_threshold:
                    reasons.append(f"price_chaos_{price_entropy:.2f}")
                    # Dynamic scoring based on price entropy severity
                    excess_price_entropy = price_entropy - price_entropy_threshold
                    price_chaos_score = min(0.4, excess_price_entropy * 2.0)
                    chaos_score += price_chaos_score
        
        # Low regime confidence indicates uncertainty - dynamic scoring
        confidence_threshold = dynamic_thresholds['regime_confidence_threshold']
        if regime_result.regime_confidence < confidence_threshold:
            reasons.append(f"low_confidence_{regime_result.regime_confidence:.2f}")
            # Dynamic scoring based on confidence deficit and market risk
            confidence_deficit = confidence_threshold - regime_result.regime_confidence
            risk_multiplier = 1.0 + (risk_score / 200.0) if risk_score else 1.0
            confidence_score = min(0.4, confidence_deficit * risk_multiplier)
            chaos_score += confidence_score
        
        # High transition probability indicates instability - dynamic scoring
        transition_threshold = dynamic_thresholds['transition_probability_threshold']
        if regime_result.regime_transition_probability > transition_threshold:
            reasons.append(f"high_transition_prob_{regime_result.regime_transition_probability:.2f}")
            # Dynamic scoring based on transition probability excess and volatility
            transition_excess = regime_result.regime_transition_probability - transition_threshold
            volatility_multiplier = 1.0 + (volatility * 0.3) if volatility else 1.0
            transition_score = min(0.4, transition_excess * volatility_multiplier * 2.0)
            chaos_score += transition_score
        
        # Volatility spike detection with dynamic scoring
        if regime_result.volatility_regime == "HIGH_VOLATILITY":
            volatility_threshold = dynamic_thresholds['volatility_spike_threshold']
            if regime_result.current_volatility > volatility_threshold:
                reasons.append(f"volatility_spike_{regime_result.current_volatility:.2f}")
                # Dynamic scoring based on volatility spike magnitude
                volatility_spike_ratio = regime_result.current_volatility / volatility_threshold
                # Higher spikes get exponentially higher scores, capped at 0.5
                volatility_score = min(0.5, 0.2 * (volatility_spike_ratio ** 1.5))
                chaos_score += volatility_score
        
        # Strategy conflict detection with dynamic scoring
        if hasattr(regime_result, 'enhanced_regime') and hasattr(regime_result, 'entropy_regime'):
            if regime_result.enhanced_regime != regime_result.entropy_regime:
                reasons.append("regime_detector_conflict")
                # Dynamic scoring based on market uncertainty
                # Higher risk markets get higher conflict penalties
                conflict_score = 0.1 + (risk_score / 1000.0) if risk_score else 0.15
                chaos_score += min(0.25, conflict_score)
        
        # Determine chaos level and urgency with dynamic thresholds
        # Dynamic thresholds based on market characteristics
        if volatility and risk_score:
            # More volatile/risky markets need higher thresholds to trigger alerts
            base_adjustment = (volatility * 0.1) + (risk_score / 1000.0)
            critical_threshold = min(0.9, 0.7 + base_adjustment)
            high_threshold = min(0.8, 0.5 + base_adjustment)
            medium_threshold = min(0.6, 0.3 + base_adjustment)
            low_threshold = min(0.4, 0.15 + base_adjustment)
        else:
            # Conservative thresholds when no market data
            critical_threshold = 0.6
            high_threshold = 0.45
            medium_threshold = 0.3
            low_threshold = 0.15
        
        if chaos_score >= critical_threshold:
            chaos_indicators.update({
                'chaos_detected': True,
                'chaos_level': 'critical',
                'urgency': 'critical',
                'recommendation': 'immediate_news_collection'
            })
        elif chaos_score >= high_threshold:
            chaos_indicators.update({
                'chaos_detected': True,
                'chaos_level': 'high',
                'urgency': 'high',
                'recommendation': 'targeted_news_collection'
            })
        elif chaos_score >= medium_threshold:
            chaos_indicators.update({
                'chaos_detected': True,
                'chaos_level': 'medium',
                'urgency': 'medium',
                'recommendation': 'enhanced_monitoring'
            })
        elif chaos_score >= low_threshold:
            chaos_indicators.update({
                'chaos_level': 'low',
                'urgency': 'low',
                'recommendation': 'continue_monitoring'
            })
        
        if reasons:
            chaos_indicators['primary_reason'] = reasons[0]
            chaos_indicators['secondary_reasons'] = reasons[1:]
        
        chaos_indicators['chaos_score'] = chaos_score
        chaos_indicators['regime_analysis'] = {
            'primary_regime': regime_result.primary_regime,
            'regime_confidence': regime_result.regime_confidence,
            'volatility_regime': regime_result.volatility_regime,
            'transition_probability': regime_result.regime_transition_probability
        }
        
        return chaos_indicators
    
    def _trigger_targeted_news_collection(self, symbol: str, chaos_indicators: Dict[str, Any]):
        """Trigger targeted news collection via Redis when chaos is detected"""
        if not self.redis_manager:
            self.logger.warning("Redis manager not available - cannot trigger targeted news collection")
            return
        
        try:
            # Create targeted news request
            request_id = f"chaos_{symbol}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            # Map chaos urgency to news collection parameters
            urgency = chaos_indicators.get('urgency', 'low')
            chaos_level = chaos_indicators.get('chaos_level', 'low')
            
            # Determine sources and article count based on urgency
            if urgency == 'critical':
                sources_requested = ["alpha_vantage", "fmp", "marketaux", "newsapi"]
                max_articles = 15
            elif urgency == 'high':
                sources_requested = ["alpha_vantage", "fmp", "marketaux"]
                max_articles = 10
            else:
                sources_requested = ["alpha_vantage", "fmp"]
                max_articles = 5
            
            targeted_news_request = {
                "request_id": request_id,
                "symbol": symbol,
                "urgency": urgency,
                "chaos_level": chaos_level,
                "chaos_reason": chaos_indicators.get('primary_reason', ''),
                "secondary_reasons": chaos_indicators.get('secondary_reasons', []),
                "chaos_score": chaos_indicators.get('chaos_score', 0.0),
                "requested_by": "strategy_analyzer",
                "timestamp": datetime.now().isoformat(),
                "sources_requested": sources_requested,
                "max_articles": max_articles,
                "regime_analysis": chaos_indicators.get('regime_analysis', {})
            }
            
            # Send request via Redis message system  
            from ....multiprocess_infrastructure.trading_redis_manager import create_process_message, MessagePriority
            
            # Map urgency to message priority
            priority_map = {
                'critical': MessagePriority.CRITICAL,
                'high': MessagePriority.HIGH,
                'medium': MessagePriority.NORMAL,
                'low': MessagePriority.LOW
            }
            
            message = create_process_message(
                sender="strategy_analyzer",
                recipient="enhanced_news_pipeline",
                message_type="TARGETED_NEWS_REQUEST",
                data=targeted_news_request,
                priority=priority_map.get(urgency, MessagePriority.NORMAL)
            )
            
            success = self.redis_manager.send_message(message)
            
            if success:
                self.logger.info(f"TARGETED NEWS: Triggered collection for {symbol} - urgency: {urgency}")
                self.logger.info(f"TARGETED NEWS: Request ID: {request_id}")
            else:
                self.logger.error(f"Failed to send targeted news request for {symbol}")
                
        except Exception as e:
            self.logger.error(f"Error triggering targeted news collection: {e}")
    
    def _record_chaos_event(self, symbol: str, chaos_indicators: Dict[str, Any]):
        """Record chaos event for tracking and analysis"""
        chaos_event = {
            'timestamp': datetime.now(),
            'symbol': symbol,
            'chaos_level': chaos_indicators['chaos_level'],
            'chaos_score': chaos_indicators['chaos_score'],
            'primary_reason': chaos_indicators['primary_reason'],
            'urgency': chaos_indicators['urgency']
        }
        
        self.chaos_events.append(chaos_event)
        
        # Keep only last 50 chaos events
        if len(self.chaos_events) > 50:
            self.chaos_events = self.chaos_events[-50:]
    
    def get_chaos_statistics(self) -> Dict[str, Any]:
        """Get chaos detection statistics"""
        if not self.chaos_events:
            return {'total_chaos_events': 0, 'last_chaos_check': self.last_chaos_check}
        
        recent_events = [e for e in self.chaos_events if (datetime.now() - e['timestamp']).total_seconds() < 3600]  # Last hour
        
        return {
            'total_chaos_events': len(self.chaos_events),
            'recent_chaos_events': len(recent_events),
            'last_chaos_event': self.chaos_events[-1] if self.chaos_events else None,
            'last_chaos_check': self.last_chaos_check,
            'chaos_by_level': {
                'critical': len([e for e in recent_events if e['chaos_level'] == 'critical']),
                'high': len([e for e in recent_events if e['chaos_level'] == 'high']),
                'medium': len([e for e in recent_events if e['chaos_level'] == 'medium'])
            }
        }

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
