#!/usr/bin/env python3
"""
Simple ASCII-Only Enhanced Trading Orchestrator

Demonstrates all the enhanced features requested:
- Dynamic risk management with configurable profiles
- Sentiment analysis with volatility-based thresholds
- Portfolio rebalancing and partial exits
- Enhanced logging with detailed timestamps and analytics
- Strategy performance tracking

No Unicode characters - Windows compatible
"""

import sys
import os
import logging
import random
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Tuple
from dataclasses import dataclass

@dataclass
class TradingDecision:
    """Trading decision data structure"""
    symbol: str
    action: str  # 'BUY', 'SELL', 'HOLD', 'PARTIAL_SELL', 'REBALANCE'
    confidence: float
    sentiment_score: float
    reasoning: str
    position_size: float
    strategy: str

class SimpleAsciiOrchestrator:
    """ASCII-only enhanced trading orchestrator"""

    def __init__(self, symbols: List[str] = None, cycle_interval: int = 30):
        self.symbols = symbols or ['AAPL', 'MSFT', 'GOOGL', 'TSLA', 'NVDA']
        self.cycle_interval = cycle_interval
        self.logger = self._setup_logging()
        self.running = False
        self.cycle_count = 0

        # Enhanced components
        self.risk_manager = self._create_enhanced_risk_manager()
        self.sentiment_analyzer = self._create_enhanced_sentiment_analyzer()
        self.portfolio_manager = self._create_enhanced_portfolio_manager()
        self.performance_tracker = self._create_performance_tracker()

        self.logger.info("=" * 60)
        self.logger.info("ASCII Enhanced Trading Orchestrator Initialized")
        self.logger.info("=" * 60)
        self.logger.info(f"Symbols: {self.symbols}")
        self.logger.info(f"Cycle Interval: {cycle_interval}s")
        self.logger.info("Features: Risk Management, Sentiment Analysis, Rebalancing")
        self.logger.info("=" * 60)

    def _setup_logging(self) -> logging.Logger:
        """Setup enhanced ASCII logging"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        logger = logging.getLogger("ascii_orchestrator")

        # Add file handler
        log_file = f"ascii_trading_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        return logger

    def _create_enhanced_risk_manager(self):
        """Create enhanced risk manager with dynamic profiles"""
        class EnhancedRiskManager:
            def __init__(self):
                self.risk_profiles = {
                    'conservative': {'max_position': 0.08, 'approval_rate': 0.75},
                    'moderate': {'max_position': 0.12, 'approval_rate': 0.85},
                    'aggressive': {'max_position': 0.18, 'approval_rate': 0.95}
                }
                self.current_profile = 'moderate'
                self.market_volatility = random.uniform(0.15, 0.35)
                self.daily_trades = 0

            def validate_trade(self, decision: TradingDecision) -> bool:
                """Enhanced risk validation with dynamic adjustment"""
                profile = self.risk_profiles[self.current_profile]

                # Position size check
                if decision.position_size > profile['max_position']:
                    return False

                # Daily trade limit
                if self.daily_trades >= 10:
                    return False

                # Confidence threshold
                min_confidence = 0.6 if self.current_profile == 'aggressive' else 0.7
                if decision.confidence < min_confidence:
                    return False

                # Random approval based on profile
                if random.random() > profile['approval_rate']:
                    return False

                self.daily_trades += 1
                return True

            def adjust_risk_profile(self, market_conditions: str):
                """Dynamically adjust risk profile"""
                if market_conditions == 'volatile':
                    self.current_profile = 'conservative'
                elif market_conditions == 'stable':
                    self.current_profile = 'moderate'
                else:
                    self.current_profile = 'aggressive'

        return EnhancedRiskManager()

    def _create_enhanced_sentiment_analyzer(self):
        """Create enhanced sentiment analyzer with volatility adjustment"""
        class EnhancedSentimentAnalyzer:
            def __init__(self):
                self.base_thresholds = {
                    'strong_positive': 0.4,
                    'positive': 0.1,
                    'neutral': 0.05,
                    'negative': -0.1,
                    'strong_negative': -0.4
                }
                self.market_volatility = random.uniform(0.15, 0.35)
                self.sentiment_history = []

            def analyze_sentiment(self, symbol: str) -> Dict[str, Any]:
                """Enhanced sentiment analysis with volatility adjustment"""
                # Simulate news sentiment
                raw_sentiment = random.uniform(-0.8, 0.8)

                # Adjust thresholds based on volatility
                volatility_factor = 1 + (self.market_volatility - 0.25)
                adjusted_sentiment = raw_sentiment * volatility_factor

                # Clamp to valid range
                final_sentiment = max(-1.0, min(1.0, adjusted_sentiment))

                # Store history for trend analysis
                self.sentiment_history.append(final_sentiment)
                if len(self.sentiment_history) > 20:
                    self.sentiment_history.pop(0)

                # Calculate confidence based on consistency
                recent_avg = sum(self.sentiment_history[-5:]) / min(5, len(self.sentiment_history))
                confidence = 0.6 + abs(final_sentiment) * 0.4

                return {
                    'sentiment_score': final_sentiment,
                    'confidence': confidence,
                    'trend': 'up' if recent_avg > 0 else 'down',
                    'volatility_adjusted': True
                }

        return EnhancedSentimentAnalyzer()

    def _create_enhanced_portfolio_manager(self):
        """Create enhanced portfolio manager with rebalancing"""
        class EnhancedPortfolioManager:
            def __init__(self):
                self.positions = {}  # symbol -> percentage
                self.cash_allocation = 0.25  # 25% cash target
                self.max_single_position = 0.15  # 15% max per stock
                self.rebalance_threshold = 0.05  # 5% deviation triggers rebalance
                self.last_rebalance = datetime.now() - timedelta(days=7)

            def get_position(self, symbol: str) -> float:
                """Get current position percentage"""
                return self.positions.get(symbol, 0.0)

            def update_position(self, symbol: str, new_pct: float):
                """Update position with rebalancing logic"""
                old_pct = self.positions.get(symbol, 0.0)
                self.positions[symbol] = new_pct

                # Remove zero positions
                if new_pct <= 0.001:
                    self.positions.pop(symbol, None)

                return old_pct

            def should_rebalance(self) -> bool:
                """Check if portfolio needs rebalancing"""
                # Time-based rebalancing
                if datetime.now() - self.last_rebalance > timedelta(days=30):
                    return True

                # Allocation-based rebalancing
                total_allocation = sum(self.positions.values())
                target_allocation = 0.75  # 75% invested

                if abs(total_allocation - target_allocation) > self.rebalance_threshold:
                    return True

                # Check for oversized positions
                for symbol, pct in self.positions.items():
                    if pct > self.max_single_position + self.rebalance_threshold:
                        return True

                return False

            def calculate_rebalanced_size(self, symbol: str, target_pct: float) -> float:
                """Calculate rebalanced position size"""
                current_allocation = sum(self.positions.values())
                max_new_allocation = 0.75  # Don't exceed 75% invested

                if current_allocation + target_pct > max_new_allocation:
                    return max(0.0, max_new_allocation - current_allocation)

                return min(target_pct, self.max_single_position)

            def get_portfolio_stats(self) -> Dict[str, Any]:
                """Get current portfolio statistics"""
                total_allocation = sum(self.positions.values())
                position_count = len([p for p in self.positions.values() if p > 0.001])
                largest_position = max(self.positions.values()) if self.positions else 0.0

                return {
                    'total_allocation': total_allocation,
                    'cash_percentage': 1.0 - total_allocation,
                    'position_count': position_count,
                    'largest_position': largest_position,
                    'positions': dict(self.positions)
                }

        return EnhancedPortfolioManager()

    def _create_performance_tracker(self):
        """Create performance tracking system"""
        class PerformanceTracker:
            def __init__(self):
                self.trades = []
                self.strategy_performance = {}
                self.daily_pnl = []

            def record_trade(self, decision: TradingDecision, success: bool):
                """Record trade execution"""
                trade_record = {
                    'timestamp': datetime.now(),
                    'symbol': decision.symbol,
                    'action': decision.action,
                    'confidence': decision.confidence,
                    'sentiment': decision.sentiment_score,
                    'strategy': decision.strategy,
                    'success': success
                }
                self.trades.append(trade_record)

                # Update strategy performance
                if decision.strategy not in self.strategy_performance:
                    self.strategy_performance[decision.strategy] = {
                        'total_trades': 0,
                        'successful_trades': 0,
                        'total_value': 0.0
                    }

                strategy_stats = self.strategy_performance[decision.strategy]
                strategy_stats['total_trades'] += 1
                if success:
                    strategy_stats['successful_trades'] += 1
                    strategy_stats['total_value'] += decision.position_size * 100000  # Assuming $100k portfolio

            def get_strategy_stats(self) -> Dict[str, Any]:
                """Get strategy performance statistics"""
                stats = {}
                for strategy, data in self.strategy_performance.items():
                    if data['total_trades'] > 0:
                        success_rate = data['successful_trades'] / data['total_trades']
                        stats[strategy] = {
                            'success_rate': success_rate,
                            'total_trades': data['total_trades'],
                            'total_value': data['total_value']
                        }
                return stats

        return PerformanceTracker()

    def start_trading(self):
        """Start the enhanced trading loop"""
        self.running = True
        self.logger.info("Starting enhanced trading system...")

        try:
            while self.running:
                self.cycle_count += 1
                cycle_start = datetime.now()

                self.logger.info(f"=== CYCLE {self.cycle_count} START ===")

                # Execute trading cycle
                cycle_results = self._execute_trading_cycle()

                # Log cycle summary
                cycle_duration = (datetime.now() - cycle_start).total_seconds()
                self._log_cycle_summary(cycle_results, cycle_duration)

                # Wait for next cycle
                if self.running:
                    self.logger.info(f"Waiting {self.cycle_interval}s for next cycle...")
                    time.sleep(self.cycle_interval)

        except KeyboardInterrupt:
            self.logger.info("Trading stopped by user")
        finally:
            self.stop_trading()

    def _execute_trading_cycle(self) -> Dict[str, Any]:
        """Execute one trading cycle with all enhanced features"""
        cycle_results = {
            'decisions_made': 0,
            'successful_trades': 0,
            'failed_trades': 0,
            'rebalancing_actions': 0,
            'partial_exits': 0
        }

        # Check if portfolio rebalancing is needed
        if self.portfolio_manager.should_rebalance():
            self.logger.info(">>> Portfolio rebalancing triggered <<<")
            self.portfolio_manager.last_rebalance = datetime.now()
            cycle_results['rebalancing_actions'] += 1

        # Adjust risk profile based on market conditions
        market_condition = random.choice(['stable', 'volatile', 'trending'])
        self.risk_manager.adjust_risk_profile(market_condition)

        # Process each symbol
        for symbol in self.symbols:
            try:
                decision = self._make_trading_decision(symbol)

                if decision.action != 'HOLD':
                    cycle_results['decisions_made'] += 1

                    # Risk validation
                    if self.risk_manager.validate_trade(decision):
                        # Execute trade (simulated)
                        success = random.random() < 0.85  # 85% success rate

                        if success:
                            cycle_results['successful_trades'] += 1
                            # Update portfolio
                            old_pct = self.portfolio_manager.update_position(
                                symbol, decision.position_size
                            )
                            self.logger.info(f"TRADE SUCCESS: {symbol} {decision.action} "
                                           f"{decision.position_size:.1%} (was {old_pct:.1%})")
                        else:
                            cycle_results['failed_trades'] += 1
                            self.logger.warning(f"TRADE FAILED: {symbol} {decision.action}")

                        # Record performance
                        self.performance_tracker.record_trade(decision, success)

                        # Track special actions
                        if decision.action == 'PARTIAL_SELL':
                            cycle_results['partial_exits'] += 1

                    else:
                        self.logger.info(f"RISK REJECTED: {symbol} {decision.action} "
                                       f"(confidence: {decision.confidence:.2f})")
                else:
                    self.logger.info(f"HOLD: {symbol} (sentiment: {decision.sentiment_score:+.2f})")

            except Exception as e:
                self.logger.error(f"Error processing {symbol}: {e}")

        return cycle_results

    def _make_trading_decision(self, symbol: str) -> TradingDecision:
        """Make enhanced trading decision for symbol"""
        # Get sentiment analysis
        sentiment_data = self.sentiment_analyzer.analyze_sentiment(symbol)
        sentiment_score = sentiment_data['sentiment_score']
        confidence = sentiment_data['confidence']

        # Get current position
        current_position = self.portfolio_manager.get_position(symbol)

        # Generate base recommendation
        if sentiment_score > 0.3 and confidence > 0.7:
            base_action = 'BUY'
        elif sentiment_score < -0.3 and confidence > 0.7:
            base_action = 'SELL'
        else:
            base_action = 'HOLD'

        # Apply position-aware logic
        final_action, position_size, reasoning, strategy = self._apply_position_logic(
            symbol, base_action, current_position, sentiment_score, confidence
        )

        return TradingDecision(
            symbol=symbol,
            action=final_action,
            confidence=confidence,
            sentiment_score=sentiment_score,
            reasoning=reasoning,
            position_size=position_size,
            strategy=strategy
        )

    def _apply_position_logic(self, symbol: str, base_action: str, current_pos: float,
                            sentiment: float, confidence: float) -> Tuple[str, float, str, str]:
        """Apply enhanced position-aware logic"""

        # Check for partial exit conditions
        if current_pos > 0.12 and sentiment < -0.2:
            # Large position with negative sentiment - partial exit
            new_size = current_pos * 0.7  # Reduce by 30%
            return ('PARTIAL_SELL', new_size,
                   f"Partial exit: reducing {current_pos:.1%} to {new_size:.1%}",
                   'risk_management_strategy')

        # Existing position logic
        if current_pos > 0.001:  # Has position
            if base_action == 'BUY' and current_pos < 0.15:
                # Add to position
                additional = min(0.03, 0.15 - current_pos)
                new_size = current_pos + additional
                return ('BUY_MORE', new_size,
                       f"Adding {additional:.1%} to position (total: {new_size:.1%})",
                       'sentiment_momentum_strategy')
            elif base_action == 'SELL' and sentiment < -0.4:
                # Strong sell - exit completely
                return ('SELL', 0.0,
                       f"Strong sell signal, exiting {current_pos:.1%} position",
                       'risk_management_strategy')
            else:
                # Hold current position
                return ('HOLD', current_pos,
                       f"Maintaining {current_pos:.1%} position",
                       'hold_strategy')
        else:
            # No position - consider opening
            if base_action == 'BUY':
                target_size = self.portfolio_manager.calculate_rebalanced_size(
                    symbol, 0.05 + confidence * 0.05
                )
                if target_size > 0.02:  # Only if meaningful size
                    return ('BUY', target_size,
                           f"Opening {target_size:.1%} position",
                           'value_opportunity_strategy')

            return ('HOLD', 0.0, f"No position, {base_action} signal", 'hold_strategy')

    def _log_cycle_summary(self, results: Dict[str, Any], duration: float):
        """Log enhanced cycle summary with ASCII formatting"""
        self.logger.info(f"=== CYCLE {self.cycle_count} SUMMARY ===")
        self.logger.info(f"Duration: {duration:.2f}s")
        self.logger.info(f"Decisions Made: {results['decisions_made']}")
        self.logger.info(f"Successful Trades: {results['successful_trades']}")
        self.logger.info(f"Failed Trades: {results['failed_trades']}")
        self.logger.info(f"Rebalancing Actions: {results['rebalancing_actions']}")
        self.logger.info(f"Partial Exits: {results['partial_exits']}")

        # Portfolio status
        portfolio_stats = self.portfolio_manager.get_portfolio_stats()
        self.logger.info(f"--- PORTFOLIO STATUS ---")
        self.logger.info(f"Total Allocation: {portfolio_stats['total_allocation']:.1%}")
        self.logger.info(f"Cash: {portfolio_stats['cash_percentage']:.1%}")
        self.logger.info(f"Active Positions: {portfolio_stats['position_count']}")

        for symbol, pct in portfolio_stats['positions'].items():
            value = pct * 100000  # Assuming $100k portfolio
            self.logger.info(f"  {symbol}: {pct:.1%} (${value:,.0f})")

        # Risk management status
        self.logger.info(f"--- RISK MANAGEMENT ---")
        self.logger.info(f"Risk Profile: {self.risk_manager.current_profile}")
        self.logger.info(f"Market Volatility: {self.risk_manager.market_volatility:.1%}")
        self.logger.info(f"Daily Trades: {self.risk_manager.daily_trades}")

        # Strategy performance
        strategy_stats = self.performance_tracker.get_strategy_stats()
        if strategy_stats:
            self.logger.info(f"--- STRATEGY PERFORMANCE ---")
            for strategy, stats in strategy_stats.items():
                success_rate = stats['success_rate'] * 100
                self.logger.info(f"{strategy}: {success_rate:.0f}% "
                               f"({stats['total_trades']} trades, ${stats['total_value']:,.0f})")

        self.logger.info("=" * 40)

    def stop_trading(self):
        """Stop the trading system"""
        self.running = False
        self.logger.info("Trading system stopped")

        # Final summary
        total_trades = len(self.performance_tracker.trades)
        if total_trades > 0:
            successful = sum(1 for t in self.performance_tracker.trades if t['success'])
            success_rate = successful / total_trades * 100

            self.logger.info("=== FINAL SUMMARY ===")
            self.logger.info(f"Total Cycles: {self.cycle_count}")
            self.logger.info(f"Total Trades: {total_trades}")
            self.logger.info(f"Success Rate: {success_rate:.1f}%")
            self.logger.info("Trading session complete")

def main():
    """Main function to run the ASCII orchestrator"""
    print("Starting ASCII Enhanced Trading Orchestrator...")
    print("Features:")
    print("- Dynamic Risk Management")
    print("- Sentiment Analysis with Volatility Adjustment")
    print("- Portfolio Rebalancing")
    print("- Enhanced Logging")
    print("- Strategy Performance Tracking")
    print("- No Unicode - Windows Compatible")
    print()

    # Get user input for symbols and interval
    symbols_input = input("Enter symbols (comma-separated, or press Enter for default): ").strip()
    if symbols_input:
        symbols = [s.strip().upper() for s in symbols_input.split(',')]
    else:
        symbols = ['AAPL', 'MSFT', 'GOOGL', 'TSLA', 'NVDA']

    interval_input = input("Enter cycle interval in seconds (or press Enter for 30): ").strip()
    if interval_input.isdigit():
        interval = int(interval_input)
    else:
        interval = 30

    # Create and start orchestrator
    orchestrator = SimpleAsciiOrchestrator(symbols=symbols, cycle_interval=interval)
    orchestrator.start_trading()

if __name__ == "__main__":
    main()
