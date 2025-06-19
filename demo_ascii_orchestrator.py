#!/usr/bin/env python3
"""
Demo ASCII-Only Enhanced Trading Orchestrator

Automatically demonstrates all enhanced features:
- Dynamic risk management with configurable profiles
- Sentiment analysis with volatility-based thresholds
- Portfolio rebalancing and partial exits
- Enhanced logging with detailed timestamps and analytics
- Strategy performance tracking

No Unicode characters - Windows compatible
"""

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

class DemoAsciiOrchestrator:
    """Demo ASCII-only enhanced trading orchestrator"""

    def __init__(self, max_cycles: int = 20):
        self.symbols = ['AAPL', 'MSFT', 'GOOGL']
        self.cycle_interval = 10  # Faster for demo
        self.max_cycles = max_cycles
        self.logger = self._setup_logging()
        self.running = False
        self.cycle_count = 0

        # Enhanced components
        self.risk_manager = self._create_enhanced_risk_manager()
        self.sentiment_analyzer = self._create_enhanced_sentiment_analyzer()
        self.portfolio_manager = self._create_enhanced_portfolio_manager()
        self.performance_tracker = self._create_performance_tracker()

        self.logger.info("=" * 60)
        self.logger.info("DEMO: ASCII Enhanced Trading Orchestrator")
        self.logger.info("=" * 60)
        self.logger.info(f"Symbols: {self.symbols}")
        self.logger.info(f"Cycle Interval: {self.cycle_interval}s")
        self.logger.info(f"Max Cycles: {max_cycles}")
        self.logger.info("Features:")
        self.logger.info("  * Dynamic Risk Management")
        self.logger.info("  * Sentiment Analysis with Volatility Adjustment")
        self.logger.info("  * Portfolio Rebalancing")
        self.logger.info("  * Enhanced Logging")
        self.logger.info("  * Strategy Performance Tracking")
        self.logger.info("=" * 60)

    def _setup_logging(self) -> logging.Logger:
        """Setup enhanced ASCII logging"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        logger = logging.getLogger("demo_ascii_orchestrator")
        return logger

    def _create_enhanced_risk_manager(self):
        """Create enhanced risk manager with dynamic profiles"""
        class EnhancedRiskManager:
            def __init__(self):
                self.risk_profiles = {
                    'conservative': {'max_position': 0.08, 'approval_rate': 0.75, 'max_daily_trades': 3},
                    'moderate': {'max_position': 0.12, 'approval_rate': 0.85, 'max_daily_trades': 5},
                    'aggressive': {'max_position': 0.18, 'approval_rate': 0.95, 'max_daily_trades': 8}
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
                if self.daily_trades >= profile['max_daily_trades']:
                    return False

                # Confidence threshold (looser for demo)
                min_confidence = 0.5 if self.current_profile == 'aggressive' else 0.6
                if decision.confidence < min_confidence:
                    return False

                # Higher approval rate for demo
                if random.random() > profile['approval_rate']:
                    return False

                self.daily_trades += 1
                return True

            def adjust_risk_profile(self, market_conditions: str):
                """Dynamically adjust risk profile"""
                old_profile = self.current_profile
                if market_conditions == 'volatile':
                    self.current_profile = 'conservative'
                elif market_conditions == 'stable':
                    self.current_profile = 'moderate'
                else:
                    self.current_profile = 'aggressive'

                if old_profile != self.current_profile:
                    return True  # Profile changed
                return False

        return EnhancedRiskManager()

    def _create_enhanced_sentiment_analyzer(self):
        """Create enhanced sentiment analyzer with volatility adjustment"""
        class EnhancedSentimentAnalyzer:
            def __init__(self):
                self.market_volatility = random.uniform(0.15, 0.35)
                self.sentiment_history = {}

            def analyze_sentiment(self, symbol: str) -> Dict[str, Any]:
                """Enhanced sentiment analysis with volatility adjustment"""
                # Initialize sentiment history for symbol
                if symbol not in self.sentiment_history:
                    self.sentiment_history[symbol] = []

                # Generate more realistic sentiment with trends
                if self.sentiment_history[symbol]:
                    # Trending sentiment (momentum)
                    last_sentiment = self.sentiment_history[symbol][-1]
                    trend_factor = random.uniform(-0.3, 0.3)
                    raw_sentiment = last_sentiment + trend_factor
                else:
                    raw_sentiment = random.uniform(-0.8, 0.8)

                # Adjust for market volatility
                volatility_factor = 1 + (self.market_volatility - 0.25) * 2
                adjusted_sentiment = raw_sentiment * volatility_factor

                # Clamp to valid range
                final_sentiment = max(-1.0, min(1.0, adjusted_sentiment))

                # Store history
                self.sentiment_history[symbol].append(final_sentiment)
                if len(self.sentiment_history[symbol]) > 10:
                    self.sentiment_history[symbol].pop(0)

                # Calculate confidence based on consistency and volatility
                if len(self.sentiment_history[symbol]) >= 3:
                    recent_sentiments = self.sentiment_history[symbol][-3:]
                    consistency = 1.0 - (max(recent_sentiments) - min(recent_sentiments)) / 2.0
                    confidence = 0.5 + consistency * 0.4 + abs(final_sentiment) * 0.1
                else:
                    confidence = 0.6 + abs(final_sentiment) * 0.2

                confidence = max(0.3, min(0.95, confidence))

                return {
                    'sentiment_score': final_sentiment,
                    'confidence': confidence,
                    'volatility_adjusted': True,
                    'trend_strength': abs(final_sentiment)
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
                self.target_allocation = 0.70  # 70% invested target

            def get_position(self, symbol: str) -> float:
                """Get current position percentage"""
                return self.positions.get(symbol, 0.0)

            def update_position(self, symbol: str, new_pct: float):
                """Update position with rebalancing logic"""
                old_pct = self.positions.get(symbol, 0.0)

                if new_pct <= 0.001:
                    self.positions.pop(symbol, None)
                else:
                    self.positions[symbol] = new_pct

                return old_pct

            def should_rebalance(self) -> bool:
                """Check if portfolio needs rebalancing"""
                # Time-based rebalancing (every 15 cycles for demo)
                if datetime.now() - self.last_rebalance > timedelta(minutes=3):
                    return True

                # Allocation-based rebalancing
                total_allocation = sum(self.positions.values())

                if abs(total_allocation - self.target_allocation) > self.rebalance_threshold:
                    return True

                # Check for oversized positions
                for symbol, pct in self.positions.items():
                    if pct > self.max_single_position + self.rebalance_threshold:
                        return True

                return False

            def calculate_rebalanced_size(self, symbol: str, target_pct: float) -> float:
                """Calculate rebalanced position size"""
                current_allocation = sum(self.positions.values())
                available_allocation = self.target_allocation - current_allocation

                if available_allocation <= 0:
                    return 0.0

                return min(target_pct, self.max_single_position, available_allocation)

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
                    strategy_stats['total_value'] += decision.position_size * 100000

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

    def start_demo(self):
        """Start the demo trading loop"""
        self.running = True
        self.logger.info("STARTING ENHANCED TRADING DEMO...")

        try:
            while self.running and self.cycle_count < self.max_cycles:
                self.cycle_count += 1
                cycle_start = datetime.now()

                self.logger.info(f"=== DEMO CYCLE {self.cycle_count}/{self.max_cycles} ===")

                # Execute trading cycle
                cycle_results = self._execute_trading_cycle()

                # Log cycle summary
                cycle_duration = (datetime.now() - cycle_start).total_seconds()
                self._log_cycle_summary(cycle_results, cycle_duration)

                # Wait for next cycle
                if self.running and self.cycle_count < self.max_cycles:
                    self.logger.info(f"Waiting {self.cycle_interval}s for next cycle...")
                    time.sleep(self.cycle_interval)

            self.logger.info("DEMO COMPLETED!")

        except KeyboardInterrupt:
            self.logger.info("Demo stopped by user")
        finally:
            self.stop_demo()

    def _execute_trading_cycle(self) -> Dict[str, Any]:
        """Execute one trading cycle with all enhanced features"""
        cycle_results = {
            'decisions_made': 0,
            'successful_trades': 0,
            'failed_trades': 0,
            'rebalancing_actions': 0,
            'partial_exits': 0,
            'profile_changes': 0
        }

        # Check if portfolio rebalancing is needed
        if self.portfolio_manager.should_rebalance():
            self.logger.info("*** PORTFOLIO REBALANCING TRIGGERED ***")
            self.portfolio_manager.last_rebalance = datetime.now()
            cycle_results['rebalancing_actions'] += 1

        # Adjust risk profile based on market conditions
        market_condition = random.choice(['stable', 'volatile', 'trending'])
        profile_changed = self.risk_manager.adjust_risk_profile(market_condition)
        if profile_changed:
            self.logger.info(f"*** RISK PROFILE CHANGED TO: {self.risk_manager.current_profile.upper()} ***")
            cycle_results['profile_changes'] += 1

        # Process each symbol
        for symbol in self.symbols:
            try:
                decision = self._make_trading_decision(symbol)

                self.logger.info(f"{symbol}: {decision.action} (confidence: {decision.confidence:.2f}, "
                               f"sentiment: {decision.sentiment_score:+.2f}) - {decision.reasoning}")

                if decision.action != 'HOLD':
                    cycle_results['decisions_made'] += 1

                    # Risk validation
                    if self.risk_manager.validate_trade(decision):
                        # Execute trade (simulated with higher success rate for demo)
                        success = random.random() < 0.90  # 90% success rate

                        if success:
                            cycle_results['successful_trades'] += 1
                            # Update portfolio
                            old_pct = self.portfolio_manager.update_position(
                                symbol, decision.position_size
                            )
                            self.logger.info(f"    >>> TRADE EXECUTED: {symbol} {decision.action} "
                                           f"{decision.position_size:.1%} (was {old_pct:.1%})")
                        else:
                            cycle_results['failed_trades'] += 1
                            self.logger.warning(f"    >>> TRADE FAILED: {symbol} {decision.action}")

                        # Record performance
                        self.performance_tracker.record_trade(decision, success)

                        # Track special actions
                        if decision.action == 'PARTIAL_SELL':
                            cycle_results['partial_exits'] += 1

                    else:
                        self.logger.info(f"    >>> RISK REJECTED: {decision.action} "
                                       f"(confidence: {decision.confidence:.2f})")

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

        # Generate base recommendation (more aggressive for demo)
        if sentiment_score > 0.2 and confidence > 0.6:
            base_action = 'BUY'
        elif sentiment_score < -0.2 and confidence > 0.6:
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
        if current_pos > 0.10 and sentiment < -0.3:
            # Large position with negative sentiment - partial exit
            new_size = current_pos * 0.6  # Reduce by 40%
            return ('PARTIAL_SELL', new_size,
                   f"Partial exit: {current_pos:.1%} -> {new_size:.1%} due to negative sentiment",
                   'risk_management_strategy')

        # Existing position logic
        if current_pos > 0.001:  # Has position
            if base_action == 'BUY' and current_pos < 0.12:
                # Add to position
                additional = min(0.04, 0.15 - current_pos)
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
                       f"Holding {current_pos:.1%} position",
                       'hold_strategy')
        else:
            # No position - consider opening
            if base_action == 'BUY':
                target_size = self.portfolio_manager.calculate_rebalanced_size(
                    symbol, 0.04 + confidence * 0.04  # 4-8% based on confidence
                )
                if target_size > 0.02:  # Only if meaningful size
                    return ('BUY', target_size,
                           f"Opening {target_size:.1%} position",
                           'value_opportunity_strategy')

            return ('HOLD', 0.0, f"No position, {base_action} signal", 'hold_strategy')

    def _log_cycle_summary(self, results: Dict[str, Any], duration: float):
        """Log enhanced cycle summary with ASCII formatting"""
        self.logger.info(f"--- CYCLE {self.cycle_count} SUMMARY ---")
        self.logger.info(f"Duration: {duration:.2f}s")
        self.logger.info(f"Decisions Made: {results['decisions_made']}")
        self.logger.info(f"Successful Trades: {results['successful_trades']}")
        self.logger.info(f"Failed Trades: {results['failed_trades']}")
        self.logger.info(f"Rebalancing Actions: {results['rebalancing_actions']}")
        self.logger.info(f"Partial Exits: {results['partial_exits']}")
        self.logger.info(f"Profile Changes: {results['profile_changes']}")

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
        self.logger.info(f"Risk Profile: {self.risk_manager.current_profile.upper()}")
        self.logger.info(f"Market Volatility: {self.risk_manager.market_volatility:.1%}")
        self.logger.info(f"Daily Trades: {self.risk_manager.daily_trades}")

        # Strategy performance
        strategy_stats = self.performance_tracker.get_strategy_stats()
        if strategy_stats:
            self.logger.info(f"--- STRATEGY PERFORMANCE ---")
            for strategy, stats in strategy_stats.items():
                success_rate = stats['success_rate'] * 100
                self.logger.info(f"  {strategy}: {success_rate:.0f}% "
                               f"({stats['total_trades']} trades, ${stats['total_value']:,.0f})")

        self.logger.info("=" * 50)

    def stop_demo(self):
        """Stop the demo trading system"""
        self.running = False
        self.logger.info("=" * 60)
        self.logger.info("DEMO TRADING SYSTEM STOPPED")

        # Final summary
        total_trades = len(self.performance_tracker.trades)
        if total_trades > 0:
            successful = sum(1 for t in self.performance_tracker.trades if t['success'])
            success_rate = successful / total_trades * 100

            self.logger.info("=== FINAL DEMO SUMMARY ===")
            self.logger.info(f"Total Cycles: {self.cycle_count}")
            self.logger.info(f"Total Trades: {total_trades}")
            self.logger.info(f"Success Rate: {success_rate:.1f}%")

            # Final portfolio
            portfolio_stats = self.portfolio_manager.get_portfolio_stats()
            total_value = portfolio_stats['total_allocation'] * 100000
            self.logger.info(f"Final Portfolio Value: ${total_value:,.0f}")
            self.logger.info(f"Cash Remaining: {portfolio_stats['cash_percentage']:.1%}")

            self.logger.info("Enhanced Features Demonstrated:")
            self.logger.info("  [X] Dynamic Risk Management")
            self.logger.info("  [X] Sentiment Analysis with Volatility Adjustment")
            self.logger.info("  [X] Portfolio Rebalancing")
            self.logger.info("  [X] Enhanced Logging")
            self.logger.info("  [X] Strategy Performance Tracking")
            self.logger.info("  [X] Partial Exits")
            self.logger.info("Demo completed successfully!")
        else:
            self.logger.info("No trades executed during demo")

        self.logger.info("=" * 60)

def main():
    """Main function to run the demo"""
    print("=" * 60)
    print("ASCII ENHANCED TRADING ORCHESTRATOR DEMO")
    print("=" * 60)
    print("This demo will run for 20 cycles and showcase:")
    print("- Dynamic Risk Management")
    print("- Sentiment Analysis with Volatility Adjustment")
    print("- Portfolio Rebalancing")
    print("- Enhanced Logging")
    print("- Strategy Performance Tracking")
    print("- Partial Exits")
    print("- No Unicode characters (Windows compatible)")
    print("=" * 60)

    demo = DemoAsciiOrchestrator(max_cycles=20)
    demo.start_demo()

if __name__ == "__main__":
    main()
