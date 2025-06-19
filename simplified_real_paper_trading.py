# -*- coding: utf-8 -*-
"""
Simplified Real Paper Trading - Enhanced Position-Aware Logic

Demonstrates enhanced position-aware trading without Django dependencies.
Shows how the ExecutionOrchestrator integration would work in practice.
"""

import os
import time
import logging
import random
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass

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
class EnhancedTradingDecision:
    """Enhanced trading decision with sentiment and position awareness"""
    symbol: str
    base_recommendation: str  # 'BUY', 'SELL', 'HOLD'
    final_action: str        # 'BUY', 'BUY_MORE', 'SELL', 'CONTINUE_HOLDING', 'HOLD'
    confidence: float
    sentiment_score: float
    strategy_name: str
    reasoning: str

    # Position context
    current_position_pct: float
    target_position_pct: float
    is_position_addition: bool

@dataclass
class ExecutionResult:
    """Mock execution result for demonstration"""
    was_successful: bool
    execution_summary: str
    executed_shares: float
    executed_amount: float
    average_price: float
    error_message: str = ""

class MockExecutionOrchestrator:
    """Mock ExecutionOrchestrator for demonstration"""

    def __init__(self):
        self.execution_count = 0
        self.success_rate = 0.9  # 90% success rate

    def execute_approved_decision(self, risk_validation, strategy_name: str, strategy_confidence: float) -> ExecutionResult:
        """Mock trade execution"""
        self.execution_count += 1

        # Simulate occasional failures
        if random.random() > self.success_rate:
            return ExecutionResult(
                was_successful=False,
                execution_summary="",
                executed_shares=0.0,
                executed_amount=0.0,
                average_price=0.0,
                error_message="Mock execution failure for testing"
            )

        # Mock successful execution
        symbol = risk_validation.symbol
        shares = abs(risk_validation.quantity)
        price = random.uniform(100, 200)  # Mock price

        return ExecutionResult(
            was_successful=True,
            execution_summary=f"Executed {shares:.0f} shares of {symbol} @ ${price:.2f}",
            executed_shares=shares,
            executed_amount=shares * price,
            average_price=price
        )

@dataclass
class RiskValidationRequest:
    """Mock risk validation request"""
    symbol: str
    side: str  # 'BUY' or 'SELL'
    quantity: float
    strategy_name: str
    strategy_confidence: float

@dataclass
class RiskValidationResult:
    """Mock risk validation result"""
    approved: bool
    symbol: str
    quantity: float
    rejection_reason: str = ""

class MockRiskManager:
    """Mock risk manager for demonstration"""

    def validate_risk_comprehensive(self, request: RiskValidationRequest) -> RiskValidationResult:
        """Mock risk validation - approve most trades"""
        # Simulate risk blocking occasionally
        if random.random() < 0.1:  # 10% rejection rate
            return RiskValidationResult(
                approved=False,
                symbol=request.symbol,
                quantity=request.quantity,
                rejection_reason="Mock risk limit exceeded"
            )

        return RiskValidationResult(
            approved=True,
            symbol=request.symbol,
            quantity=request.quantity
        )

class MockPositionSizer:
    """Enhanced mock position sizer with realistic position tracking"""

    def __init__(self):
        # Track positions across trades
        self.positions = {}
        self.portfolio_value = 100000.0  # $100k portfolio

    def get_current_size(self, symbol: str) -> float:
        """Get current position as percentage of portfolio"""
        return self.positions.get(symbol, 0.0)

    def size_up(self, symbol: str, target_pct: float):
        """Calculate size change needed"""
        current_pct = self.get_current_size(symbol)
        size_change = target_pct - current_pct

        # Mock size calculation object
        class SizeCalc:
            def __init__(self, symbol, target_pct, size_change, portfolio_value):
                self.symbol = symbol
                self.target_size_pct = target_pct
                self.size_change_pct = size_change
                self.portfolio_value = portfolio_value
                self.target_dollar_amount = target_pct * portfolio_value
                self.trade_dollar_amount = abs(size_change * portfolio_value)
                self.shares_to_trade = self.trade_dollar_amount / 150.0  # Mock $150/share
                self.needs_trade = abs(size_change) > 0.005  # 0.5% minimum

        return SizeCalc(symbol, target_pct, size_change, self.portfolio_value)

    def update_position(self, symbol: str, new_pct: float):
        """Update position after trade"""
        self.positions[symbol] = new_pct

class EnhancedPositionAwareAnalyzer:
    """Position-aware decision logic with sentiment-based sizing"""

    def __init__(self, position_sizer: MockPositionSizer, logger: logging.Logger):
        self.position_sizer = position_sizer
        self.logger = logger

        # Enhanced decision rules configuration
        self.config = {
            'min_sentiment_for_addition': 0.7,    # Minimum sentiment to add to position
            'max_position_addition_pct': 0.03,    # Max 3% addition per trade
            'max_total_position_pct': 0.15,       # Absolute maximum per symbol (15%)
            'base_position_range': (0.03, 0.12), # 3-12% base sizing range
            'sentiment_modifier_range': (0.8, 1.2) # Sentiment multiplier range
        }

    def calculate_initial_position_size(self, confidence: float, sentiment_score: float) -> float:
        """Calculate initial position size based on confidence and sentiment"""
        # Base size from confidence (3-12% range)
        min_size, max_size = self.config['base_position_range']
        confidence_size = min_size + (confidence - 0.5) * (max_size - min_size) * 2
        confidence_size = max(min_size, min(max_size, confidence_size))

        # Sentiment modifier (0.8x to 1.2x)
        sentiment_modifier = 1.0 + (sentiment_score * 0.2)
        sentiment_modifier = max(0.8, min(1.2, sentiment_modifier))

        # Calculate final size
        target_size = confidence_size * sentiment_modifier

        # Enforce limits: 3% minimum, 15% maximum
        return max(0.03, min(0.15, target_size))

    def calculate_additional_size(self, current_size: float, sentiment_score: float) -> float:
        """Calculate additional position size for existing holdings"""
        if sentiment_score < self.config['min_sentiment_for_addition']:
            return 0.0

        # Add 1-3% based on sentiment strength above 0.7 threshold
        sentiment_excess = sentiment_score - self.config['min_sentiment_for_addition']
        max_excess = 1.0 - self.config['min_sentiment_for_addition']  # 0.3 for 0.7 threshold

        additional = 0.01 + (sentiment_excess / max_excess) * 0.02  # 1-3% range

        # Don't exceed maximum total position or addition limits
        max_additional = min(
            self.config['max_position_addition_pct'],
            self.config['max_total_position_pct'] - current_size
        )

        return min(additional, max_additional)

    def analyze_position_aware_decision(self, symbol: str, base_recommendation: str,
                                      confidence: float, sentiment_score: float) -> EnhancedTradingDecision:
        """Apply position-aware decision logic from enhanced decision rules"""

        # Get current position
        current_position_pct = self.position_sizer.get_current_size(symbol)

        self.logger.info(f"📊 {symbol} Analysis: Current position: {current_position_pct:.1%}, "
                        f"Recommendation: {base_recommendation}, Sentiment: {sentiment_score:.2f}")

        # Apply position-aware logic
        if current_position_pct > 0:
            # Already holding position - apply position-aware rules
            final_action, target_pct, reasoning = self._handle_existing_position(
                symbol, base_recommendation, current_position_pct, sentiment_score, confidence
            )
        else:
            # No current position - normal logic
            final_action, target_pct, reasoning = self._handle_new_position(
                base_recommendation, sentiment_score, confidence
            )

        # Determine strategy based on action and sentiment
        strategy_name = self._determine_strategy(final_action, sentiment_score)

        return EnhancedTradingDecision(
            symbol=symbol,
            base_recommendation=base_recommendation,
            final_action=final_action,
            confidence=confidence,
            sentiment_score=sentiment_score,
            strategy_name=strategy_name,
            reasoning=reasoning,
            current_position_pct=current_position_pct,
            target_position_pct=target_pct,
            is_position_addition=(final_action == 'BUY_MORE')
        )

    def _handle_existing_position(self, symbol: str, recommendation: str, current_pct: float,
                                sentiment_score: float, confidence: float) -> Tuple[str, float, str]:
        """Handle decisions for existing positions"""

        if recommendation == 'BUY':
            if (sentiment_score >= self.config['min_sentiment_for_addition'] and
                current_pct < self.config['max_total_position_pct']):
                # High sentiment allows adding to position
                additional_size = self.calculate_additional_size(current_pct, sentiment_score)
                if additional_size > 0:
                    target_pct = current_pct + additional_size
                    reasoning = f"Adding {additional_size:.1%} to position due to strong sentiment ({sentiment_score:.2f})"
                    return "BUY_MORE", target_pct, reasoning

            # Normal buy signal becomes hold when already positioned
            reasoning = f"Already holding {current_pct:.1%}, continuing to hold"
            return "CONTINUE_HOLDING", current_pct, reasoning

        elif recommendation == 'SELL':
            reasoning = f"Selling entire {current_pct:.1%} position"
            return "SELL", 0.0, reasoning

        else:  # recommendation == 'HOLD'
            reasoning = f"Holding current {current_pct:.1%} position"
            return "CONTINUE_HOLDING", current_pct, reasoning

    def _handle_new_position(self, recommendation: str, sentiment_score: float,
                           confidence: float) -> Tuple[str, float, str]:
        """Handle decisions for new positions"""

        if recommendation == 'BUY':
            target_pct = self.calculate_initial_position_size(confidence, sentiment_score)
            reasoning = f"Opening {target_pct:.1%} position (confidence: {confidence:.2f}, sentiment: {sentiment_score:.2f})"
            return "BUY", target_pct, reasoning
        else:
            reasoning = f"No position, recommendation: {recommendation}"
            return "HOLD", 0.0, reasoning

    def _determine_strategy(self, action: str, sentiment_score: float) -> str:
        """Determine strategy name based on action and sentiment"""
        if action in ['BUY', 'BUY_MORE']:
            if sentiment_score > 0.5:
                return 'sentiment_momentum_strategy'
            else:
                return 'value_opportunity_strategy'
        elif action == 'SELL':
            return 'risk_management_strategy'
        else:
            return 'hold_strategy'

class SimplifiedRealPaperTrading:
    """Simplified real paper trading with enhanced position-aware logic"""

    def __init__(self, symbols: List[str], cycle_interval_seconds: int = 120):
        self.symbols = symbols
        self.cycle_interval_seconds = cycle_interval_seconds
        self.logger = self._setup_logging()
        self.start_time = datetime.now()
        self.cycle_count = 0

        # Initialize mock components
        self.position_sizer = MockPositionSizer()
        self.risk_manager = MockRiskManager()
        self.execution_orchestrator = MockExecutionOrchestrator()

        # Initialize enhanced analyzer
        self.position_analyzer = EnhancedPositionAwareAnalyzer(
            self.position_sizer, self.logger
        )

        # Performance tracking
        self.portfolio_performance = {
            'start_value': 100000.0,
            'current_value': 100000.0,
            'total_trades': 0,
            'successful_trades': 0,
            'failed_trades': 0,
            'position_additions': 0,
            'strategy_performance': {}
        }

        self.logger.info("🚀 Simplified Real Paper Trading System initialized")
        self.logger.info(f"📊 Symbols: {symbols}")
        self.logger.info(f"⏱️  Cycle interval: {cycle_interval_seconds}s")
        self.logger.info("🧠 Enhanced with position-aware decision logic")

    def _setup_logging(self) -> logging.Logger:
        """Setup logging"""
        logger = logging.getLogger("simplified_paper_trading")
        logger.setLevel(logging.INFO)

        # Console handler
        console_handler = logging.StreamHandler()
        console_formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s'
        )
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)

        return logger

    def run_trading_system(self):
        """Run the simplified trading system"""
        self.logger.info("🎯 Starting Simplified Real Paper Trading System...")

        try:
            while True:
                cycle_start = datetime.now()
                self.cycle_count += 1

                self.logger.info(f"📈 === Enhanced Trading Cycle {self.cycle_count} ===")

                # Execute trading cycle
                cycle_results = self._execute_trading_cycle()

                # Log cycle summary
                cycle_duration = (datetime.now() - cycle_start).total_seconds()
                self._log_cycle_summary(cycle_results, cycle_duration)

                # Wait for next cycle
                self.logger.info(f"⏸️  Waiting {self.cycle_interval_seconds}s for next cycle...")
                time.sleep(self.cycle_interval_seconds)

        except KeyboardInterrupt:
            self.logger.info("🛑 Trading system stopped by user")
        except Exception as e:
            self.logger.error(f"❌ Unexpected error: {e}")

    def _execute_trading_cycle(self) -> Dict[str, Any]:
        """Execute enhanced trading cycle with position-aware logic"""
        cycle_results = {
            'decisions_made': 0,
            'trades_executed': 0,
            'successful_trades': 0,
            'failed_trades': 0,
            'position_additions': 0,
            'trade_details': []
        }

        for symbol in self.symbols:
            try:
                # Step 1: Perform market analysis
                market_analysis = self._perform_market_analysis(symbol)

                # Step 2: Generate base recommendation
                base_recommendation = self._generate_base_recommendation(market_analysis)

                # Step 3: Apply position-aware decision logic
                enhanced_decision = self.position_analyzer.analyze_position_aware_decision(
                    symbol, base_recommendation, market_analysis.confidence, market_analysis.sentiment_score
                )

                # Step 4: Log decision
                self.logger.info(f"🎯 {symbol}: {enhanced_decision.final_action} "
                               f"(confidence: {enhanced_decision.confidence:.2f}, "
                               f"sentiment: {enhanced_decision.sentiment_score:.2f})")

                if enhanced_decision.reasoning:
                    self.logger.info(f"   📝 {enhanced_decision.reasoning}")

                # Step 5: Execute if action needed
                if enhanced_decision.final_action not in ['HOLD', 'CONTINUE_HOLDING']:
                    cycle_results['decisions_made'] += 1

                    if enhanced_decision.is_position_addition:
                        cycle_results['position_additions'] += 1
                        self.logger.info(f"   ➕ POSITION ADDITION DETECTED for {symbol}")

                    # Execute through mock ExecutionOrchestrator
                    execution_result = self._execute_enhanced_trade(enhanced_decision)

                    if execution_result and execution_result.was_successful:
                        cycle_results['trades_executed'] += 1
                        cycle_results['successful_trades'] += 1
                        cycle_results['trade_details'].append({
                            'symbol': symbol,
                            'action': enhanced_decision.final_action,
                            'shares': execution_result.executed_shares,
                            'price': execution_result.average_price,
                            'strategy': enhanced_decision.strategy_name,
                            'sentiment': enhanced_decision.sentiment_score
                        })

                        self.logger.info(f"✅ PAPER TRADE EXECUTED: {execution_result.execution_summary}")

                        # Update position tracker
                        self.position_sizer.update_position(symbol, enhanced_decision.target_position_pct)

                        # Track strategy performance
                        strategy = enhanced_decision.strategy_name
                        if strategy not in self.portfolio_performance['strategy_performance']:
                            self.portfolio_performance['strategy_performance'][strategy] = {
                                'trades': 0, 'successes': 0, 'total_amount': 0.0
                            }

                        perf = self.portfolio_performance['strategy_performance'][strategy]
                        perf['trades'] += 1
                        perf['successes'] += 1
                        perf['total_amount'] += abs(execution_result.executed_amount)

                    else:
                        cycle_results['failed_trades'] += 1
                        error = execution_result.error_message if execution_result else "Unknown error"
                        self.logger.warning(f"⚠️  Trade failed for {symbol}: {error}")

            except Exception as e:
                self.logger.error(f"❌ Error processing {symbol}: {e}")

        return cycle_results

    def _perform_market_analysis(self, symbol: str) -> MarketAnalysis:
        """Perform mock market analysis with realistic patterns"""
        # Mock sentiment analysis that varies by symbol and time
        base_sentiments = {
            'AAPL': 0.2, 'MSFT': 0.1, 'GOOGL': 0.0, 'TSLA': 0.3, 'NVDA': 0.2
        }

        base_sentiment = base_sentiments.get(symbol, 0.0)
        # Add some realistic variation
        sentiment_variation = random.uniform(-0.4, 0.4)
        sentiment_score = max(-1.0, min(1.0, base_sentiment + sentiment_variation))

        # Convert to label
        if sentiment_score > 0.2:
            sentiment_label = 'positive'
        elif sentiment_score < -0.2:
            sentiment_label = 'negative'
        else:
            sentiment_label = 'neutral'

        # Generate realistic confidence
        confidence = random.uniform(0.6, 0.9)

        return MarketAnalysis(
            symbol=symbol,
            sentiment_score=sentiment_score,
            sentiment_label=sentiment_label,
            confidence=confidence,
            news_count=random.randint(2, 5),
            analysis_time=datetime.now()
        )

    def _generate_base_recommendation(self, analysis: MarketAnalysis) -> str:
        """Generate base recommendation from market analysis"""
        # Simple logic based on sentiment and confidence
        if analysis.sentiment_score > 0.3 and analysis.confidence > 0.7:
            return 'BUY'
        elif analysis.sentiment_score < -0.3:
            return 'SELL'
        else:
            return 'HOLD'

    def _execute_enhanced_trade(self, decision: EnhancedTradingDecision):
        """Execute enhanced trading decision through mock ExecutionOrchestrator"""
        try:
            # Calculate size using position sizer
            size_calc = self.position_sizer.size_up(decision.symbol, decision.target_position_pct)

            if not size_calc.needs_trade:
                self.logger.info(f"📊 {decision.symbol}: No trade needed (below minimum threshold)")
                return None

            # Create risk validation request
            risk_request = RiskValidationRequest(
                symbol=decision.symbol,
                side='BUY' if decision.final_action in ['BUY', 'BUY_MORE'] else 'SELL',
                quantity=abs(size_calc.shares_to_trade),
                strategy_name=decision.strategy_name,
                strategy_confidence=decision.confidence
            )

            # Get risk validation
            risk_result = self.risk_manager.validate_risk_comprehensive(risk_request)

            if not risk_result.approved:
                self.logger.warning(f"🛡️  Risk blocked {decision.symbol}: {risk_result.rejection_reason}")
                return None

            # Execute through ExecutionOrchestrator
            execution_result = self.execution_orchestrator.execute_approved_decision(
                risk_validation=risk_result,
                strategy_name=decision.strategy_name,
                strategy_confidence=decision.confidence
            )

            return execution_result

        except Exception as e:
            self.logger.error(f"❌ Error executing enhanced trade for {decision.symbol}: {e}")
            return None

    def _log_cycle_summary(self, cycle_results: Dict[str, Any], cycle_duration: float):
        """Enhanced cycle summary logging"""
        self.logger.info(f"📊 Enhanced Cycle {self.cycle_count} Summary:")
        self.logger.info(f"   ⏱️  Duration: {cycle_duration:.1f}s")
        self.logger.info(f"   🎯 Decisions: {cycle_results['decisions_made']}")
        self.logger.info(f"   ✅ Successful trades: {cycle_results['successful_trades']}")
        self.logger.info(f"   ❌ Failed trades: {cycle_results['failed_trades']}")
        self.logger.info(f"   ➕ Position additions: {cycle_results['position_additions']}")

        # Update performance tracking
        self.portfolio_performance['total_trades'] += cycle_results['trades_executed']
        self.portfolio_performance['successful_trades'] += cycle_results['successful_trades']
        self.portfolio_performance['failed_trades'] += cycle_results['failed_trades']
        self.portfolio_performance['position_additions'] += cycle_results['position_additions']

        # Log current holdings
        self.logger.info(f"   🏦 Current Portfolio Positions:")
        for symbol in self.symbols:
            current_pct = self.position_sizer.get_current_size(symbol)
            if current_pct > 0:
                value = current_pct * self.position_sizer.portfolio_value
                self.logger.info(f"      {symbol}: {current_pct:.1%} (${value:,.0f})")

        # Log enhanced trade details
        for trade in cycle_results['trade_details']:
            sentiment_emoji = "📈" if trade['sentiment'] > 0.2 else "📉" if trade['sentiment'] < -0.2 else "➡️"
            self.logger.info(
                f"   💼 {trade['symbol']}: {trade['action']} {abs(trade['shares']):.0f} @ ${trade['price']:.2f} "
                f"({trade['strategy']}) {sentiment_emoji}"
            )

        # Log strategy performance summary
        if self.portfolio_performance['strategy_performance']:
            self.logger.info("   📈 Strategy Performance:")
            for strategy, perf in self.portfolio_performance['strategy_performance'].items():
                success_rate = perf['successes'] / max(perf['trades'], 1) * 100
                self.logger.info(f"      {strategy}: {success_rate:.0f}% ({perf['trades']} trades, ${perf['total_amount']:,.0f})")

def main():
    """Main function"""
    print("🚀 Simplified Real Paper Trading - Enhanced Position Logic")
    print("=" * 60)
    print("📝 Demonstrates enhanced decision rules from enhanced_decision_rules.md")
    print("🎯 Uses position-aware logic with sentiment-based sizing")
    print("📊 Mock ExecutionOrchestrator integration for testing")
    print()

    # Configuration
    symbols = ['AAPL', 'MSFT', 'GOOGL', 'TSLA', 'NVDA']
    cycle_interval = 30  # 30 seconds for testing

    print(f"📋 Trading symbols: {symbols}")
    print(f"⏱️  Cycle interval: {cycle_interval}s")
    print()

    # Start system
    system = SimplifiedRealPaperTrading(symbols, cycle_interval)
    system.run_trading_system()

if __name__ == "__main__":
    main()
