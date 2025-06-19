# -*- coding: utf-8 -*-
"""
Test Enhanced Orchestrator - Position-Aware Logic

Tests the enhanced decision logic without requiring full ExecutionOrchestrator setup.
Demonstrates the sophisticated position-aware logic from enhanced_decision_rules.md
"""

import random
import logging
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

class MockPositionSizer:
    """Mock position sizer for testing"""

    def __init__(self):
        # Simulate some existing positions
        self.mock_positions = {
            'AAPL': 0.08,  # 8% position
            'MSFT': 0.05,  # 5% position
            'GOOGL': 0.0,  # No position
            'TSLA': 0.12,  # 12% position
            'NVDA': 0.0    # No position
        }

    def get_current_size(self, symbol: str) -> float:
        return self.mock_positions.get(symbol, 0.0)

    def update_position(self, symbol: str, new_size: float):
        """Update mock position (for testing)"""
        self.mock_positions[symbol] = new_size

class EnhancedPositionAwareAnalyzer:
    """Implements position-aware decision logic with sentiment-based sizing"""

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

def setup_logging() -> logging.Logger:
    """Setup logging for testing"""
    logger = logging.getLogger("enhanced_test")
    logger.setLevel(logging.INFO)

    # Console handler
    console_handler = logging.StreamHandler()
    console_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s'
    )
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    return logger

def perform_mock_market_analysis(symbol: str) -> MarketAnalysis:
    """Perform mock market analysis"""
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

def generate_base_recommendation(analysis: MarketAnalysis) -> str:
    """Generate base recommendation from market analysis"""
    # Simple logic based on sentiment and confidence
    if analysis.sentiment_score > 0.3 and analysis.confidence > 0.7:
        return 'BUY'
    elif analysis.sentiment_score < -0.3:
        return 'SELL'
    else:
        return 'HOLD'

def test_enhanced_position_logic():
    """Test the enhanced position-aware logic"""
    logger = setup_logging()
    position_sizer = MockPositionSizer()
    analyzer = EnhancedPositionAwareAnalyzer(position_sizer, logger)

    symbols = ['AAPL', 'MSFT', 'GOOGL', 'TSLA', 'NVDA']

    print("🚀 Testing Enhanced Position-Aware Trading Logic")
    print("=" * 60)
    print(f"📊 Current Mock Positions:")
    for symbol in symbols:
        current_pct = position_sizer.get_current_size(symbol)
        print(f"   {symbol}: {current_pct:.1%}")
    print()

    # Run multiple test cycles
    for cycle in range(1, 4):
        print(f"🔄 Test Cycle {cycle}")
        print("-" * 30)

        for symbol in symbols:
            # Generate market analysis
            analysis = perform_mock_market_analysis(symbol)
            base_recommendation = generate_base_recommendation(analysis)

            # Apply enhanced logic
            decision = analyzer.analyze_position_aware_decision(
                symbol, base_recommendation, analysis.confidence, analysis.sentiment_score
            )

            # Log results
            print(f"🎯 {symbol}:")
            print(f"   📈 Market: {analysis.sentiment_label} sentiment ({analysis.sentiment_score:.2f})")
            print(f"   💡 Base rec: {base_recommendation}")
            print(f"   ⚡ Final action: {decision.final_action}")
            print(f"   📝 {decision.reasoning}")
            print(f"   🎪 Strategy: {decision.strategy_name}")

            if decision.is_position_addition:
                print(f"   ➕ POSITION ADDITION DETECTED!")

            # Update mock position for demonstration
            if decision.final_action in ['BUY', 'BUY_MORE']:
                position_sizer.update_position(symbol, decision.target_position_pct)
            elif decision.final_action == 'SELL':
                position_sizer.update_position(symbol, 0.0)

            print()

        print(f"📊 Updated Positions after Cycle {cycle}:")
        for symbol in symbols:
            current_pct = position_sizer.get_current_size(symbol)
            print(f"   {symbol}: {current_pct:.1%}")
        print("\n" + "="*60 + "\n")

def test_specific_scenarios():
    """Test specific scenarios from enhanced decision rules"""
    logger = setup_logging()
    position_sizer = MockPositionSizer()
    analyzer = EnhancedPositionAwareAnalyzer(position_sizer, logger)

    print("🧪 Testing Specific Enhanced Decision Scenarios")
    print("=" * 60)

    # Scenario 1: High sentiment, existing position -> BUY_MORE
    print("📋 Scenario 1: High sentiment (0.8) + BUY rec + existing position (8%)")
    decision1 = analyzer.analyze_position_aware_decision('AAPL', 'BUY', 0.8, 0.8)
    print(f"   Result: {decision1.final_action} - {decision1.reasoning}")
    print(f"   Target: {decision1.target_position_pct:.1%}")
    print()

    # Scenario 2: Low sentiment, existing position -> CONTINUE_HOLDING
    print("📋 Scenario 2: Low sentiment (0.3) + BUY rec + existing position (8%)")
    decision2 = analyzer.analyze_position_aware_decision('AAPL', 'BUY', 0.7, 0.3)
    print(f"   Result: {decision2.final_action} - {decision2.reasoning}")
    print()

    # Scenario 3: New position with high sentiment
    print("📋 Scenario 3: High sentiment (0.9) + BUY rec + no position")
    decision3 = analyzer.analyze_position_aware_decision('GOOGL', 'BUY', 0.85, 0.9)
    print(f"   Result: {decision3.final_action} - {decision3.reasoning}")
    print(f"   Target: {decision3.target_position_pct:.1%}")
    print()

    # Scenario 4: Sell existing position
    print("📋 Scenario 4: SELL rec + existing position (12%)")
    decision4 = analyzer.analyze_position_aware_decision('TSLA', 'SELL', 0.8, -0.5)
    print(f"   Result: {decision4.final_action} - {decision4.reasoning}")
    print()

    # Scenario 5: Position at limit, high sentiment
    position_sizer.update_position('NVDA', 0.15)  # Set to 15% limit
    print("📋 Scenario 5: High sentiment (0.9) + BUY rec + position at limit (15%)")
    decision5 = analyzer.analyze_position_aware_decision('NVDA', 'BUY', 0.8, 0.9)
    print(f"   Result: {decision5.final_action} - {decision5.reasoning}")
    print()

if __name__ == "__main__":
    print("🔬 Enhanced Position-Aware Trading Logic Test")
    print("Implementing sophisticated rules from enhanced_decision_rules.md")
    print()

    # Run tests
    test_specific_scenarios()
    test_enhanced_position_logic()

    print("✅ Enhanced decision logic testing complete!")
    print("📝 This demonstrates the position-aware logic that will be used")
    print("   in the real paper trading orchestrator with ExecutionOrchestrator.")
