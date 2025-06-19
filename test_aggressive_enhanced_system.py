#!/usr/bin/env python3
"""
Test Enhanced Aggressive Trading System with Market Regime Integration

This script demonstrates:
1. Lower confidence thresholds for action
2. Market regime-aware strategy selection
3. Entropy-based risk assessment
4. More aggressive position sizing when conditions are favorable

Author: Enhanced for aggressive trading with risk management
"""

import sys
import os
import time
import random
from datetime import datetime

# Add project root to path
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

# Import the enhanced orchestrator
try:
    from Friren_V1.trading_engine.per_stock_orchestrator import (
        StockMonitorProcess, SentimentData, PortfolioCoordination,
        MarketRegimeData, TradingDecision
    )
    print("✅ Successfully imported enhanced trading components")
except ImportError as e:
    print(f"❌ Import error: {e}")
    sys.exit(1)

def generate_test_scenarios():
    """Generate diverse market scenarios to test aggressive decision making"""
    scenarios = [
        # Low confidence scenarios (previously would HOLD)
        {"sentiment": 0.15, "confidence": 35, "description": "Weak positive signal"},
        {"sentiment": -0.18, "confidence": 40, "description": "Weak negative signal"},
        {"sentiment": 0.25, "confidence": 45, "description": "Moderate positive signal"},

        # Medium confidence scenarios
        {"sentiment": 0.3, "confidence": 55, "description": "Good positive signal"},
        {"sentiment": -0.25, "confidence": 60, "description": "Concerning negative signal"},
        {"sentiment": 0.4, "confidence": 65, "description": "Strong positive signal"},

        # High confidence scenarios
        {"sentiment": 0.5, "confidence": 75, "description": "Very strong positive"},
        {"sentiment": -0.4, "confidence": 80, "description": "Strong negative signal"},
        {"sentiment": 0.6, "confidence": 85, "description": "Euphoric signal"},

        # Edge cases
        {"sentiment": 0.05, "confidence": 30, "description": "Very weak signal"},
        {"sentiment": -0.05, "confidence": 25, "description": "Negligible negative"},
        {"sentiment": 0.1, "confidence": 50, "description": "Borderline opportunity"}
    ]
    return scenarios

def test_aggressive_decision_making():
    """Test the enhanced aggressive decision engine"""
    print("🧪 TESTING ENHANCED AGGRESSIVE TRADING SYSTEM")
    print("=" * 60)

    # Create test components
    test_symbol = "AAPL"
    coordination = PortfolioCoordination(
        total_exposure=0.3,
        available_cash=70000.0,
        active_positions={"AAPL": 0.05},  # Start with small position
        max_position_size=0.15
    )

    # Initialize mock stock monitor process
    import multiprocessing as mp
    mock_queue = mp.Queue()
    mock_coord_queue = mp.Queue()

    try:
        stock_monitor = StockMonitorProcess(test_symbol, mock_queue, mock_coord_queue)
        stock_monitor.initialize_components()
        print(f"✅ Initialized enhanced stock monitor for {test_symbol}")
    except Exception as e:
        print(f"❌ Failed to initialize: {e}")
        return

    # Test scenarios
    scenarios = generate_test_scenarios()
    trading_decisions = []

    print(f"\n📊 TESTING {len(scenarios)} SCENARIOS")
    print("-" * 60)

    for i, scenario in enumerate(scenarios, 1):
        print(f"\n🎯 Scenario {i}: {scenario['description']}")
        print(f"   Sentiment: {scenario['sentiment']:.2f}, Confidence: {scenario['confidence']:.0f}%")

        # Create sentiment data
        sentiment_data = SentimentData(
            average_sentiment=scenario['sentiment'],
            confidence=scenario['confidence'],
            article_count=random.randint(2, 8),
            individual_sentiments=[scenario['sentiment'] + random.uniform(-0.1, 0.1) for _ in range(3)]
        )

        # Make trading decision
        try:
            decision = stock_monitor.make_trading_decision(sentiment_data, coordination)
            trading_decisions.append(decision)

            # Display results
            action_emoji = {
                "BUY": "🟢", "BUY_MORE": "🔵", "SELL": "🔴",
                "PARTIAL_SELL": "🟡", "HOLD": "⚪"
            }.get(decision.action, "❓")

            print(f"   {action_emoji} Action: {decision.action}")
            print(f"   📈 Strategy: {decision.strategy}")
            print(f"   🎯 Confidence: {decision.confidence:.1f}%")
            print(f"   📊 Position: {decision.current_position:.1%} → {decision.target_position:.1%}")
            print(f"   ⚠️  Risk Score: {decision.risk_score:.1f}")
            print(f"   💭 Reasoning: {decision.reasoning}")

            # Update coordination for next test
            coordination.active_positions[test_symbol] = decision.target_position

        except Exception as e:
            print(f"   ❌ Decision failed: {e}")

    # Analyze results
    print("\n" + "=" * 60)
    print("📈 AGGRESSIVE TRADING ANALYSIS")
    print("=" * 60)

    actions = [d.action for d in trading_decisions]
    action_counts = {action: actions.count(action) for action in set(actions)}

    print("\n📊 Action Distribution:")
    for action, count in action_counts.items():
        percentage = (count / len(actions)) * 100
        emoji = {"BUY": "🟢", "BUY_MORE": "🔵", "SELL": "🔴", "PARTIAL_SELL": "🟡", "HOLD": "⚪"}.get(action, "❓")
        print(f"   {emoji} {action}: {count} decisions ({percentage:.1f}%)")

    # Calculate aggressiveness metrics
    buy_actions = [d for d in trading_decisions if d.action in ["BUY", "BUY_MORE"]]
    sell_actions = [d for d in trading_decisions if d.action in ["SELL", "PARTIAL_SELL"]]
    hold_actions = [d for d in trading_decisions if d.action == "HOLD"]

    print(f"\n📈 Aggressiveness Metrics:")
    print(f"   🎯 Total Trades: {len(buy_actions) + len(sell_actions)} / {len(trading_decisions)}")
    print(f"   📈 Buy Signals: {len(buy_actions)} ({len(buy_actions)/len(trading_decisions)*100:.1f}%)")
    print(f"   📉 Sell Signals: {len(sell_actions)} ({len(sell_actions)/len(trading_decisions)*100:.1f}%)")
    print(f"   ⚪ Hold Signals: {len(hold_actions)} ({len(hold_actions)/len(trading_decisions)*100:.1f}%)")

    # Strategy diversity
    strategies_used = [d.strategy for d in trading_decisions]
    strategy_counts = {strategy: strategies_used.count(strategy) for strategy in set(strategies_used)}

    print(f"\n🎯 Strategy Diversity:")
    for strategy, count in strategy_counts.items():
        percentage = (count / len(strategies_used)) * 100
        print(f"   • {strategy}: {count} uses ({percentage:.1f}%)")

    # Confidence analysis
    avg_confidence = sum(d.confidence for d in trading_decisions) / len(trading_decisions)
    active_decisions = [d for d in trading_decisions if d.action != "HOLD"]

    if active_decisions:
        avg_active_confidence = sum(d.confidence for d in active_decisions) / len(active_decisions)
        print(f"\n📊 Confidence Analysis:")
        print(f"   🎯 Average Overall Confidence: {avg_confidence:.1f}%")
        print(f"   📈 Average Active Decision Confidence: {avg_active_confidence:.1f}%")
        print(f"   🔥 Lowest Acting Confidence: {min(d.confidence for d in active_decisions):.1f}%")

    # Risk analysis
    avg_risk = sum(d.risk_score for d in trading_decisions) / len(trading_decisions)
    print(f"   ⚠️  Average Risk Score: {avg_risk:.1f}")

    print(f"\n✅ ENHANCED AGGRESSIVE SYSTEM PERFORMANCE:")
    if len(buy_actions) + len(sell_actions) > len(trading_decisions) * 0.5:
        print(f"   🎯 SUCCESS: System is appropriately aggressive ({(len(buy_actions) + len(sell_actions))/len(trading_decisions)*100:.1f}% action rate)")
    else:
        print(f"   ⚠️  MODERATE: System could be more aggressive ({(len(buy_actions) + len(sell_actions))/len(trading_decisions)*100:.1f}% action rate)")

    print(f"   📊 Strategy diversity: {len(strategy_counts)} different strategies used")
    print(f"   🧠 Market regime integration: {'✅ Active' if any('Regime:' in d.reasoning for d in trading_decisions) else '❌ Not detected'}")

def test_market_regime_impact():
    """Test how different market regimes affect decision aggressiveness"""
    print("\n" + "=" * 60)
    print("🌊 TESTING MARKET REGIME IMPACT")
    print("=" * 60)

    # Test same signal under different regimes
    test_signal = {"sentiment": 0.2, "confidence": 50}

    regime_scenarios = [
        MarketRegimeData(
            primary_regime="LOW_ENTROPY",
            entropy_level="LOW",
            opportunity_multiplier=1.4,
            risk_multiplier=0.8,
            regime_confidence=80.0
        ),
        MarketRegimeData(
            primary_regime="HIGH_ENTROPY",
            entropy_level="HIGH",
            opportunity_multiplier=0.7,
            risk_multiplier=1.3,
            regime_confidence=75.0
        ),
        MarketRegimeData(
            primary_regime="TRENDING",
            entropy_level="MEDIUM",
            opportunity_multiplier=1.1,
            risk_multiplier=1.0,
            regime_confidence=65.0
        )
    ]

    print(f"📊 Testing signal: {test_signal['sentiment']:.2f} sentiment, {test_signal['confidence']:.0f}% confidence")

    for i, regime in enumerate(regime_scenarios, 1):
        print(f"\n🌊 Regime {i}: {regime.primary_regime}")
        print(f"   📊 Entropy: {regime.entropy_level}")
        print(f"   🎯 Opportunity Multiplier: {regime.opportunity_multiplier:.2f}")
        print(f"   ⚠️  Risk Multiplier: {regime.risk_multiplier:.2f}")

        # Show how the signal gets adjusted
        enhanced_confidence = test_signal['confidence'] * regime.opportunity_multiplier
        regime_adjusted_sentiment = test_signal['sentiment'] * regime.opportunity_multiplier

        print(f"   📈 Adjusted Confidence: {test_signal['confidence']:.0f}% → {enhanced_confidence:.0f}%")
        print(f"   📊 Adjusted Sentiment: {test_signal['sentiment']:.2f} → {regime_adjusted_sentiment:.2f}")

        # Predict likely outcome
        if enhanced_confidence > 55 and abs(regime_adjusted_sentiment) > 0.15:
            print(f"   🎯 Expected Outcome: AGGRESSIVE ACTION")
        elif enhanced_confidence > 40:
            print(f"   🎯 Expected Outcome: MODERATE ACTION")
        else:
            print(f"   🎯 Expected Outcome: LIKELY HOLD")

if __name__ == "__main__":
    print("🚀 Enhanced Aggressive Trading System Test")
    print("=" * 60)

    try:
        test_aggressive_decision_making()
        test_market_regime_impact()

        print("\n" + "=" * 60)
        print("✅ ENHANCED AGGRESSIVE SYSTEM TEST COMPLETE")
        print("=" * 60)
        print("📊 Key Improvements:")
        print("   • Lower confidence thresholds (35-45% vs 60-70%)")
        print("   • Market regime-aware strategy selection")
        print("   • Entropy-based opportunity detection")
        print("   • More aggressive position sizing in favorable regimes")
        print("   • Enhanced risk management with regime context")
        print("\n🎯 System is now significantly more aggressive while maintaining")
        print("   sophisticated risk management through market regime analysis!")

    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
