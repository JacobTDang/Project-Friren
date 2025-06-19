#!/usr/bin/env python3
"""
Enhanced Sentiment Discovery Demo

Demonstrates the new positive sentiment discovery system that:
1. Monitors news sentiment across the market
2. Queues high-confidence positive sentiment stocks
3. Evaluates candidates for watchlist addition
4. Integrates with existing trading system
"""

import sys
import time
from pathlib import Path
from datetime import datetime

# Add project root to path
project_root = Path(__file__).parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

try:
    from Friren_V1.trading_engine.orchestrator import UnifiedOrchestrator
    print("✅ Successfully imported UnifiedOrchestrator")
except ImportError as e:
    print(f"❌ Import error: {e}")
    sys.exit(1)

def create_demo_symbols():
    """Create symbols for demo"""
    core_symbols = ['AAPL', 'MSFT', 'GOOGL']
    return core_symbols

def simulate_positive_sentiment(orchestrator):
    """Simulate finding positive sentiment stocks"""
    print("\\n🔍 Simulating positive sentiment discovery...")

    # Mock positive sentiment candidates
    candidates = [
        ('AMD', 0.72, 0.85, ['AMD announces breakthrough AI chip']),
        ('META', 0.65, 0.78, ['Meta shows strong VR growth']),
    ]

    for symbol, sentiment, confidence, headlines in candidates:
        if symbol not in orchestrator.symbols:
            print(f"📰 Found: {symbol} (sentiment: {sentiment:.2f})")

            # Create mock news data
            class MockNews:
                def __init__(self, sentiment, confidence, headlines):
                    self.overall_sentiment_score = sentiment
                    self.sentiment_confidence = confidence
                    self.news_volume = len(headlines)
                    self.key_articles = [type('MockArticle', (), {'title': h}) for h in headlines]
                    self.market_events = []
                    self.data_quality_score = 0.8

            mock_news = MockNews(sentiment, confidence, headlines)
            orchestrator._process_sentiment_for_queue(symbol, mock_news)

def run_demo():
    """Run the enhanced sentiment discovery demo"""
    print("🚀 Enhanced Sentiment Discovery Demo")
    print("=" * 50)

    # Create orchestrator
    symbols = create_demo_symbols()
    orchestrator = UnifiedOrchestrator(
        symbols=symbols,
        cycle_interval_seconds=5,
        mode="paper_trading"
    )

    print(f"📈 Initial watchlist: {', '.join(symbols)}")

    try:
        # Simulate market sentiment discovery
        simulate_positive_sentiment(orchestrator)

        print(f"📊 Queue size: {orchestrator.positive_sentiment_queue.qsize()}")

        # Run a few cycles to process the queue
        for cycle in range(3):
            print(f"\\n📊 === Cycle {cycle + 1} ===")

            # Execute cycle
            cycle_results = orchestrator._execute_trading_cycle()

            # Update status
            orchestrator.status.cycles_completed += 1

            # Show status
            queue_size = orchestrator.positive_sentiment_queue.qsize()
            watchlist_size = len(orchestrator.symbols)

            print(f"Watchlist size: {watchlist_size}")
            print(f"Queue size: {queue_size}")
            print(f"Decisions made: {len(orchestrator.watchlist_decisions)}")

            time.sleep(2)  # Brief pause

        print("\\n✅ Demo completed!")

        # Show final results
        if orchestrator.watchlist_decisions:
            print("\\n📋 Watchlist Decisions:")
            for decision in orchestrator.watchlist_decisions:
                print(f"  {decision.symbol}: {decision.decision}")

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        orchestrator.stop_trading_system()

if __name__ == "__main__":
    run_demo()
