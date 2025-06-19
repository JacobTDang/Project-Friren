#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
demo_enhanced_orchestrator.py

Demonstration of Enhanced Orchestrator with News Pipeline Integration

This script shows how to integrate the 15-minute news collection and FinBERT
analysis pipeline with your existing orchestrator.py system.

Features demonstrated:
- Enhanced orchestrator initialization
- News pipeline configuration (15-minute intervals)
- Stock-specific tracking
- Queue-based communication with decision engine
- Real-time monitoring and status updates
"""

import asyncio
import time
import sys
import os
from typing import Optional
from datetime import datetime

# Add Friren_V1 to Python path for imports
current_dir = os.path.dirname(os.path.abspath(__file__))
friren_dir = os.path.join(current_dir, 'Friren_V1')
if friren_dir not in sys.path:
    sys.path.insert(0, friren_dir)

# Import the integration modules
try:
    from orchestrator_news_integration import (
        create_enhanced_orchestrator,
        NewsIntegrationConfig
    )
    from enhanced_news_finbert_xgboost_pipeline import NewsProcessingConfig
    INTEGRATION_AVAILABLE = True
    print("SUCCESS: Integration modules imported successfully")
except ImportError as e:
    print(f"WARNING: Integration modules not fully available: {e}")
    print("NOTE: This is expected if running without full dependencies")
    INTEGRATION_AVAILABLE = False


class EnhancedOrchestratorDemo:
    """Demonstration of the enhanced orchestrator with news pipeline"""

    def __init__(self):
        self.orchestrator = None
        self.demo_symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']

    def create_demo_configuration(self) -> Optional[NewsIntegrationConfig]:
        """Create demonstration configuration for news pipeline"""
        if not INTEGRATION_AVAILABLE:
            return None

        return NewsIntegrationConfig(
            # Enable 15-minute news collection
            enable_news_pipeline=True,
            news_collection_interval=15,  # Every 15 minutes as requested
            include_extended_hours=True,

            # Use demo symbols for testing
            use_orchestrator_symbols=False,  # We'll specify our own
            additional_symbols=self.demo_symbols,
            max_articles_per_symbol=10,

            # Resource limits (suitable for t3.micro)
            news_memory_limit_mb=250,
            finbert_batch_size=3,
            max_concurrent_processing=2,

            # Integration settings
            auto_start_with_orchestrator=True,
            send_to_decision_engine=True,
            update_shared_state=True,

            # Monitoring
            enable_performance_monitoring=True,
            alert_on_failures=True,
            max_consecutive_failures=3
        )

    def run_demo(self) -> bool:
        """Run the enhanced orchestrator demonstration"""
        print("Enhanced Orchestrator with News Pipeline Demo")
        print("=" * 60)

        if not INTEGRATION_AVAILABLE:
            print("ERROR: Integration modules not available")
            print("NOTE: Please ensure all dependencies are installed")
            return False

        try:
            # Step 1: Create configuration
            print("Step 1: Creating news pipeline configuration...")
            news_config = self.create_demo_configuration()

            print(f"   News collection interval: {news_config.news_collection_interval} minutes")
            print(f"   Monitored symbols: {news_config.additional_symbols}")
            print(f"   Memory limit: {news_config.news_memory_limit_mb}MB")

            # Step 2: Create enhanced orchestrator
            print("\nStep 2: Creating enhanced orchestrator...")
            self.orchestrator = create_enhanced_orchestrator(
                system_config=None,  # Using defaults for demo
                news_config=news_config
            )
            print("   Enhanced orchestrator created")

            # Step 3: Initialize system
            print("\nStep 3: Initializing enhanced orchestrator...")
            init_success = self.orchestrator.initialize_system()

            if init_success:
                print("   Enhanced orchestrator initialized successfully")
                print("   News pipeline ready for 15-minute intervals")
                print("   FinBERT analysis ready")
                print("   XGBoost recommendations ready")
            else:
                print("   ERROR: Enhanced orchestrator initialization failed")
                return False

            # Step 4: Start system
            print("\nStep 4: Starting enhanced orchestrator...")
            start_success = self.orchestrator.start_system()

            if start_success:
                print("   Enhanced orchestrator started successfully")
                print("   News collection will occur every 15 minutes")
                print("   Results will be sent to decision engine queue")
            else:
                print("   ERROR: Enhanced orchestrator failed to start")
                return False

            # Step 5: Monitor system
            print("\nStep 5: Monitoring system status...")
            self.monitor_system_demo()

            return True

        except Exception as e:
            print(f"ERROR: Demo failed with error: {e}")
            return False
        finally:
            # Cleanup
            if self.orchestrator:
                print("\nStep 6: Stopping enhanced orchestrator...")
                self.orchestrator.stop_system()
                print("   Enhanced orchestrator stopped")

    def monitor_system_demo(self):
        """Demonstrate system monitoring capabilities"""
        try:
            for i in range(3):  # Monitor for 3 cycles
                print(f"\nMonitoring Cycle {i+1}/3:")

                # Get system status
                status = self.orchestrator.get_system_status()

                print(f"   Integration Status: {status['integration_status']}")
                print(f"   News Pipeline: {status['news_pipeline']['state']}")
                print(f"   News Pipeline Enabled: {status['news_pipeline_enabled']}")

                # Get news pipeline metrics if available
                try:
                    metrics = self.orchestrator.get_news_pipeline_metrics()
                    if 'basic_metrics' in metrics:
                        basic = metrics['basic_metrics']
                        print(f"   Cycles Completed: {basic.get('cycles_completed', 0)}")
                        print(f"   Articles Analyzed: {basic.get('articles_analyzed', 0)}")
                        print(f"   Recommendations: {basic.get('recommendations_generated', 0)}")

                        if 'health_indicators' in metrics:
                            health = metrics['health_indicators']['status_health']
                            print(f"   Health Status: {health}")
                    else:
                        print(f"   Metrics: {metrics.get('error', 'Not yet available')}")

                except Exception as e:
                    print(f"   Metrics unavailable: {e}")

                # Show next run estimate
                if 'news_pipeline' in status and 'next_run_estimate' in status['news_pipeline']:
                    next_run = status['news_pipeline']['next_run_estimate']
                    print(f"   Next Collection: {next_run}")

                # Show configuration
                config = status.get('config', {})
                print(f"   Config: {config.get('news_interval_minutes', 'N/A')} min intervals")
                symbols = config.get('monitored_symbols', [])
                print(f"   Monitoring: {len(symbols)} symbols {symbols[:3]}{'...' if len(symbols) > 3 else ''}")

                if i < 2:  # Don't sleep on last iteration
                    print("   Waiting 5 seconds...")
                    time.sleep(5)

        except Exception as e:
            print(f"   Monitoring error: {e}")

    def demonstrate_symbol_update(self):
        """Demonstrate dynamic symbol list updates"""
        if not self.orchestrator:
            return

        print("\nDemonstrating symbol list updates...")

        new_symbols = ['NVDA', 'META', 'NFLX']
        success = self.orchestrator.update_monitored_symbols(new_symbols)

        if success:
            print(f"   Updated symbols to: {new_symbols}")
        else:
            print("   Failed to update symbols")


def demonstrate_integration_features():
    """Demonstrate key integration features"""
    print("\nKey Integration Features:")
    print("=" * 40)

    features = [
        "News collection every 15 minutes during market hours",
        "Automatic FinBERT sentiment analysis",
        "XGBoost-based trading recommendations",
        "Queue-based communication with decision engine",
        "Stock-specific tracking for monitored symbols",
        "Resource optimization for t3.micro instances",
        "Real-time monitoring and health checks",
        "Error handling and automatic recovery",
        "Dynamic symbol list management",
        "Market hours awareness and scheduling"
    ]

    for i, feature in enumerate(features, 1):
        print(f"   {i}. {feature}")

    print("\nUsage in your orchestrator.py:")
    print("   1. Import: from orchestrator_news_integration import create_enhanced_orchestrator")
    print("   2. Configure: NewsIntegrationConfig(news_collection_interval=15)")
    print("   3. Create: enhanced_orchestrator = create_enhanced_orchestrator(config)")
    print("   4. Use: Same API as your existing orchestrator!")


def demonstrate_queue_message_format():
    """Show the format of messages sent to decision engine"""
    print("\nQueue Message Format to Decision Engine:")
    print("=" * 50)

    example_message = {
        "symbol": "AAPL",
        "timestamp": "2024-01-15T10:30:00",
        "news_data": {
            "news_volume": 8,
            "sentiment_score": 0.35,
            "confidence": 0.78,
            "sources_used": ["alpha_vantage", "newsapi", "reddit"],
            "market_events": ["earnings_report"],
            "staleness_minutes": 12
        },
        "sentiment_data": {
            "success_count": 8,
            "average_confidence": 0.82,
            "sentiment_distribution": {"POSITIVE": 5, "NEUTRAL": 2, "NEGATIVE": 1},
            "processing_time": 2.3
        },
        "recommendation": {
            "action": "BUY",
            "confidence": 0.84,
            "prediction_score": 0.67,
            "reasoning": "positive news sentiment (0.35), high news volume (0.80)",
            "model_version": "xgboost_news_v1.0"
        },
        "pipeline_metadata": {
            "cycle_number": 42,
            "confidence_level": 0.82
        }
    }

    import json
    print(json.dumps(example_message, indent=2))


async def main():
    """Main demonstration function"""
    print("Enhanced Orchestrator with News Pipeline")
    print("15-Minute News Collection & FinBERT Analysis Integration")
    print("=" * 70)

    # Show integration features
    demonstrate_integration_features()

    # Show queue message format
    demonstrate_queue_message_format()

    # Run the actual demo
    demo = EnhancedOrchestratorDemo()
    success = demo.run_demo()

    if success:
        print("\nSUCCESS: Demo completed successfully!")
        print("Next steps:")
        print("   1. Review the integration guide: news_pipeline_integration_guide.md")
        print("   2. Test with your actual orchestrator.py")
        print("   3. Configure your symbols and intervals")
        print("   4. Monitor the 15-minute news collection cycles")
        print("   5. Integrate news recommendations into your decision engine")
    else:
        print("\nWARNING: Demo encountered issues")
        print("NOTE: This is normal if dependencies aren't fully installed")
        print("TIP: See the integration guide for setup instructions")


if __name__ == "__main__":
    asyncio.run(main())
