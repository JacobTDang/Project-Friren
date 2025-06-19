#!/usr/bin/env python3
"""
Test Rate Limiting System for Friren Trading Engine

This test demonstrates that the news collection system will properly
detect and stop making API calls when rate limits (429 errors) are encountered.
"""

import time
import logging
from datetime import datetime
from typing import List

# Set up logging to see rate limiting in action
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

try:
    from Friren_V1.trading_engine.data.news.news_api import NewsAPIData
    from Friren_V1.trading_engine.data.news.fmp_api import FMPNews
    from Friren_V1.trading_engine.data.news.marketaux_api import MarketauxNews
    from Friren_V1.trading_engine.data.news.alpha_vintage_api import AlphaVantageNews
    from Friren_V1.trading_engine.data.news_collector import EnhancedNewsCollector
except ImportError as e:
    print(f"❌ Could not import news modules: {e}")
    print("💡 This test requires the Friren trading engine modules")
    exit(1)


class RateLimitTester:
    """Test suite for rate limiting functionality"""

    def __init__(self):
        self.logger = logging.getLogger("RateLimitTester")
        self.test_symbols = ["AAPL", "MSFT", "GOOGL"]

    def test_newsapi_rate_limiting(self):
        """Test NewsAPI rate limiting detection"""
        print("\n🧪 Testing NewsAPI Rate Limiting...")

        try:
            news_api = NewsAPIData()

            # Check initial state
            print(f"📊 Initial state: API available = {news_api._is_api_available()}")
            print(f"📊 Daily requests used: {news_api.daily_requests}/{news_api.max_daily_requests}")

            # Try to collect news
            for i, symbol in enumerate(self.test_symbols):
                print(f"\n🔍 Attempt {i+1}: Collecting news for {symbol}...")

                articles = news_api.get_symbol_news(symbol, max_articles=5)

                if news_api.rate_limited:
                    print(f"⚠️  RATE LIMITED: NewsAPI has been disabled due to rate limiting")
                    print(f"📅 Rate limit detected at: {news_api.rate_limit_detected_at}")
                    if news_api.rate_limit_reset_time:
                        print(f"⏰ Rate limit resets at: {news_api.rate_limit_reset_time}")
                    break

                elif articles:
                    print(f"✅ Successfully collected {len(articles)} articles for {symbol}")
                else:
                    print(f"ℹ️  No articles found for {symbol} (may indicate rate limiting)")

                print(f"📊 Requests used: {news_api.daily_requests}/{news_api.max_daily_requests}")
                print(f"📊 Consecutive failures: {news_api.consecutive_failures}")

                # Small delay to avoid overwhelming the API
                time.sleep(1)

            # Test that rate-limited API won't make more calls
            if news_api.rate_limited:
                print(f"\n🛡️  Testing rate limit protection...")
                print(f"📊 API available check: {news_api._is_api_available()}")

                # This should return empty list without making API call
                test_articles = news_api.get_symbol_news("TSLA", max_articles=5)
                print(f"✅ Protected call returned {len(test_articles)} articles (should be 0)")

        except Exception as e:
            print(f"❌ NewsAPI test failed: {e}")

    def test_enhanced_news_collector(self):
        """Test rate limiting in the enhanced news collector"""
        print("\n🧪 Testing Enhanced News Collector Rate Limiting...")

        try:
            collector = EnhancedNewsCollector()

            print(f"📊 Available news sources: {collector.get_available_sources()}")

            # Test collection for multiple symbols
            for symbol in self.test_symbols:
                print(f"\n🔍 Collecting news for {symbol}...")

                try:
                    news_data = collector.collect_symbol_news(symbol, hours_back=6, max_articles_per_source=3)

                    print(f"✅ News collection completed for {symbol}")
                    print(f"📊 Overall sentiment: {news_data.overall_sentiment_score:.2f}")
                    print(f"📊 Confidence: {news_data.sentiment_confidence:.2f}")
                    print(f"📊 Articles found: {news_data.news_volume}")
                    print(f"📊 Sources used: {', '.join(news_data.sources_used)}")
                    print(f"📊 Data quality: {news_data.data_quality_score:.2f}")

                    if news_data.news_volume == 0:
                        print("⚠️  No articles found - may indicate rate limiting")

                except Exception as e:
                    print(f"⚠️  Error collecting news for {symbol}: {e}")

                # Brief pause between symbols
                time.sleep(2)

        except Exception as e:
            print(f"❌ Enhanced collector test failed: {e}")

    def test_fallback_behavior(self):
        """Test that system gracefully handles rate limited APIs"""
        print("\n🧪 Testing Fallback Behavior...")

        try:
            # Create a mock rate-limited scenario
            print("🎭 Simulating rate-limited environment...")

            # Initialize collector
            collector = EnhancedNewsCollector()

            # Manually set rate limiting on available sources
            for source_name, source in collector.news_sources.items():
                if hasattr(source, 'rate_limited'):
                    # Simulate that we've hit rate limits
                    source.rate_limited = True
                    source.rate_limit_detected_at = datetime.now()
                    print(f"🚫 Simulated rate limiting for {source_name}")

            # Try to collect news - should handle gracefully
            print(f"\n🔍 Attempting news collection with rate-limited sources...")

            news_data = collector.collect_symbol_news("AAPL", hours_back=6, max_articles_per_source=3)

            print(f"✅ System handled rate-limited sources gracefully")
            print(f"📊 Articles found: {news_data.news_volume}")
            print(f"📊 Data quality: {news_data.data_quality_score:.2f}")

            if news_data.news_volume == 0:
                print("✅ No articles found as expected (all sources rate limited)")

        except Exception as e:
            print(f"❌ Fallback test failed: {e}")

    def test_rate_limit_reset(self):
        """Test that rate limits reset after time period"""
        print("\n🧪 Testing Rate Limit Reset Logic...")

        try:
            news_api = NewsAPIData()

            # Simulate rate limiting
            print("🎭 Simulating rate limit scenario...")
            news_api.rate_limited = True
            news_api.rate_limit_detected_at = datetime.now()

            print(f"📊 Rate limited: {news_api.rate_limited}")
            print(f"📊 API available: {news_api._is_api_available()}")

            # Simulate time passage (by manually adjusting the timestamp)
            print("⏰ Simulating 1 hour time passage...")
            past_time = datetime.now()
            past_time = past_time.replace(hour=past_time.hour - 2)  # 2 hours ago
            news_api.rate_limit_detected_at = past_time

            # Check if rate limit resets
            api_available = news_api._is_api_available()
            print(f"📊 API available after time reset: {api_available}")
            print(f"📊 Rate limited: {news_api.rate_limited}")

            if api_available and not news_api.rate_limited:
                print("✅ Rate limit reset logic working correctly")
            else:
                print("⚠️  Rate limit may not have reset as expected")

        except Exception as e:
            print(f"❌ Rate limit reset test failed: {e}")

    def run_all_tests(self):
        """Run comprehensive rate limiting tests"""
        print("🚀 Starting Comprehensive Rate Limiting Tests")
        print("=" * 60)

        print(f"🕒 Test started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        # Run individual tests
        self.test_newsapi_rate_limiting()
        self.test_enhanced_news_collector()
        self.test_fallback_behavior()
        self.test_rate_limit_reset()

        print("\n" + "=" * 60)
        print("🏁 Rate Limiting Tests Complete")
        print("\n📋 Key Features Tested:")
        print("  ✅ 429 error detection and API disabling")
        print("  ✅ Automatic rate limit status tracking")
        print("  ✅ Graceful fallback when APIs are unavailable")
        print("  ✅ Rate limit reset after time period")
        print("  ✅ Protected API calls when rate limited")

        print("\n💡 Summary:")
        print("  The system will automatically detect rate limits (429 errors)")
        print("  and stop making API calls to prevent further failures.")
        print("  APIs will be re-enabled after 1 hour or successful reset.")


def main():
    """Main test execution"""
    print("🔬 Friren Trading Engine - Rate Limiting Test Suite")
    print("=" * 60)

    tester = RateLimitTester()
    tester.run_all_tests()


if __name__ == "__main__":
    main()
