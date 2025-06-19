# -*- coding: utf-8 -*-
"""
Windows-Compatible Trading Orchestrator
Single-threaded version that avoids Windows multiprocessing issues
"""

import sys
import os
import time
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime
from dataclasses import dataclass
import random

# Setup logging
os.makedirs('logs', exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/windows_orchestrator.log'),
        logging.StreamHandler()
    ]
)

@dataclass
class SystemStatus:
    """System status tracking"""
    start_time: datetime
    symbols: List[str]
    mode: str = "paper_trading"
    cycles_completed: int = 0
    last_news_update: Optional[datetime] = None
    last_sentiment_update: Optional[datetime] = None
    last_analysis_update: Optional[datetime] = None
    errors: List[str] = None

    def __post_init__(self):
        if self.errors is None:
            self.errors = []

class MockNewsCollector:
    """Mock news collector for testing"""

    def get_symbol_news(self, symbol: str, limit: int = 5) -> List[Dict[str, Any]]:
        # Generate mock news articles
        articles = []
        news_types = ["earnings", "merger", "expansion", "innovation", "partnership"]
        sentiments = ["positive", "neutral", "negative"]

        for i in range(limit):
            news_type = random.choice(news_types)
            sentiment_hint = random.choice(sentiments)

            if sentiment_hint == "positive":
                title = f"{symbol} Reports Strong {news_type.title()} Performance"
                desc = f"Analysts are optimistic about {symbol}'s recent developments."
            elif sentiment_hint == "negative":
                title = f"{symbol} Faces Challenges in {news_type.title()}"
                desc = f"Market concerns about {symbol}'s performance continue."
            else:
                title = f"{symbol} Updates on {news_type.title()}"
                desc = f"Standard business update regarding {symbol}."

            articles.append({
                'title': title,
                'description': desc,
                'url': f"https://mock.news/{symbol}/{i+1}",
                'published_at': datetime.now().isoformat(),
                'source': 'MockNews',
                'sentiment_hint': sentiment_hint
            })
        return articles

class MockSentimentAnalyzer:
    """Mock sentiment analyzer for testing"""

    def analyze_text(self, text: str) -> Dict[str, Any]:
        # Generate sentiment based on keywords
        text_lower = text.lower()

        positive_words = ['strong', 'optimistic', 'growth', 'profit', 'success', 'gains']
        negative_words = ['challenges', 'concerns', 'decline', 'loss', 'risks', 'problems']

        positive_count = sum(1 for word in positive_words if word in text_lower)
        negative_count = sum(1 for word in negative_words if word in text_lower)

        if positive_count > negative_count:
            sentiment = 'positive'
            confidence = min(0.9, 0.6 + (positive_count * 0.1))
        elif negative_count > positive_count:
            sentiment = 'negative'
            confidence = min(0.9, 0.6 + (negative_count * 0.1))
        else:
            sentiment = 'neutral'
            confidence = random.uniform(0.4, 0.7)

        return {
            'sentiment': sentiment,
            'confidence': confidence,
            'text_length': len(text)
        }

class WindowsOrchestrator:
    """Windows-compatible single-threaded orchestrator"""

    def __init__(self, symbols: List[str] = None):
        self.logger = logging.getLogger("windows_orchestrator")
        self.symbols = symbols or ['AAPL', 'MSFT', 'GOOGL', 'TSLA', 'NVDA']
        self.status = SystemStatus(
            start_time=datetime.now(),
            symbols=self.symbols
        )
        self.running = False
        self.cycle_interval = 60  # 1 minute between cycles for demo

        # Initialize components
        self.news_collectors = {'mock': MockNewsCollector()}
        self.sentiment_analyzer = MockSentimentAnalyzer()
        self.decision_engine = {
            'last_decisions': {},
            'portfolio_value': 100000.0,
            'positions': {},
            'analysis_history': []
        }

        self.logger.info(f"Windows Orchestrator initialized with symbols: {self.symbols}")

    def start_trading_cycle(self):
        """Start the main trading cycle"""
        self.running = True
        self.logger.info("Starting Windows-compatible trading cycle...")

        try:
            while self.running:
                cycle_start = datetime.now()
                self.logger.info(f"=== Starting trading cycle {self.status.cycles_completed + 1} ===")

                # Run trading cycle components
                self._run_news_collection()
                self._run_sentiment_analysis()
                self._run_market_analysis()
                self._run_decision_making()

                # Update status
                self.status.cycles_completed += 1
                cycle_duration = (datetime.now() - cycle_start).total_seconds()

                self.logger.info(f"Cycle {self.status.cycles_completed} completed in {cycle_duration:.1f}s")
                self._log_system_status()

                # Wait for next cycle
                if self.running:
                    self.logger.info(f"Waiting {self.cycle_interval}s for next cycle...")
                    time.sleep(self.cycle_interval)

        except KeyboardInterrupt:
            self.logger.info("Shutdown requested by user")
        except Exception as e:
            self.logger.error(f"Trading cycle error: {e}")
            self.status.errors.append(f"Cycle error: {e}")
        finally:
            self.running = False
            self.logger.info("Trading cycle stopped")

    def _run_news_collection(self):
        """Collect news for all symbols"""
        try:
            news_data = {}

            for symbol in self.symbols:
                symbol_news = []

                for source_name, collector in self.news_collectors.items():
                    try:
                        articles = collector.get_symbol_news(symbol, limit=3)
                        symbol_news.extend(articles)
                    except Exception as e:
                        self.logger.warning(f"News collection failed for {symbol}: {e}")

                news_data[symbol] = symbol_news
                self.logger.info(f"Collected {len(symbol_news)} articles for {symbol}")

            self.latest_news = news_data
            self.status.last_news_update = datetime.now()

        except Exception as e:
            self.logger.error(f"News collection error: {e}")
            self.status.errors.append(f"News error: {e}")

    def _run_sentiment_analysis(self):
        """Analyze sentiment of collected news"""
        try:
            if not hasattr(self, 'latest_news'):
                return

            sentiment_results = {}

            for symbol, articles in self.latest_news.items():
                if not articles:
                    continue

                symbol_sentiments = []

                for article in articles:
                    try:
                        text = f"{article.get('title', '')} {article.get('description', '')}"
                        result = self.sentiment_analyzer.analyze_text(text)
                        symbol_sentiments.append({
                            'sentiment': result.get('sentiment', 'neutral'),
                            'confidence': result.get('confidence', 0.5),
                            'title': article.get('title', '')
                        })
                    except Exception as e:
                        self.logger.warning(f"Sentiment analysis failed: {e}")

                sentiment_results[symbol] = symbol_sentiments
                self.logger.info(f"Analyzed sentiment for {len(symbol_sentiments)} articles for {symbol}")

            self.latest_sentiment = sentiment_results
            self.status.last_sentiment_update = datetime.now()

        except Exception as e:
            self.logger.error(f"Sentiment analysis error: {e}")
            self.status.errors.append(f"Sentiment error: {e}")

    def _run_market_analysis(self):
        """Run market analysis for symbols"""
        try:
            analysis_results = {}

            for symbol in self.symbols:
                analysis = {
                    'symbol': symbol,
                    'timestamp': datetime.now().isoformat(),
                    'recommendation': 'HOLD',
                    'confidence': 0.5,
                    'news_sentiment': self._get_symbol_sentiment(symbol),
                    'analysis_type': 'sentiment_based'
                }

                analysis_results[symbol] = analysis

            self.latest_analysis = analysis_results
            self.status.last_analysis_update = datetime.now()

            self.logger.info(f"Completed market analysis for {len(analysis_results)} symbols")

        except Exception as e:
            self.logger.error(f"Market analysis error: {e}")
            self.status.errors.append(f"Analysis error: {e}")

    def _get_symbol_sentiment(self, symbol: str) -> Dict[str, Any]:
        """Get aggregated sentiment for a symbol"""
        if not hasattr(self, 'latest_sentiment') or symbol not in self.latest_sentiment:
            return {'overall': 'neutral', 'confidence': 0.5}

        sentiments = self.latest_sentiment[symbol]
        if not sentiments:
            return {'overall': 'neutral', 'confidence': 0.5}

        positive_count = sum(1 for s in sentiments if s['sentiment'] == 'positive')
        negative_count = sum(1 for s in sentiments if s['sentiment'] == 'negative')

        if positive_count > negative_count:
            overall = 'positive'
        elif negative_count > positive_count:
            overall = 'negative'
        else:
            overall = 'neutral'

        avg_confidence = sum(s['confidence'] for s in sentiments) / len(sentiments)

        return {
            'overall': overall,
            'confidence': avg_confidence,
            'positive_count': positive_count,
            'negative_count': negative_count,
            'total_articles': len(sentiments)
        }

    def _run_decision_making(self):
        """Make trading decisions based on analysis"""
        try:
            if not hasattr(self, 'latest_analysis'):
                return

            decisions = {}

            for symbol, analysis in self.latest_analysis.items():
                sentiment = analysis.get('news_sentiment', {})

                if sentiment.get('overall') == 'positive' and sentiment.get('confidence', 0) > 0.65:
                    decision = 'BUY'
                elif sentiment.get('overall') == 'negative' and sentiment.get('confidence', 0) > 0.65:
                    decision = 'SELL'
                else:
                    decision = 'HOLD'

                decisions[symbol] = {
                    'action': decision,
                    'confidence': sentiment.get('confidence', 0.5),
                    'reason': f"Sentiment: {sentiment.get('overall', 'neutral')}",
                    'timestamp': datetime.now().isoformat()
                }

                # Log decision (paper trading)
                conf = sentiment.get('confidence', 0.5)
                self.logger.info(f"PAPER TRADE - {symbol}: {decision} (Confidence: {conf:.2f})")

            # Store decisions
            self.decision_engine['last_decisions'] = decisions
            self.decision_engine['analysis_history'].append({
                'timestamp': datetime.now().isoformat(),
                'decisions': decisions
            })

            # Keep only last 100 entries
            if len(self.decision_engine['analysis_history']) > 100:
                self.decision_engine['analysis_history'] = self.decision_engine['analysis_history'][-100:]

        except Exception as e:
            self.logger.error(f"Decision making error: {e}")
            self.status.errors.append(f"Decision error: {e}")

    def _log_system_status(self):
        """Log current system status"""
        uptime = datetime.now() - self.status.start_time

        self.logger.info(f"System Status - Uptime: {uptime}, Cycles: {self.status.cycles_completed}, Errors: {len(self.status.errors)}")

        # Log recent decisions
        if self.decision_engine:
            recent_decisions = self.decision_engine.get('last_decisions', {})
            for symbol, decision in recent_decisions.items():
                self.logger.info(f"Latest {symbol}: {decision['action']} ({decision['reason']})")

    def stop(self):
        """Stop the trading cycle"""
        self.logger.info("Stopping trading cycle...")
        self.running = False

def main():
    """Main entry point"""
    print("🚀 Starting Windows-Compatible Friren Trading System...")

    orchestrator = WindowsOrchestrator(
        symbols=['AAPL', 'MSFT', 'GOOGL', 'TSLA', 'NVDA']
    )

    try:
        orchestrator.start_trading_cycle()
    except KeyboardInterrupt:
        print("\n⏹️ Shutdown requested by user")
    except Exception as e:
        print(f"❌ System error: {e}")
    finally:
        orchestrator.stop()
        print("✅ System shutdown complete")

if __name__ == "__main__":
    main()
