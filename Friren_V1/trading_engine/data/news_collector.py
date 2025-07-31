"""
Enhanced News Collector - Refactored with Modular Components

Provides backward compatibility while using the new modular components.
Maintains the original interface for existing code while leveraging
the extracted modular components for improved maintainability.
"""

import time
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass, field
from collections import defaultdict
import re
import threading

# Import API resilience system
try:
    from Friren_V1.infrastructure.api_resilience import (
        APIServiceType, get_resilience_manager, resilient_news_call
    )
    HAS_RESILIENCE = True
except ImportError:
    HAS_RESILIENCE = False

# Import the new modular components
try:
    from .news.collectors import NewsCollector, SourceManager, ArticleDeduplicator
    from .news.base import NewsArticle
except ImportError as e:
    # Fallback for direct execution
    try:
        from news.collectors import NewsCollector, SourceManager, ArticleDeduplicator
        from news.base import NewsArticle
    except ImportError as e2:
        print(f"CRITICAL ERROR: Required modular components failed to import: {e}, {e2}")
        print("SYSTEM CANNOT OPERATE: News collection requires modular components")
        raise ImportError(f"Critical modular component import failure: {e}, {e2}. System cannot continue.")


@dataclass
class ProcessedNewsData:
    """Processed and weighted news data for decision engine"""
    symbol: str
    timestamp: datetime

    # Aggregated sentiment scores (ready for decision engine)
    overall_sentiment_score: float      # -1.0 to 1.0 (negative to positive)
    sentiment_confidence: float         # 0.0 to 1.0 (how confident we are)
    news_volume: int                   # Number of unique articles

    # Source breakdown for analysis
    professional_sentiment: float      # NewsAPI, Alpha Vantage, Marketaux
    social_sentiment: float           # Reddit sentiment
    market_events: List[str]          # Earnings, analyst actions, etc.

    # Key articles for context (limited to most important)
    key_articles: List[NewsArticle] = field(default_factory=list)

    # Metadata
    sources_used: List[str] = field(default_factory=list)
    source_weights: Dict[str, float] = field(default_factory=dict)
    last_updated: datetime = field(default_factory=datetime.now)

    # Quality indicators
    data_quality_score: float = 0.0    # How reliable is this data
    staleness_minutes: int = 0          # How old is the newest article
    
    def to_dict(self) -> Dict[str, any]:
        """Convert to dictionary with datetime serialization"""
        return {
            'symbol': self.symbol,
            'timestamp': self.timestamp.isoformat(),
            'overall_sentiment_score': self.overall_sentiment_score,
            'sentiment_confidence': self.sentiment_confidence,
            'news_volume': self.news_volume,
            'key_articles': [article.to_dict() if hasattr(article, 'to_dict') else article for article in self.key_articles],
            'sources_used': self.sources_used,
            'source_weights': self.source_weights,
            'last_updated': self.last_updated.isoformat(),
            'data_quality_score': self.data_quality_score,
            'staleness_minutes': self.staleness_minutes
        }


class EnhancedNewsCollector:
    """
    Backward compatible interface for enhanced news collection
    
    Now uses modular components internally while maintaining the same external interface.
    This allows existing code to continue working without changes while benefiting
    from the improved modular architecture.
    """

    def __init__(self):
        """Initialize the enhanced news collector using modular components with resilience"""
        self.logger = logging.getLogger("enhanced_news_collector")

        # Initialize API resilience manager
        self.resilience_manager = get_resilience_manager() if HAS_RESILIENCE else None

        # Initialize the new modular components
        self.source_manager = SourceManager()
        self.deduplicator = ArticleDeduplicator()
        self.collector = NewsCollector(self.source_manager, self.deduplicator)

        # Provide backward compatibility for existing attributes
        self.news_sources = self.source_manager.news_sources
        self.source_weights = self.source_manager.source_weights
        self.min_articles_for_confidence = self.collector.min_articles_for_confidence

        if HAS_RESILIENCE:
            self.logger.info("EnhancedNewsCollector: Initialized with modular components and resilience protection")
        else:
            self.logger.info("EnhancedNewsCollector: Initialized with modular components (basic error handling)")
        self.logger.info("EnhancedNewsCollector: Backward compatibility maintained")

    def _ensure_sources_initialized(self):
        """Ensure news sources are initialized (delegate to source manager)"""
        self.source_manager.ensure_sources_initialized()
        # Update backward compatibility attributes
        self.news_sources = self.source_manager.news_sources
        self._sources_initialized = self.source_manager.is_initialized()

    def _get_dynamic_discovery_symbols(self) -> List[str]:
        """Get discovery symbols from database with intelligent fallback"""
        discovery_symbols = []
        
        try:
            # Try to import database manager
            from ...portfolio_manager.tools.db_manager import DatabaseManager
            
            # Get database symbols
            db_manager = DatabaseManager()
            
            # Get current holdings
            current_holdings = db_manager.get_current_holdings()
            if current_holdings:
                holdings_symbols = [holding['symbol'] for holding in current_holdings]
                discovery_symbols.extend(holdings_symbols)
                self.logger.info(f"Dynamic discovery: Loaded {len(holdings_symbols)} symbols from current holdings")
            
            # Get high-priority opportunities  
            opportunities = db_manager.get_high_priority_opportunities()
            if opportunities:
                opportunity_symbols = [opp['symbol'] for opp in opportunities]
                discovery_symbols.extend(opportunity_symbols)
                self.logger.info(f"Dynamic discovery: Loaded {len(opportunity_symbols)} symbols from opportunities")
            
            # Remove duplicates while preserving order
            discovery_symbols = list(dict.fromkeys(discovery_symbols))
            
        except Exception as e:
            self.logger.warning(f"Database symbol loading failed: {e}")
            
        # Intelligent fallback: Major market ETFs if no database symbols available
        if not discovery_symbols:
            discovery_symbols = ['SPY', 'QQQ', 'IWM']  # Only essential market ETFs
            self.logger.info("Dynamic discovery: Using minimal ETF fallback (no database symbols available)")
        else:
            # Always include major market indicators for market regime context
            essential_etfs = ['SPY', 'QQQ', 'IWM']
            for etf in essential_etfs:
                if etf not in discovery_symbols:
                    discovery_symbols.append(etf)
            self.logger.info(f"Dynamic discovery: Added essential ETFs for market context")
            
        self.logger.info(f"Dynamic discovery: Final symbol list: {len(discovery_symbols)} symbols")
        return discovery_symbols

    def collect_news(self, symbols: List[str], max_articles_per_symbol: int = 10) -> Dict[str, List[NewsArticle]]:
        """Collect news for given symbols with lazy loading"""
        # Ensure sources are initialized before collecting news
        self._ensure_sources_initialized()

        # Call the original collect_news method
        return self._collect_news_original(symbols, max_articles_per_symbol)

    def discover_market_opportunities(self, max_articles_per_symbol: int = 8) -> Dict[str, List[NewsArticle]]:
        """ULTRA CRITICAL: Scan broad market for new trading opportunities"""
        self.logger.info("DISCOVERY MODE: Scanning broad market for opportunities...")
        
        # Ensure sources are initialized
        self._ensure_sources_initialized()
        
        # Get dynamic discovery symbols from database
        discovery_symbols = self._get_dynamic_discovery_symbols()
        
        # Use dynamic discovery symbols for market-wide scanning
        discovery_results = self._collect_news_original(discovery_symbols, max_articles_per_symbol)
        
        # ULTRA ENHANCEMENT: Parse news articles for additional stock mentions
        mentioned_stocks = self._extract_stock_mentions_from_news(discovery_results)
        
        # Add newly discovered stocks to results if they have enough mentions
        for stock_symbol, mention_data in mentioned_stocks.items():
            if mention_data['mention_count'] >= 3 and stock_symbol not in discovery_results:
                self.logger.info(f"📈 NEWS DISCOVERY: Found {stock_symbol} mentioned {mention_data['mention_count']} times")
                # Add the articles that mentioned this stock
                discovery_results[stock_symbol] = mention_data['articles']
        
        # Log discovery progress with visibility
        total_articles = sum(len(articles) for articles in discovery_results.values())
        symbols_found = len([s for s, articles in discovery_results.items() if articles])
        
        self.logger.info(f"DISCOVERY RESULTS: {symbols_found}/{len(discovery_symbols)} symbols, {total_articles} total articles")
        if mentioned_stocks:
            self.logger.info(f"NEWS MENTIONS: Found {len(mentioned_stocks)} additional stocks in news content")
        
        # Enhanced logging for visibility
        for symbol, articles in discovery_results.items():
            if articles:
                self.logger.info(f"📰 DISCOVERY: {symbol} - {len(articles)} articles found")
                # Log first article title for visibility
                if articles:
                    self.logger.info(f"   🔸 Latest: {articles[0].title[:80]}...")
        
        return discovery_results

    def _extract_stock_mentions_from_news(self, news_results: Dict[str, List[NewsArticle]]) -> Dict[str, Dict]:
        """Extract stock symbol mentions from news articles content"""
        import re
        
        # Common stock symbols to look for in news
        potential_stocks = [
            'MSFT', 'AAPL', 'AMZN', 'GOOGL', 'GOOG', 'META', 'TSLA', 'NVDA', 'AMD', 'INTC',
            'JPM', 'BAC', 'WFC', 'GS', 'MS', 'C', 'V', 'MA', 'PYPL', 'SQ',
            'JNJ', 'PFE', 'UNH', 'ABBV', 'MRK', 'CVS', 'TMO', 'DHR', 'ABT',
            'XOM', 'CVX', 'COP', 'EOG', 'SLB', 'OXY', 'MPC', 'VLO', 'PSX',
            'WMT', 'HD', 'PG', 'KO', 'PEP', 'NKE', 'COST', 'TGT', 'LOW',
            'DIS', 'NFLX', 'CMCSA', 'VZ', 'T', 'ORCL', 'CRM', 'NOW', 'ADBE'
        ]
        
        mentioned_stocks = {}
        
        # Search through all articles for stock mentions
        for symbol, articles in news_results.items():
            for article in articles:
                # Combine title and content for searching
                full_text = f"{article.title} {getattr(article, 'content', '')} {getattr(article, 'description', '')}"
                
                # Look for stock symbols in the text
                for stock in potential_stocks:
                    # Create pattern to find stock mentions (avoid false positives)
                    pattern = rf'\b{stock}\b(?:\s+(?:stock|shares|equity|Corp|Inc|Corporation|Company))?'
                    matches = re.findall(pattern, full_text, re.IGNORECASE)
                    
                    if matches and stock != symbol:  # Don't count the main symbol
                        if stock not in mentioned_stocks:
                            mentioned_stocks[stock] = {
                                'mention_count': 0,
                                'articles': [],
                                'contexts': []
                            }
                        
                        mentioned_stocks[stock]['mention_count'] += len(matches)
                        if article not in mentioned_stocks[stock]['articles']:
                            mentioned_stocks[stock]['articles'].append(article)
                            
                        # Extract context around the mention
                        for match in matches[:2]:  # Max 2 contexts per article
                            start_pos = full_text.find(match)
                            context = full_text[max(0, start_pos-50):start_pos+50]
                            mentioned_stocks[stock]['contexts'].append(context.strip())
        
        # Filter out stocks with too few mentions
        filtered_stocks = {k: v for k, v in mentioned_stocks.items() if v['mention_count'] >= 2}
        
        return filtered_stocks

    def _collect_news_original(self, symbols: List[str], max_articles_per_symbol: int = 10) -> Dict[str, List[NewsArticle]]:
        """Original collect_news implementation - REAL NEWS COLLECTION"""
        self.logger.info(f"Collecting real news for {len(symbols)} symbols: {symbols}")
        
        # Quality thresholds
        max_article_age_hours = 6
        
        # Results storage
        all_symbol_news = {}
        
        # Collect from each available source
        for symbol in symbols:
            symbol_articles = []
            
            # Collect from all available sources
            for source_name, news_source in self.news_sources.items():
                try:
                    self.logger.debug(f"Collecting from {source_name} for {symbol}")
                    
                    # Get articles from this source
                    if hasattr(news_source, 'get_symbol_news'):
                        articles = news_source.get_symbol_news(
                            symbol=symbol,
                            hours_back=max_article_age_hours,
                            max_articles=max_articles_per_symbol
                        )
                        
                        if articles:
                            symbol_articles.extend(articles)
                            self.logger.info(f"Got {len(articles)} articles from {source_name} for {symbol}")
                            
                            # BUSINESS LOGIC OUTPUT: Live news collection for each article
                            try:
                                from terminal_color_system import print_news_collector
                                for article in articles:
                                    article_title = article.title[:60] + "..." if len(article.title) > 60 else article.title
                                    print_news_collector(f"{symbol}: '{article_title}' from {source_name}")
                            except ImportError:
                                for article in articles:
                                    article_title = article.title[:60] + "..." if len(article.title) > 60 else article.title
                                    print(f"[NEWS COLLECTOR] {symbol}: '{article_title}' from {source_name}")
                        else:
                            self.logger.debug(f"No articles from {source_name} for {symbol}")
                    else:
                        self.logger.warning(f"Source {source_name} doesn't have get_symbol_news method")
                        
                except Exception as e:
                    self.logger.warning(f"Error collecting from {source_name} for {symbol}: {e}")
                    continue
            
            # Deduplicate articles for this symbol
            if symbol_articles:
                unique_articles = self.deduplicate_articles(symbol_articles)
                all_symbol_news[symbol] = unique_articles
                self.logger.info(f"REAL NEWS: {symbol} - {len(unique_articles)} unique articles after deduplication")
            else:
                self.logger.info(f"REAL NEWS: {symbol} - No articles found from any source")
                all_symbol_news[symbol] = []
        
        # Log summary
        total_articles = sum(len(articles) for articles in all_symbol_news.values())
        symbols_with_news = len([s for s, articles in all_symbol_news.items() if articles])
        
        self.logger.info(f"REAL NEWS COLLECTION COMPLETE: {total_articles} articles for {symbols_with_news}/{len(symbols)} symbols")
        
        return all_symbol_news

    # Delegate core methods to modular components
    def collect_symbol_news(self, symbol: str, hours_back: int = 6,
                          max_articles_per_source: int = 15) -> ProcessedNewsData:
        """Collect and process news for a single symbol (delegates to modular component)"""
        return self.collector.collect_symbol_news(symbol, hours_back, max_articles_per_source)

    def collect_watchlist_news(self, symbols: List[str], hours_back: int = 6,
                             max_articles_per_symbol: int = 10) -> Dict[str, ProcessedNewsData]:
        """Efficiently collect news for multiple symbols (delegates to modular component)"""
        return self.collector.collect_watchlist_news(symbols, hours_back, max_articles_per_symbol)

    def deduplicate_articles(self, articles: List[NewsArticle]) -> List[NewsArticle]:
        """Remove duplicate articles using multiple strategies (delegates to modular component)"""
        return self.deduplicator.deduplicate_articles(articles)

    def get_available_sources(self) -> List[str]:
        """Get list of successfully initialized news sources (delegates to source manager)"""
        return self.source_manager.get_available_sources()

    def get_source_weights(self) -> Dict[str, float]:
        """Get current source weights (delegates to source manager)"""
        return self.source_manager.get_source_weights()

    def update_source_weights(self, new_weights: Dict[str, float]):
        """Update source weights for sentiment calculation (delegates to source manager)"""
        self.source_manager.update_source_weights(new_weights)
        # Update backward compatibility attribute
        self.source_weights = self.source_manager.source_weights