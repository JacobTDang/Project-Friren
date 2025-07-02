"""
Professional Targeted News Collector - Event-Driven Multi-Source Collection

This collector provides professional-grade, targeted news collection using multiple
reliable news APIs for specific symbols triggered by chaos detection from the Strategy Analyzer.

Designed for event-driven architecture where news is collected only when:
1. Strategy Analyzer detects market chaos using entropy regime detection
2. Decision Engine requires additional context for trading decisions  
3. Specific symbols show unusual activity requiring immediate news context

This approach solves rate limiting issues by:
- Reducing total API calls through selective collection
- Using multiple reliable professional sources (NewsAPI, Alpha Vantage, FMP, Marketaux)
- Operating on-demand rather than continuous polling
- Intelligent fallback between sources to avoid rate limits

Professional News Sources (in priority order):
1. Alpha Vantage - Excellent sentiment analysis, 500 requests/day free
2. NewsAPI - Premium financial sources (CNBC, Reuters, Bloomberg), 500 requests/day free  
3. FMP - Financial Modeling Prep, 250 requests/day free
4. Marketaux - Real-time financial news, 100 requests/day free
"""

import os
import time
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any, Tuple
from dataclasses import dataclass
from enum import Enum

# Import base NewsArticle class
try:
    from .news.base import NewsArticle
    from .news.alpha_vintage_api import AlphaVantageNews
    from .news.news_api import NewsAPIData
    from .news.fmp_api import FMPNews
    from .news.marketaux_api import MarketauxNews
except ImportError:
    from Friren_V1.trading_engine.data.news.base import NewsArticle
    from Friren_V1.trading_engine.data.news.alpha_vintage_api import AlphaVantageNews
    from Friren_V1.trading_engine.data.news.news_api import NewsAPIData
    from Friren_V1.trading_engine.data.news.fmp_api import FMPNews
    from Friren_V1.trading_engine.data.news.marketaux_api import MarketauxNews


class NewsSource(Enum):
    """Available professional news sources"""
    ALPHA_VANTAGE = "alpha_vantage"
    NEWS_API = "news_api"
    FMP = "fmp"
    MARKETAUX = "marketaux"


@dataclass
class TargetedNewsRequest:
    """Request for targeted news collection"""
    symbol: str
    trigger_reason: str  # "high_volatility", "volume_spike", "strategy_conflict", etc.
    urgency: str  # "low", "medium", "high", "critical"
    max_articles: int = 5
    hours_back: int = 6
    request_id: str = ""
    timestamp: datetime = None
    preferred_sources: List[NewsSource] = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()
        if not self.request_id:
            self.request_id = f"{self.symbol}_{int(time.time())}"
        if self.preferred_sources is None:
            # Default priority order based on quality and rate limits
            self.preferred_sources = [
                NewsSource.ALPHA_VANTAGE,  # Best sentiment analysis
                NewsSource.NEWS_API,       # Premium sources
                NewsSource.FMP,            # Good coverage
                NewsSource.MARKETAUX       # Real-time focus
            ]


@dataclass
class TargetedNewsResponse:
    """Response from targeted news collection"""
    request_id: str
    symbol: str
    articles: List[NewsArticle]
    collection_time: datetime
    processing_time_ms: float
    source_used: str = ""
    sources_attempted: List[str] = None
    success: bool = True
    error_message: str = ""
    
    def __post_init__(self):
        if self.sources_attempted is None:
            self.sources_attempted = []


class ProfessionalTargetedCollector:
    """
    Professional multi-source collector for targeted, event-driven news collection
    
    Uses multiple professional financial news APIs with intelligent fallback to ensure
    reliable news collection even when individual sources hit rate limits.
    
    Designed for speed and reliability when decision engine needs immediate
    context for specific symbols during chaotic market conditions.
    """
    
    def __init__(self):
        self.logger = logging.getLogger("professional_targeted_news")
        
        # Initialize available news sources
        self.sources = {}
        self._initialize_sources()
        
        # Performance tracking
        self.requests_processed = 0
        self.avg_response_time = 0.0
        self.last_request_time = None
        self.source_usage_stats = {source.value: 0 for source in NewsSource}
        self.source_success_rates = {source.value: 1.0 for source in NewsSource}
        
    def _initialize_sources(self):
        """Initialize available news sources"""
        # Alpha Vantage - Best for sentiment analysis
        try:
            alpha_key = os.getenv('ALPHA_VANTAGE_API_KEY')
            if alpha_key:
                self.sources[NewsSource.ALPHA_VANTAGE] = AlphaVantageNews(alpha_key)
                self.logger.info("Alpha Vantage news source initialized")
            else:
                self.logger.warning("Alpha Vantage API key not found")
        except Exception as e:
            self.logger.warning(f"Failed to initialize Alpha Vantage: {e}")
        
        # NewsAPI - Premium financial sources
        try:
            news_key = os.getenv('NEWS_API_KEY')
            if news_key:
                self.sources[NewsSource.NEWS_API] = NewsAPIData(news_key)
                self.logger.info("NewsAPI source initialized")
            else:
                self.logger.warning("NewsAPI key not found")
        except Exception as e:
            self.logger.warning(f"Failed to initialize NewsAPI: {e}")
        
        # FMP - Financial Modeling Prep
        try:
            fmp_key = os.getenv('FMP_API_KEY')
            if fmp_key:
                self.sources[NewsSource.FMP] = FMPNews(fmp_key)
                self.logger.info("FMP news source initialized")
            else:
                self.logger.warning("FMP API key not found")
        except Exception as e:
            self.logger.warning(f"Failed to initialize FMP: {e}")
        
        # Marketaux - Real-time financial news
        try:
            marketaux_key = os.getenv('MARKETAUX_API_KEY')
            if marketaux_key:
                self.sources[NewsSource.MARKETAUX] = MarketauxNews(marketaux_key)
                self.logger.info("Marketaux news source initialized")
            else:
                self.logger.warning("Marketaux API key not found")
        except Exception as e:
            self.logger.warning(f"Failed to initialize Marketaux: {e}")
        
        if not self.sources:
            self.logger.error("No news sources available! Check API keys in environment variables.")
        else:
            available_sources = list(self.sources.keys())
            self.logger.info(f"Professional news collector initialized with {len(available_sources)} sources: {[s.value for s in available_sources]}")
    
    def collect_targeted_news(self, request: TargetedNewsRequest) -> TargetedNewsResponse:
        """
        Collect targeted news for a specific symbol using professional sources with fallback
        
        This is the main entry point called by the decision engine when it receives
        a chaos detection signal and needs additional context for decision making.
        """
        start_time = time.time()
        
        try:
            self.logger.info(f"PROFESSIONAL TARGETED COLLECTION: {request.symbol} - Trigger: {request.trigger_reason}, Urgency: {request.urgency}")
            
            # Quick validation
            if not request.symbol or len(request.symbol) > 5:
                raise ValueError(f"Invalid symbol: {request.symbol}")
            
            if not self.sources:
                raise ValueError("No news sources available")
            
            # Try sources in priority order with intelligent fallback
            articles, source_used, sources_attempted = self._collect_with_fallback(request)
            
            processing_time = (time.time() - start_time) * 1000  # Convert to milliseconds
            
            # Update performance metrics
            self._update_metrics(processing_time, source_used, len(articles) > 0)
            
            # Create successful response
            response = TargetedNewsResponse(
                request_id=request.request_id,
                symbol=request.symbol,
                articles=articles,
                collection_time=datetime.now(),
                processing_time_ms=processing_time,
                source_used=source_used,
                sources_attempted=sources_attempted,
                success=True
            )
            
            self.logger.info(f"PROFESSIONAL SUCCESS: {request.symbol} - {len(articles)} articles in {processing_time:.1f}ms from {source_used}")
            return response
            
        except Exception as e:
            processing_time = (time.time() - start_time) * 1000
            self.logger.error(f"PROFESSIONAL ERROR: {request.symbol} - {e}")
            
            # Create error response
            response = TargetedNewsResponse(
                request_id=request.request_id,
                symbol=request.symbol,
                articles=[],
                collection_time=datetime.now(),
                processing_time_ms=processing_time,
                sources_attempted=getattr(self, '_last_sources_attempted', []),
                success=False,
                error_message=str(e)
            )
            
            return response
    
    def _collect_with_fallback(self, request: TargetedNewsRequest) -> Tuple[List[NewsArticle], str, List[str]]:
        """Collect news with intelligent fallback between sources"""
        sources_attempted = []
        
        # Filter available sources based on request preferences
        preferred_sources = [s for s in request.preferred_sources if s in self.sources]
        
        if not preferred_sources:
            # If no preferred sources available, use all available sources
            preferred_sources = list(self.sources.keys())
        
        # Sort by success rate for intelligent fallback
        sources_by_success = sorted(
            preferred_sources,
            key=lambda s: self.source_success_rates.get(s.value, 0.0),
            reverse=True
        )
        
        for source_enum in sources_by_success:
            source_name = source_enum.value
            sources_attempted.append(source_name)
            
            try:
                self.logger.debug(f"Trying {source_name} for {request.symbol}")
                
                source_instance = self.sources[source_enum]
                articles = source_instance.get_symbol_news(
                    symbol=request.symbol,
                    hours_back=request.hours_back,
                    max_articles=request.max_articles
                )
                
                if articles:
                    self.logger.info(f"SUCCESS: {source_name} returned {len(articles)} articles for {request.symbol}")
                    
                    # BUSINESS LOGIC OUTPUT - Detailed collection messages
                    for i, article in enumerate(articles):
                        timestamp = datetime.now().strftime('%H:%M:%S')
                        title = article.get('title', 'Unknown Title')
                        if len(title) > 50:
                            title = title[:47] + '...'
                        
                        # Map source names for display
                        display_source = {
                            'alpha_vantage': 'Alpha Vantage',
                            'fmp': 'Financial Modeling Prep', 
                            'marketaux': 'Marketaux',
                            'newsapi': 'NewsAPI'
                        }.get(source_name, source_name)
                        
                        print(f"[NEWS COLLECTOR] {request.symbol}: '{title}' from {display_source} - collected {timestamp}")
                    
                    self._last_sources_attempted = sources_attempted
                    return articles, source_name, sources_attempted
                else:
                    self.logger.debug(f"No articles from {source_name} for {request.symbol}")
                    
            except Exception as e:
                self.logger.warning(f"Error with {source_name} for {request.symbol}: {e}")
                # Update success rate for this source
                current_rate = self.source_success_rates.get(source_name, 1.0)
                self.source_success_rates[source_name] = current_rate * 0.9  # Decay success rate
                continue
        
        # If we get here, no sources returned articles
        self.logger.warning(f"No articles found from any source for {request.symbol}")
        self._last_sources_attempted = sources_attempted
        return [], "none", sources_attempted
    
    def _update_metrics(self, processing_time_ms: float, source_used: str, success: bool):
        """Update performance metrics"""
        self.requests_processed += 1
        
        if self.avg_response_time == 0:
            self.avg_response_time = processing_time_ms
        else:
            # Rolling average
            self.avg_response_time = (self.avg_response_time * 0.8) + (processing_time_ms * 0.2)
        
        self.last_request_time = datetime.now()
        
        # Update source usage stats
        if source_used in self.source_usage_stats:
            self.source_usage_stats[source_used] += 1
        
        # Update success rates
        if source_used in self.source_success_rates and success:
            current_rate = self.source_success_rates[source_used]
            self.source_success_rates[source_used] = min(1.0, current_rate * 1.1)  # Boost success rate
    
    def get_performance_stats(self) -> Dict[str, Any]:
        """Get comprehensive performance statistics"""
        available_sources = [s.value for s in self.sources.keys()]
        
        return {
            'requests_processed': self.requests_processed,
            'avg_response_time_ms': round(self.avg_response_time, 2),
            'last_request_time': self.last_request_time.isoformat() if self.last_request_time else None,
            'status': 'healthy' if self.avg_response_time < 5000 else 'slow',
            'available_sources': available_sources,
            'source_usage_stats': self.source_usage_stats.copy(),
            'source_success_rates': {k: round(v, 3) for k, v in self.source_success_rates.items()},
            'total_sources_available': len(self.sources)
        }
    
    def test_sources(self) -> Dict[str, bool]:
        """Test connection to all available sources"""
        results = {}
        
        for source_enum, source_instance in self.sources.items():
            source_name = source_enum.value
            try:
                self.logger.info(f"Testing {source_name} connection...")
                success = source_instance.test_connection()
                results[source_name] = success
                self.logger.info(f"{source_name}: {'✓ Connected' if success else '✗ Failed'}")
            except Exception as e:
                results[source_name] = False
                self.logger.error(f"{source_name}: ✗ Error - {e}")
        
        return results
    
    def get_api_usage_summary(self) -> Dict[str, Dict[str, Any]]:
        """Get API usage summary for all sources"""
        usage_summary = {}
        
        for source_enum, source_instance in self.sources.items():
            source_name = source_enum.value
            try:
                if hasattr(source_instance, 'get_api_usage'):
                    usage = source_instance.get_api_usage()
                    usage_summary[source_name] = usage
                else:
                    usage_summary[source_name] = {'status': 'no_usage_data'}
            except Exception as e:
                usage_summary[source_name] = {'error': str(e)}
        
        return usage_summary
    
    def get_best_source_for_urgency(self, urgency: str) -> List[NewsSource]:
        """Get best sources based on urgency level"""
        if urgency == "critical":
            # For critical urgency, prefer fast and reliable sources
            return [NewsSource.ALPHA_VANTAGE, NewsSource.NEWS_API, NewsSource.MARKETAUX, NewsSource.FMP]
        elif urgency == "high":
            # For high urgency, balance speed and coverage
            return [NewsSource.NEWS_API, NewsSource.ALPHA_VANTAGE, NewsSource.MARKETAUX, NewsSource.FMP]
        elif urgency == "medium":
            # For medium urgency, prefer comprehensive coverage
            return [NewsSource.ALPHA_VANTAGE, NewsSource.FMP, NewsSource.NEWS_API, NewsSource.MARKETAUX]
        else:  # low urgency
            # For low urgency, use any available source
            return [NewsSource.FMP, NewsSource.MARKETAUX, NewsSource.ALPHA_VANTAGE, NewsSource.NEWS_API]


# Global instance for system-wide use
_professional_targeted_collector = None

def get_professional_targeted_collector() -> ProfessionalTargetedCollector:
    """Get global professional targeted collector instance"""
    global _professional_targeted_collector
    if _professional_targeted_collector is None:
        _professional_targeted_collector = ProfessionalTargetedCollector()
    return _professional_targeted_collector


# Compatibility function for existing code
def get_targeted_collector() -> ProfessionalTargetedCollector:
    """Get targeted collector (now returns professional version)"""
    return get_professional_targeted_collector()