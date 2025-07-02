"""
Targeted News Collector - Event-Driven Single Stock Collection

This collector is designed for targeted, event-driven news collection when
the strategy analyzer detects chaotic market conditions and the decision 
engine needs additional context for specific symbols.

Key Features:
- Yahoo Finance only (reliable, high rate limits)
- Single stock focus (fast, targeted)
- Event-driven (efficient resource usage)
- Decision engine integration
"""

import time
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any
from dataclasses import dataclass
import requests
from bs4 import BeautifulSoup

# Import base NewsArticle class
try:
    from .news.base import NewsArticle
except ImportError:
    from Friren_V1.trading_engine.data.news.base import NewsArticle


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
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()
        if not self.request_id:
            self.request_id = f"{self.symbol}_{int(time.time())}"


@dataclass
class TargetedNewsResponse:
    """Response from targeted news collection"""
    request_id: str
    symbol: str
    articles: List[NewsArticle]
    collection_time: datetime
    processing_time_ms: float
    source_used: str = "yahoo_finance"
    success: bool = True
    error_message: str = ""


class YahooTargetedCollector:
    """
    Lightweight Yahoo Finance collector for targeted, event-driven news collection
    
    Designed for speed and reliability when decision engine needs immediate
    context for specific symbols during chaotic market conditions.
    """
    
    def __init__(self):
        self.logger = logging.getLogger("targeted_news_collector")
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
        # Performance tracking
        self.requests_processed = 0
        self.avg_response_time = 0.0
        self.last_request_time = None
        
    def collect_targeted_news(self, request: TargetedNewsRequest) -> TargetedNewsResponse:
        """
        Collect targeted news for a specific symbol in response to strategy analyzer signal
        
        This is the main entry point called by the decision engine when it receives
        a chaos detection signal and needs additional context for decision making.
        """
        start_time = time.time()
        
        try:
            self.logger.info(f"TARGETED COLLECTION: {request.symbol} - Trigger: {request.trigger_reason}, Urgency: {request.urgency}")
            
            # Quick validation
            if not request.symbol or len(request.symbol) > 5:
                raise ValueError(f"Invalid symbol: {request.symbol}")
            
            # Collect from Yahoo Finance (fast, reliable)
            articles = self._collect_yahoo_news(request.symbol, request.max_articles, request.hours_back)
            
            processing_time = (time.time() - start_time) * 1000  # Convert to milliseconds
            
            # Update performance metrics
            self._update_metrics(processing_time)
            
            # Create successful response
            response = TargetedNewsResponse(
                request_id=request.request_id,
                symbol=request.symbol,
                articles=articles,
                collection_time=datetime.now(),
                processing_time_ms=processing_time,
                success=True
            )
            
            self.logger.info(f"TARGETED SUCCESS: {request.symbol} - {len(articles)} articles in {processing_time:.1f}ms")
            return response
            
        except Exception as e:
            processing_time = (time.time() - start_time) * 1000
            self.logger.error(f"TARGETED ERROR: {request.symbol} - {e}")
            
            # Create error response
            response = TargetedNewsResponse(
                request_id=request.request_id,
                symbol=request.symbol,
                articles=[],
                collection_time=datetime.now(),
                processing_time_ms=processing_time,
                success=False,
                error_message=str(e)
            )
            
            return response
    
    def _collect_yahoo_news(self, symbol: str, max_articles: int, hours_back: int) -> List[NewsArticle]:
        """Collect news from Yahoo Finance for specific symbol"""
        articles = []
        
        try:
            # Yahoo Finance symbol-specific news URL
            url = f"https://finance.yahoo.com/quote/{symbol}/"
            
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find news articles on the symbol page
            news_elements = soup.find_all(['h3', 'h4'], class_=lambda x: x and ('news' in x.lower() or 'story' in x.lower()))
            
            cutoff_time = datetime.now() - timedelta(hours=hours_back)
            
            for element in news_elements[:max_articles * 2]:  # Get extra in case some are filtered
                try:
                    # Extract article data
                    title = element.get_text(strip=True)
                    if not title or len(title) < 10:
                        continue
                    
                    # Find link
                    link_element = element.find('a') or element.find_parent('a')
                    url = link_element.get('href', '') if link_element else ''
                    if url.startswith('/'):
                        url = f"https://finance.yahoo.com{url}"
                    
                    # Create article
                    article = NewsArticle(
                        title=title,
                        url=url,
                        source="Yahoo Finance",
                        published_date=datetime.now(),  # Yahoo doesn't provide exact timestamps easily
                        content="",  # For targeted collection, title is often sufficient
                        symbols_mentioned=[symbol],
                        author="Yahoo Finance"
                    )
                    
                    articles.append(article)
                    
                    if len(articles) >= max_articles:
                        break
                        
                except Exception as e:
                    self.logger.debug(f"Error parsing article element: {e}")
                    continue
            
            # If we didn't find enough on symbol page, try general finance news
            if len(articles) < max_articles:
                articles.extend(self._collect_yahoo_general_news(symbol, max_articles - len(articles)))
            
        except Exception as e:
            self.logger.error(f"Error collecting Yahoo news for {symbol}: {e}")
        
        return articles[:max_articles]
    
    def _collect_yahoo_general_news(self, symbol: str, needed: int) -> List[NewsArticle]:
        """Collect from Yahoo Finance general news that mentions the symbol"""
        articles = []
        
        try:
            url = "https://finance.yahoo.com/news/"
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find articles that mention the symbol
            for element in soup.find_all(['h3', 'h4', 'h2']):
                title_text = element.get_text(strip=True)
                
                # Check if symbol is mentioned in title
                if symbol.upper() in title_text.upper():
                    link_element = element.find('a') or element.find_parent('a')
                    url = link_element.get('href', '') if link_element else ''
                    if url.startswith('/'):
                        url = f"https://finance.yahoo.com{url}"
                    
                    article = NewsArticle(
                        title=title_text,
                        url=url,
                        source="Yahoo Finance News",
                        published_date=datetime.now(),
                        content="",
                        symbols_mentioned=[symbol],
                        author="Yahoo Finance"
                    )
                    
                    articles.append(article)
                    
                    if len(articles) >= needed:
                        break
        
        except Exception as e:
            self.logger.debug(f"Error collecting general Yahoo news: {e}")
        
        return articles
    
    def _update_metrics(self, processing_time_ms: float):
        """Update performance metrics"""
        self.requests_processed += 1
        
        if self.avg_response_time == 0:
            self.avg_response_time = processing_time_ms
        else:
            # Rolling average
            self.avg_response_time = (self.avg_response_time * 0.8) + (processing_time_ms * 0.2)
        
        self.last_request_time = datetime.now()
    
    def get_performance_stats(self) -> Dict[str, Any]:
        """Get performance statistics"""
        return {
            'requests_processed': self.requests_processed,
            'avg_response_time_ms': round(self.avg_response_time, 2),
            'last_request_time': self.last_request_time.isoformat() if self.last_request_time else None,
            'status': 'healthy' if self.avg_response_time < 5000 else 'slow'
        }


# Global instance for system-wide use
_targeted_collector = None

def get_targeted_collector() -> YahooTargetedCollector:
    """Get global targeted collector instance"""
    global _targeted_collector
    if _targeted_collector is None:
        _targeted_collector = YahooTargetedCollector()
    return _targeted_collector