"""
News Collection Output Formatter for Friren Trading System
==========================================================

Formats news collection output to provide real-time visibility into
article collection, processing, and source attribution.

Target Format:
[NEWS COLLECTOR] AAPL: 'Apple Reports Strong Q4 Earnings Beat' from MarketWatch - collected 15:21:45
[NEWS PROCESSOR] AAPL: processed 12 articles in 847ms
"""

from datetime import datetime
from typing import Dict, List, Optional, Any
from dataclasses import dataclass


@dataclass
class CollectedArticle:
    """Real article data structure - NO HARDCODED DEFAULTS"""
    symbol: str
    title: str
    source: str
    url: str
    content: str
    collected_at: datetime
    relevance_score: float
    article_id: str
    api_confidence: float
    market_impact_predicted: float


class NewsFormatter:
    """Standardized news collection output formatter"""
    
    @staticmethod
    def format_news_collection(symbol: str, title: str, source: str, timestamp: datetime) -> str:
        """
        Format individual news article collection
        
        Args:
            symbol: Stock symbol (e.g., 'AAPL')
            title: Real article title from API
            source: Actual source name (Yahoo Finance, MarketWatch, etc.)
            timestamp: Precise API request timestamp
            
        Returns:
            Formatted news collection string
        """
        time_str = timestamp.strftime("%H:%M:%S")
        truncated_title = title[:60] + "..." if len(title) > 60 else title
        return f"[NEWS COLLECTOR] {symbol}: '{truncated_title}' from {source} - collected {time_str}"
    
    @staticmethod
    def format_news_collection_with_sentiment(symbol: str, title: str, sentiment: str) -> str:
        """
        Format news collection with sentiment analysis result - standard format
        
        Args:
            symbol: Stock symbol (e.g., 'AAPL')
            title: Real article title from API
            sentiment: Sentiment analysis result (POSITIVE/NEGATIVE/NEUTRAL)
            
        Returns:
            Formatted news collection string matching standard format
        """
        truncated_title = title[:60] + "..." if len(title) > 60 else title
        return f"[NEWS PIPELINE] News collected: '{truncated_title}' for {symbol} - {sentiment}"
    
    @staticmethod
    def format_article_processing(symbol: str, articles_count: int, processing_time_ms: float) -> str:
        """
        Format article processing summary
        
        Args:
            symbol: Stock symbol
            articles_count: Real number of articles processed
            processing_time_ms: Actual processing time in milliseconds
            
        Returns:
            Formatted processing summary
        """
        return f"[NEWS PROCESSOR] {symbol}: processed {articles_count} articles in {processing_time_ms:.0f}ms"
    
    @staticmethod
    def format_source_status(source: str, status: str, articles_collected: int, 
                           response_time_ms: float) -> str:
        """
        Format news source status information
        
        Args:
            source: Source name (Yahoo Finance, etc.)
            status: Current status (ACTIVE, RATE_LIMITED, ERROR)
            articles_collected: Number of articles collected from this source
            response_time_ms: Average response time
            
        Returns:
            Formatted source status
        """
        return f"[NEWS SOURCE] {source}: {status} | articles: {articles_collected} | response: {response_time_ms:.0f}ms"
    
    @staticmethod
    def format_batch_collection(symbol: str, total_articles: int, sources_used: List[str], 
                              total_time_ms: float) -> str:
        """
        Format batch news collection summary
        
        Args:
            symbol: Stock symbol
            total_articles: Total articles collected across all sources
            sources_used: List of source names used
            total_time_ms: Total collection time
            
        Returns:
            Formatted batch collection summary
        """
        sources_str = ", ".join(sources_used)
        return f"[NEWS BATCH] {symbol}: {total_articles} articles from {len(sources_used)} sources ({sources_str}) in {total_time_ms:.0f}ms"
    
    @staticmethod
    def format_relevance_filtering(symbol: str, articles_before: int, articles_after: int, 
                                 threshold: float) -> str:
        """
        Format relevance filtering results
        
        Args:
            symbol: Stock symbol
            articles_before: Articles before filtering
            articles_after: Articles after filtering
            threshold: Relevance threshold used
            
        Returns:
            Formatted filtering results
        """
        filtered_count = articles_before - articles_after
        return f"[NEWS FILTER] {symbol}: {filtered_count} articles filtered (threshold: {threshold:.2f}) | {articles_after} relevant articles remain"
    
    @staticmethod
    def format_deduplication(symbol: str, articles_before: int, articles_after: int, 
                           duplicates_removed: int) -> str:
        """
        Format article deduplication results
        
        Args:
            symbol: Stock symbol
            articles_before: Articles before deduplication
            articles_after: Articles after deduplication
            duplicates_removed: Number of duplicates removed
            
        Returns:
            Formatted deduplication results
        """
        return f"[NEWS DEDUP] {symbol}: {duplicates_removed} duplicates removed | {articles_after} unique articles remain"
    
    @staticmethod
    def format_collection_error(symbol: str, source: str, error_type: str, 
                              retry_count: int) -> str:
        """
        Format news collection errors
        
        Args:
            symbol: Stock symbol
            source: Source that failed
            error_type: Type of error (RATE_LIMITED, TIMEOUT, API_ERROR)
            retry_count: Number of retries attempted
            
        Returns:
            Formatted error message
        """
        return f"[NEWS ERROR] {symbol}: {source} failed ({error_type}) | retries: {retry_count}"
    
    @staticmethod
    def format_collection_metrics(symbol: str, metrics: Dict[str, Any]) -> str:
        """
        Format comprehensive collection metrics
        
        Args:
            symbol: Stock symbol
            metrics: Dictionary containing collection metrics
            
        Returns:
            Formatted metrics summary
        """
        # ZERO-HARDCODING: All metrics must be provided by caller - NO DEFAULTS
        total_articles = metrics['total_articles']
        success_rate = metrics['success_rate']
        avg_response_time = metrics['avg_response_time_ms']
        sources_active = metrics['sources_active']
        
        return f"[NEWS METRICS] {symbol}: {total_articles} articles | {success_rate:.1f}% success | {avg_response_time:.0f}ms avg | {sources_active} sources active"