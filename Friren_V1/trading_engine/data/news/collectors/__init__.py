"""
News Collectors - Modular News Collection Components

This package contains modular components extracted from the original news_collector.py
for improved maintainability and testing:

- ArticleDeduplicator: Handles article deduplication using multiple strategies
- SourceManager: Manages news source initialization and configuration  
- NewsCollector: Core news collection and processing functionality

Usage:
    from Friren_V1.trading_engine.data.news.collectors import (
        ArticleDeduplicator, SourceManager, NewsCollector
    )
    
    # Create a complete news collection system
    source_manager = SourceManager()
    deduplicator = ArticleDeduplicator()
    collector = NewsCollector(source_manager, deduplicator)
    
    # Collect news for a symbol
    news_data = collector.collect_symbol_news('AAPL')
"""

from .article_deduplicator import ArticleDeduplicator
from .source_manager import SourceManager
from .news_collector import NewsCollector, ProcessedNewsData

__all__ = ['ArticleDeduplicator', 'SourceManager', 'NewsCollector', 'ProcessedNewsData']