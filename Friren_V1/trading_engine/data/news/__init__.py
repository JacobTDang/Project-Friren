"""
Simple news data collection module

Just collects clean news data. The enhanced collector will handle FinBERT prep.
"""

from .base import NewsArticle, NewsDataSource
from .yahoo_news import YahooFinanceNews, SymbolExtractor

# Import NewsAPI if available
try:
    from .news_api import NewsAPIData
    NEWSAPI_AVAILABLE = True
except ImportError:
    NEWSAPI_AVAILABLE = False

# Import Reddit if available
try:
    from .reddit import RedditNews
    REDDIT_AVAILABLE = True
except ImportError:
    REDDIT_AVAILABLE = False

# Build __all__ dynamically
__all__ = [
    'NewsArticle',
    'NewsDataSource',
    'YahooFinanceNews',
    'SymbolExtractor'
]

if NEWSAPI_AVAILABLE:
    __all__.append('NewsAPIData')

if REDDIT_AVAILABLE:
    __all__.append('RedditNews')
