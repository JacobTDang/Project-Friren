"""
Simple base classes for news data collection

Just defines the NewsArticle structure and basic interface.
The collector will handle FinBERT preparation later.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional, Dict
from abc import ABC, abstractmethod


@dataclass
class NewsArticle:
    """Simple, standardized news article structure"""
    title: str
    content: str
    source: str
    url: str
    published_date: datetime
    symbols_mentioned: List[str] = field(default_factory=list)
    author: Optional[str] = None
    
    def to_dict(self) -> Dict[str, any]:
        """Convert to dictionary with datetime serialization"""
        return {
            'title': self.title,
            'content': self.content,
            'source': self.source,
            'url': self.url,
            'published_date': self.published_date.isoformat(),
            'symbols_mentioned': self.symbols_mentioned,
            'author': self.author
        }


class NewsDataSource(ABC):
    """Simple abstract base for news sources"""

    def __init__(self, source_name: str):
        self.source_name = source_name

    @abstractmethod
    def collect_news(self, hours_back: int = 24, max_articles: int = 50) -> List[NewsArticle]:
        """Collect general news articles from this source"""
        pass

    @abstractmethod
    def get_symbol_news(self, symbol: str, hours_back: int = 24, max_articles: int = 20) -> List[NewsArticle]:
        """Get news specifically for a symbol - CRITICAL for decision engine watchlist"""
        pass

    @abstractmethod
    def get_watchlist_news(self, symbols: List[str], hours_back: int = 24, max_articles_per_symbol: int = 10) -> Dict[str, List[NewsArticle]]:
        """Get news for multiple symbols (watchlist) - returns {symbol: [articles]}"""
        pass

    @abstractmethod
    def test_connection(self) -> bool:
        """Test if the news source is accessible"""
        pass
