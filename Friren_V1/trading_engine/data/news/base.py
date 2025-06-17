"""
Simple base classes for news data collection

Just defines the NewsArticle structure and basic interface.
The collector will handle FinBERT preparation later.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional
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


class NewsDataSource(ABC):
    """Simple abstract base for news sources"""

    def __init__(self, source_name: str):
        self.source_name = source_name

    @abstractmethod
    def collect_news(self, hours_back: int = 24, max_articles: int = 50) -> List[NewsArticle]:
        """Collect news articles from this source"""
        pass

    @abstractmethod
    def test_connection(self) -> bool:
        """Test if the news source is accessible"""
        pass
