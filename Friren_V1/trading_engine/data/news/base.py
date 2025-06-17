"""
Abstract Base Classes for News Data Collection

Defines standard interfaces and data structures for all news sources.
Ensures consistent data format for FinBERT sentiment processing.

Key Requirements for FinBERT:
- Clean, standardized article text
- Symbol extraction and validation
- Source attribution and confidence scoring
- Timestamp information for relevance filtering
- Content length validation for model limits
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any, Set
import re
import logging
from enum import Enum


class NewsSourceType(Enum):
    """Types of news sources for processing logic"""
    FINANCIAL_NEWS = "financial_news"      # Professional financial news
    SOCIAL_MEDIA = "social_media"          # Reddit, Twitter, etc.
    COMPANY_OFFICIAL = "company_official"  # Earnings calls, press releases
    ANALYST_RESEARCH = "analyst_research"  # Research reports, upgrades/downgrades
    MARKET_DATA = "market_data"           # Market-focused news feeds


@dataclass
class NewsArticle:
    """
    Standardized news article structure for FinBERT processing

    This format ensures all news sources provide consistent data
    that meets FinBERT's input requirements.
    """
    # Core content (required for FinBERT)
    title: str                              # Article headline
    content: str                            # Article body/description
    source: str                             # Source name (e.g., "Yahoo Finance", "NewsAPI-Reuters")
    source_type: NewsSourceType             # Type of source for processing logic

    # Metadata (required)
    url: str                                # Original article URL
    published_date: datetime                # When article was published
    symbols_mentioned: List[str] = field(default_factory=list)  # Extracted stock symbols

    # Optional metadata
    author: Optional[str] = None            # Article author
    summary: Optional[str] = None           # Article summary (if available)

    # FinBERT processing metadata
    content_length: int = field(init=False) # Character count (calculated)
    clean_text: str = field(init=False)     # Preprocessed text for FinBERT
    relevance_score: float = 0.0            # 0-1 relevance score for symbol
    confidence_score: float = 0.0           # 0-1 source confidence score

    # Source-specific data
    engagement_metrics: Dict[str, Any] = field(default_factory=dict)  # Likes, shares, etc.
    additional_metadata: Dict[str, Any] = field(default_factory=dict) # Source-specific fields

    def __post_init__(self):
        """Calculate derived fields after initialization"""
        # Ensure content is not None
        if self.content is None:
            self.content = ""

        # Calculate content length
        self.content_length = len(self.title) + len(self.content)

        # Create clean text for FinBERT (title + content)
        self.clean_text = self._create_clean_text()

        # Validate and clean symbols
        self.symbols_mentioned = self._validate_symbols(self.symbols_mentioned)

    def _create_clean_text(self) -> str:
        """
        Create clean text for FinBERT processing

        Combines title and content, removes special characters,
        and ensures proper formatting for model input.
        """
        # Combine title and content
        full_text = f"{self.title}. {self.content}".strip()

        # Clean text for FinBERT
        # Remove excessive whitespace
        full_text = re.sub(r'\s+', ' ', full_text)

        # Remove special characters that might confuse the model
        full_text = re.sub(r'[^\w\s\.,;:!?\-\$%()&]', ' ', full_text)

        # Ensure it doesn't exceed FinBERT's typical input limits (512 tokens ≈ 2000 chars)
        if len(full_text) > 2000:
            full_text = full_text[:1997] + "..."

        return full_text.strip()

    def _validate_symbols(self, symbols: List[str]) -> List[str]:
        """Validate and clean symbol list"""
        if not symbols:
            return []

        validated = []
        for symbol in symbols:
            if symbol and isinstance(symbol, str):
                cleaned = symbol.upper().strip()
                if re.match(r'^[A-Z]{1,5}$', cleaned):  # Basic symbol validation
                    validated.append(cleaned)

        return list(set(validated))  # Remove duplicates

    def is_suitable_for_finbert(self) -> bool:
        """
        Check if article meets FinBERT processing requirements

        Returns:
            bool: True if article is suitable for sentiment analysis
        """
        # Minimum content length
        if self.content_length < 10:
            return False

        # Must have clean text
        if not self.clean_text or len(self.clean_text.strip()) < 10:
            return False

        # Should have at least one symbol (for targeted analysis)
        if not self.symbols_mentioned:
            return False

        # Check if content is substantive (not just navigation/boilerplate)
        substantive_keywords = ['earnings', 'revenue', 'profit', 'stock', 'shares', 'market',
                               'analyst', 'upgrade', 'downgrade', 'target', 'price', 'forecast']

        text_lower = self.clean_text.lower()
        has_financial_content = any(keyword in text_lower for keyword in substantive_keywords)

        return has_financial_content

    def get_finbert_input(self) -> str:
        """
        Get properly formatted text for FinBERT input

        Returns:
            str: Clean text ready for sentiment analysis
        """
        return self.clean_text

    def add_symbol(self, symbol: str) -> None:
        """Add a symbol to the article if valid"""
        if symbol and isinstance(symbol, str):
            cleaned = symbol.upper().strip()
            if re.match(r'^[A-Z]{1,5}$', cleaned) and cleaned not in self.symbols_mentioned:
                self.symbols_mentioned.append(cleaned)

    def set_relevance_score(self, symbol: str, score: float) -> None:
        """Set relevance score for a specific symbol"""
        self.relevance_score = max(0.0, min(1.0, score))

    def set_confidence_score(self, score: float) -> None:
        """Set overall confidence score for the source"""
        self.confidence_score = max(0.0, min(1.0, score))


class SymbolExtractorBase(ABC):
    """
    Abstract base class for symbol extraction

    Each news source can implement custom symbol extraction logic
    while maintaining consistent interface.
    """

    def __init__(self):
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self._load_known_symbols()

    @abstractmethod
    def _load_known_symbols(self) -> None:
        """Load known symbols set - implement in subclass"""
        pass

    @abstractmethod
    def extract_symbols(self, text: str) -> List[str]:
        """
        Extract stock symbols from text

        Args:
            text: Text to extract symbols from

        Returns:
            List[str]: Extracted and validated symbols
        """
        pass

    def validate_symbol(self, symbol: str) -> bool:
        """
        Validate if a symbol is legitimate

        Args:
            symbol: Symbol to validate

        Returns:
            bool: True if symbol appears valid
        """
        if not symbol or not isinstance(symbol, str):
            return False

        symbol = symbol.upper().strip()

        # Basic format validation
        if not re.match(r'^[A-Z]{1,5}$', symbol):
            return False

        # Check against common false positives
        false_positives = {'US', 'USA', 'UK', 'EU', 'CEO', 'CFO', 'IPO', 'SEC', 'FDA', 'AI', 'IT', 'API'}
        if symbol in false_positives:
            return False

        return True


class NewsDataSourceBase(ABC):
    """
    Abstract base class for all news data sources

    Ensures consistent interface and data quality for FinBERT processing.
    All news sources must implement this interface.
    """

    def __init__(self, source_name: str, source_type: NewsSourceType,
                 rate_limit_delay: float = 1.0, max_retries: int = 3):
        self.source_name = source_name
        self.source_type = source_type
        self.rate_limit_delay = rate_limit_delay
        self.max_retries = max_retries
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

        # Initialize symbol extractor
        self.symbol_extractor = self._create_symbol_extractor()

        # Source confidence score (0-1, based on reliability)
        self.base_confidence_score = self._get_base_confidence_score()

    @abstractmethod
    def _create_symbol_extractor(self) -> SymbolExtractorBase:
        """Create appropriate symbol extractor for this source"""
        pass

    @abstractmethod
    def _get_base_confidence_score(self) -> float:
        """Return base confidence score for this source (0-1)"""
        pass

    @abstractmethod
    def collect_news(self, hours_back: int = 24, max_articles: int = 50,
                    symbols: Optional[List[str]] = None) -> List[NewsArticle]:
        """
        Collect news articles from this source

        Args:
            hours_back: How many hours back to collect news
            max_articles: Maximum number of articles to return
            symbols: Optional list of symbols to focus on

        Returns:
            List[NewsArticle]: Collected articles meeting FinBERT requirements
        """
        pass

    @abstractmethod
    def test_connection(self) -> bool:
        """Test if the news source is accessible"""
        pass

    def post_process_articles(self, articles: List[NewsArticle]) -> List[NewsArticle]:
        """
        Post-process articles to ensure FinBERT compatibility

        Args:
            articles: Raw articles from source

        Returns:
            List[NewsArticle]: Processed and validated articles
        """
        processed = []

        for article in articles:
            try:
                # Set source confidence
                article.set_confidence_score(self.base_confidence_score)

                # Validate for FinBERT processing
                if article.is_suitable_for_finbert():
                    processed.append(article)
                else:
                    self.logger.debug(f"Article filtered out (not suitable for FinBERT): {article.title[:50]}...")

            except Exception as e:
                self.logger.warning(f"Error processing article: {e}")
                continue

        return processed

    def filter_by_relevance(self, articles: List[NewsArticle],
                           target_symbols: Set[str], min_relevance: float = 0.3) -> List[NewsArticle]:
        """
        Filter articles by relevance to target symbols

        Args:
            articles: Articles to filter
            target_symbols: Symbols we're interested in
            min_relevance: Minimum relevance score (0-1)

        Returns:
            List[NewsArticle]: Relevant articles
        """
        relevant = []

        for article in articles:
            relevance_score = 0.0

            # Calculate relevance based on symbol mentions
            if article.symbols_mentioned:
                matching_symbols = set(article.symbols_mentioned) & target_symbols
                if matching_symbols:
                    # Higher score for more symbol matches
                    relevance_score = len(matching_symbols) / len(target_symbols)

                    # Boost score if symbol is in title
                    title_lower = article.title.lower()
                    for symbol in matching_symbols:
                        if symbol.lower() in title_lower:
                            relevance_score += 0.2

            relevance_score = min(1.0, relevance_score)
            article.set_relevance_score("", relevance_score)  # General relevance

            if relevance_score >= min_relevance:
                relevant.append(article)

        return relevant

    def deduplicate_articles(self, articles: List[NewsArticle]) -> List[NewsArticle]:
        """
        Remove duplicate articles based on title similarity

        Args:
            articles: Articles to deduplicate

        Returns:
            List[NewsArticle]: Deduplicated articles
        """
        seen_titles = set()
        unique_articles = []

        for article in articles:
            # Create normalized title for comparison
            title_key = re.sub(r'\W+', '', article.title.lower())[:50]

            if title_key not in seen_titles:
                seen_titles.add(title_key)
                unique_articles.append(article)

        return unique_articles

    def get_source_info(self) -> Dict[str, Any]:
        """Get information about this news source"""
        return {
            'name': self.source_name,
            'type': self.source_type.value,
            'confidence_score': self.base_confidence_score,
            'rate_limit_delay': self.rate_limit_delay,
            'max_retries': self.max_retries
        }


# Utility functions for FinBERT integration
def prepare_articles_for_finbert(articles: List[NewsArticle],
                                target_symbol: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Prepare articles for FinBERT sentiment analysis

    Args:
        articles: Articles to prepare
        target_symbol: Optional symbol to focus on

    Returns:
        List[Dict]: Articles formatted for FinBERT input
    """
    finbert_inputs = []

    for article in articles:
        if not article.is_suitable_for_finbert():
            continue

        # Filter by symbol if specified
        if target_symbol and target_symbol not in article.symbols_mentioned:
            continue

        finbert_input = {
            'text': article.get_finbert_input(),
            'symbols': article.symbols_mentioned,
            'source': article.source,
            'source_type': article.source_type.value,
            'confidence': article.confidence_score,
            'published_date': article.published_date,
            'url': article.url,
            'metadata': {
                'title': article.title,
                'relevance_score': article.relevance_score,
                'content_length': article.content_length
            }
        }

        finbert_inputs.append(finbert_input)

    return finbert_inputs


def validate_finbert_compatibility(articles: List[NewsArticle]) -> Dict[str, Any]:
    """
    Validate a collection of articles for FinBERT compatibility

    Args:
        articles: Articles to validate

    Returns:
        Dict: Validation report
    """
    total_articles = len(articles)
    suitable_articles = sum(1 for a in articles if a.is_suitable_for_finbert())
    articles_with_symbols = sum(1 for a in articles if a.symbols_mentioned)

    avg_content_length = sum(a.content_length for a in articles) / total_articles if total_articles > 0 else 0

    # Get symbol distribution
    all_symbols = []
    for article in articles:
        all_symbols.extend(article.symbols_mentioned)

    from collections import Counter
    symbol_counts = Counter(all_symbols)

    return {
        'total_articles': total_articles,
        'suitable_for_finbert': suitable_articles,
        'finbert_ready_percentage': (suitable_articles / total_articles * 100) if total_articles > 0 else 0,
        'articles_with_symbols': articles_with_symbols,
        'symbol_extraction_rate': (articles_with_symbols / total_articles * 100) if total_articles > 0 else 0,
        'average_content_length': avg_content_length,
        'unique_symbols_found': len(symbol_counts),
        'top_symbols': dict(symbol_counts.most_common(10)),
        'source_distribution': {
            article.source: sum(1 for a in articles if a.source == article.source)
            for article in articles
        }
    }
