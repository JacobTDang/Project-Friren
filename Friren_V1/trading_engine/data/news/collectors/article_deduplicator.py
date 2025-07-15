"""
Article Deduplicator - Modular News Collection Component

Handles deduplication of news articles using multiple strategies:
- URL-based deduplication
- Title similarity matching
- Time-based clustering
- Content normalization

Extracted from news_collector.py for improved modularity.
"""

import re
import logging
from datetime import datetime
from typing import List
from ..base import NewsArticle


class ArticleDeduplicator:
    """
    Handles deduplication of news articles using multiple strategies
    Optimized for performance and accuracy
    """

    def __init__(self):
        """Initialize the article deduplicator"""
        self.logger = logging.getLogger("article_deduplicator")
        
    def deduplicate_articles(self, articles: List[NewsArticle]) -> List[NewsArticle]:
        """
        Remove duplicate articles using multiple strategies
        Optimized for t3.micro performance constraints
        """
        if not articles:
            return []

        self.logger.debug(f"Deduplicating {len(articles)} articles")

        # Strategy 1: Fast URL-based deduplication
        unique_articles = self._deduplicate_by_url(articles)

        # Strategy 2: Title similarity (for remaining articles)
        if len(unique_articles) > 1:
            unique_articles = self._deduplicate_by_title(unique_articles)

        # Strategy 3: Time-based clustering (for very similar articles)
        if len(unique_articles) > 5:  # Only for larger sets
            unique_articles = self._deduplicate_by_time_clustering(unique_articles)

        self.logger.debug(f"Deduplication complete: {len(unique_articles)} unique articles")
        return unique_articles

    def _deduplicate_by_url(self, articles: List[NewsArticle]) -> List[NewsArticle]:
        """Fast deduplication by normalized URL"""
        seen_urls = set()
        unique_articles = []

        for article in articles:
            normalized_url = self._normalize_url(article.url)

            if normalized_url not in seen_urls:
                seen_urls.add(normalized_url)
                unique_articles.append(article)

        return unique_articles

    def _deduplicate_by_title(self, articles: List[NewsArticle]) -> List[NewsArticle]:
        """Deduplication by normalized title similarity"""
        seen_titles = set()
        unique_articles = []

        for article in articles:
            title_key = self._normalize_title(article.title)

            if title_key not in seen_titles:
                seen_titles.add(title_key)
                unique_articles.append(article)

        return unique_articles

    def _deduplicate_by_time_clustering(self, articles: List[NewsArticle]) -> List[NewsArticle]:
        """Remove articles that are very similar and published within 2 hours"""
        if len(articles) <= 5:
            return articles  # Skip for small sets

        # Sort by publication time
        sorted_articles = sorted(articles, key=lambda x: x.published_date)
        unique_articles = []

        for article in sorted_articles:
            # Check if we have a similar article within 2 hours
            is_duplicate = False

            for existing in unique_articles:
                time_diff = abs((article.published_date - existing.published_date).total_seconds())

                if time_diff < 7200:  # 2 hours
                    # Check title similarity
                    if self._titles_similar(article.title, existing.title):
                        is_duplicate = True
                        break

            if not is_duplicate:
                unique_articles.append(article)

        return unique_articles

    def _normalize_url(self, url: str) -> str:
        """Normalize URL for comparison"""
        if not url:
            return ""

        # Remove query parameters and fragments
        url = url.split('?')[0].split('#')[0]

        # Normalize protocol and www
        url = url.replace('https://', '').replace('http://', '')
        url = url.replace('www.', '')

        return url.lower().strip('/')

    def _normalize_title(self, title: str) -> str:
        """Normalize title for comparison"""
        if not title:
            return ""

        # Remove special characters, convert to lowercase
        normalized = re.sub(r'[^\w\s]', '', title.lower())

        # Remove extra whitespace
        normalized = ' '.join(normalized.split())

        # Create a key from first 50 characters
        return normalized[:50]

    def _titles_similar(self, title1: str, title2: str, threshold: float = 0.7) -> bool:
        """Check if two titles are similar using simple word overlap"""
        words1 = set(self._normalize_title(title1).split())
        words2 = set(self._normalize_title(title2).split())

        if not words1 or not words2:
            return False

        intersection = len(words1.intersection(words2))
        union = len(words1.union(words2))

        similarity = intersection / union if union > 0 else 0
        return similarity >= threshold