"""
Source Manager - Modular News Collection Component

Handles initialization and management of news sources:
- Lazy loading of news source instances
- Source weight management for sentiment calculation
- Timeout protection for source initialization
- Source utility methods

Extracted from news_collector.py for improved modularity.
"""

import logging
import threading
import time
from typing import Dict, List, Optional, Any

# Import configuration manager to eliminate ALL hardcoded values
from Friren_V1.infrastructure.configuration_manager import get_config

try:
    # Import all news sources
    from Friren_V1.trading_engine.data.news.yahoo_news import YahooFinanceNews
    from Friren_V1.trading_engine.data.news.marketaux_api import MarketauxNews
    from Friren_V1.trading_engine.data.news.alpha_vintage_api import AlphaVantageNews
    from Friren_V1.trading_engine.data.news.fmp_api import FMPNews
    from Friren_V1.trading_engine.data.news.news_api import NewsAPIData
    from Friren_V1.trading_engine.data.news.reddit_api import RedditNews
except ImportError as e:
    # Fallback for relative imports
    try:
        from ..yahoo_news import YahooFinanceNews
        from ..marketaux_api import MarketauxNews
        from ..alpha_vintage_api import AlphaVantageNews
        from ..fmp_api import FMPNews
        from ..news_api import NewsAPIData
        from ..reddit_api import RedditNews
    except ImportError as e2:
        # PRODUCTION: NO FALLBACKS - System must fail if dependencies are missing
        raise ImportError(f"CRITICAL: News source dependencies required for production: {e}, {e2}. Install missing packages and configure API keys.")


class SourceManager:
    """
    Manages news source initialization and configuration
    Handles lazy loading and source weight management
    """

    def __init__(self):
        """Initialize the source manager with lazy loading"""
        self.logger = logging.getLogger("source_manager")

        # Initialize with empty news sources - will be loaded on demand
        self.news_sources: Dict[str, Any] = {}
        self._sources_initialized = False

        # Initialize source weights for sentiment calculation
        # Source weights - FROM CONFIGURATION MANAGER
        self.source_weights = {
            'yahoo': get_config('SOURCE_WEIGHT_YAHOO', 0.25),            # Free professional financial news
            'alpha_vantage': get_config('SOURCE_WEIGHT_ALPHA_VANTAGE', 0.25),    # Market-moving news with sentiment
            'fmp': get_config('SOURCE_WEIGHT_FMP', 0.20),              # Earnings, analyst actions
            'reddit': get_config('SOURCE_WEIGHT_REDDIT', 0.15),           # Social sentiment
            'newsapi': get_config('SOURCE_WEIGHT_NEWSAPI', 0.10),          # Professional business news
            'marketaux': get_config('SOURCE_WEIGHT_MARKETAUX', 0.05)         # Supporting market news
        }

        self.logger.info("SourceManager: Basic initialization complete")
        self.logger.info("SourceManager: News sources will be initialized on first use")

    def ensure_sources_initialized(self):
        """Ensure news sources are initialized (lazy loading)"""
        if not self._sources_initialized:
            self.logger.info("Lazy loading: Initializing news sources on first use...")
            self._initialize_news_sources()
            self._sources_initialized = True
            self.logger.info("Lazy loading: News sources initialization complete")

    def _initialize_news_sources(self):
        """Initialize all available news sources with error handling and timeout protection"""
        # Full source list - try all sources with graceful fallback
        source_classes = {
            'yahoo': YahooFinanceNews,
            'alpha_vantage': AlphaVantageNews,
            'fmp': FMPNews,
            'reddit': RedditNews,
            'newsapi': NewsAPIData,
            'marketaux': MarketauxNews
        }

        self.logger.info(f"Initializing all available news sources: {list(source_classes.keys())}")

        for source_name, source_class in source_classes.items():
            try:
                self.logger.info(f"[DEBUG] About to initialize {source_name} news source...")

                # Use timeout protection for each source initialization
                init_result = [None]
                init_error = [None]

                def init_source():
                    try:
                        init_result[0] = source_class()
                    except Exception as e:
                        init_error[0] = e

                # Start initialization in a thread with timeout
                init_thread = threading.Thread(target=init_source, daemon=True)
                init_thread.start()

                # Wait for initialization with timeout (reduced for faster startup)
                timeout_seconds = get_config('SOURCE_INIT_TIMEOUT', 15)  # Timeout from configuration
                init_thread.join(timeout=timeout_seconds)

                if init_thread.is_alive():
                    self.logger.warning(f"Initialization of {source_name} timed out after {timeout_seconds} seconds, skipping")
                    continue

                if init_error[0]:
                    if isinstance(init_error[0], ValueError):
                        self.logger.warning(f"Skipping {source_name}: {init_error[0]}")
                    else:
                        self.logger.error(f"Error initializing {source_name}: {init_error[0]}")
                    continue

                if init_result[0]:
                    self.news_sources[source_name] = init_result[0]
                    self.logger.info(f"Initialized {source_name} news source")
                else:
                    self.logger.warning(f"Initialization of {source_name} returned None, skipping")

            except Exception as e:
                self.logger.error(f"Unexpected error initializing {source_name}: {e}")
                continue

        # Log initialization results
        successful_sources = list(self.news_sources.keys())
        self.logger.info(f"News source initialization complete. Successfully initialized {len(successful_sources)} sources: {successful_sources}")

        if not self.news_sources:
            self.logger.warning("No news sources were successfully initialized. Check API keys and credentials.")
        else:
            self.logger.info(f"Available news sources: {list(self.news_sources.keys())}")

    def get_available_sources(self) -> List[str]:
        """Get list of successfully initialized news sources"""
        return list(self.news_sources.keys())

    def get_source_weights(self) -> Dict[str, float]:
        """Get current source weights"""
        return self.source_weights.copy()

    def update_source_weights(self, new_weights: Dict[str, float]):
        """Update source weights for sentiment calculation"""
        self.source_weights.update(new_weights)
        self.logger.info(f"Updated source weights: {self.source_weights}")

    def extract_source_name(self, source: str) -> str:
        """Extract clean source name from article source"""
        source_lower = source.lower()

        if 'alpha' in source_lower:
            return 'alpha_vantage'
        elif 'fmp' in source_lower:
            return 'fmp'
        elif 'reddit' in source_lower:
            return 'reddit'
        elif 'newsapi' in source_lower:
            return 'newsapi'
        elif 'marketaux' in source_lower:
            return 'marketaux'
        elif 'yahoo' in source_lower:
            return 'yahoo'
        else:
            return 'unknown'

    def get_source_weight(self, source_name: str) -> float:
        """Get weight for a specific source"""
        return self.source_weights.get(source_name, 0.1)

    def is_initialized(self) -> bool:
        """Check if sources have been initialized"""
        return self._sources_initialized

    def get_source_count(self) -> int:
        """Get count of initialized sources"""
        return len(self.news_sources)