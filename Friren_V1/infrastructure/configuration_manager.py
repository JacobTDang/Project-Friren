"""
Configuration Management System for Friren Trading System

Provides centralized configuration management with environment-based settings,
validation, and hot-reloading capabilities. Eliminates all hardcoded values
throughout the trading system.

Features:
- Environment variable based configuration
- Configuration validation and type checking
- Environment-specific configuration profiles
- Hot-reloading for non-critical parameters
- Configuration change notifications
- Secure credential management
- Configuration audit logging
"""

import os
import json
import logging
import threading
from typing import Dict, Any, Optional, List, Union, Callable, Type
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime, timedelta
import re
from pathlib import Path


class ConfigurationEnvironment(Enum):
    """Configuration environment types"""
    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"
    TESTING = "testing"


class ConfigurationType(Enum):
    """Configuration value types"""
    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    LIST = "list"
    PATH = "path"
    URL = "url"
    JSON = "json"


@dataclass
class ConfigurationSchema:
    """Schema definition for configuration values"""
    name: str
    config_type: ConfigurationType
    required: bool = True
    default_value: Optional[Any] = None
    min_value: Optional[Union[int, float]] = None
    max_value: Optional[Union[int, float]] = None
    valid_values: Optional[List[Any]] = None
    description: str = ""
    sensitive: bool = False  # For credentials/secrets
    hot_reloadable: bool = True  # Can be changed at runtime
    validator: Optional[Callable[[Any], bool]] = None


@dataclass
class ConfigurationChange:
    """Represents a configuration change event"""
    timestamp: datetime
    config_name: str
    old_value: Any
    new_value: Any
    environment: ConfigurationEnvironment
    changed_by: str = "system"


class ConfigurationError(Exception):
    """Configuration-related errors"""
    pass


class ConfigurationManager:
    """
    Central configuration manager for the Friren trading system
    
    Manages all configuration parameters through environment variables
    with validation, type checking, and change notifications.
    """
    
    def __init__(self, environment: Optional[ConfigurationEnvironment] = None):
        self.logger = logging.getLogger("configuration_manager")
        
        # Determine environment
        self.environment = environment or self._detect_environment()
        
        # Configuration storage
        self._config_values: Dict[str, Any] = {}
        self._config_schemas: Dict[str, ConfigurationSchema] = {}
        self._change_listeners: List[Callable[[ConfigurationChange], None]] = []
        self._change_history: List[ConfigurationChange] = []
        
        # Thread safety
        self._lock = threading.RLock()
        
        # Initialize configuration schemas
        self._initialize_schemas()
        
        # Load configuration values
        self._load_configuration()
        
        # Validate configuration
        self._validate_configuration()
        
        self.logger.info(f"Configuration manager initialized for {self.environment.value} environment")
    
    def _detect_environment(self) -> ConfigurationEnvironment:
        """Detect environment from environment variables"""
        env_name = os.getenv('FRIREN_ENVIRONMENT', 'development').lower()
        
        try:
            return ConfigurationEnvironment(env_name)
        except ValueError:
            self.logger.warning(f"Unknown environment '{env_name}', defaulting to development")
            return ConfigurationEnvironment.DEVELOPMENT
    
    def _initialize_schemas(self):
        """Initialize configuration schemas for all system parameters"""
        
        # REDIS CONFIGURATION - CRITICAL
        self.register_schema(ConfigurationSchema(
            name="REDIS_HOST",
            config_type=ConfigurationType.STRING,
            required=True,
            default_value="localhost",
            description="Redis server hostname",
            hot_reloadable=False
        ))
        
        self.register_schema(ConfigurationSchema(
            name="REDIS_PORT",
            config_type=ConfigurationType.INTEGER,
            required=True,
            default_value=6379,
            min_value=1,
            max_value=65535,
            description="Redis server port",
            hot_reloadable=False
        ))
        
        self.register_schema(ConfigurationSchema(
            name="REDIS_DB",
            config_type=ConfigurationType.INTEGER,
            required=True,
            default_value=0,
            min_value=0,
            max_value=15,
            description="Redis database number",
            hot_reloadable=False
        ))
        
        self.register_schema(ConfigurationSchema(
            name="REDIS_MAX_CONNECTIONS",
            config_type=ConfigurationType.INTEGER,
            required=True,
            default_value=20,
            min_value=1,
            max_value=100,
            description="Maximum Redis connections in pool"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="REDIS_CONNECT_TIMEOUT",
            config_type=ConfigurationType.INTEGER,
            required=True,
            default_value=5,
            min_value=1,
            max_value=30,
            description="Redis connection timeout in seconds"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="REDIS_SOCKET_TIMEOUT",
            config_type=ConfigurationType.INTEGER,
            required=True,
            default_value=10,
            min_value=1,
            max_value=60,
            description="Redis socket timeout in seconds"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="REDIS_HEALTH_CHECK_INTERVAL",
            config_type=ConfigurationType.INTEGER,
            required=True,
            default_value=30,
            min_value=5,
            max_value=300,
            description="Redis health check interval in seconds"
        ))
        
        # XGBOOST MODEL CONFIGURATION - CRITICAL
        self.register_schema(ConfigurationSchema(
            name="XGBOOST_MODEL_PATH",
            config_type=ConfigurationType.PATH,
            required=True,
            default_value="models/demo_xgb_model.json",
            description="Path to XGBoost model file",
            hot_reloadable=False,
            validator=lambda x: Path(x).suffix in ['.json', '.pkl', '.model']
        ))
        
        self.register_schema(ConfigurationSchema(
            name="XGBOOST_BUY_THRESHOLD",
            config_type=ConfigurationType.FLOAT,
            required=True,
            default_value=0.65,
            min_value=0.0,
            max_value=1.0,
            description="XGBoost buy signal threshold"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="XGBOOST_SELL_THRESHOLD",
            config_type=ConfigurationType.FLOAT,
            required=True,
            default_value=0.35,
            min_value=0.0,
            max_value=1.0,
            description="XGBoost sell signal threshold"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="XGBOOST_RECOMMENDATION_THRESHOLD",
            config_type=ConfigurationType.FLOAT,
            required=True,
            default_value=0.65,
            min_value=0.0,
            max_value=1.0,
            description="XGBoost recommendation confidence threshold"
        ))
        
        # DISCOVERY SYSTEM CONFIGURATION - CRITICAL (TEMPORARY DEFAULT FOR TESTING)
        self.register_schema(ConfigurationSchema(
            name="DISCOVERY_MODE",
            config_type=ConfigurationType.STRING,
            required=True,
            default_value="CONFIGURED",  # Temporary default for data flow testing
            valid_values=["DYNAMIC", "CONFIGURED"],
            description="Discovery mode: DYNAMIC (finds unknown stocks) or CONFIGURED (scans your list)"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="DISCOVERY_SYMBOLS",
            config_type=ConfigurationType.LIST,
            required=True,
            default_value=["AAPL", "MSFT", "GOOGL"],  # Temporary default for data flow testing
            description="List of symbols for discovery scanning - REQUIRED"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="MAX_DISCOVERY_SYMBOLS",
            config_type=ConfigurationType.INTEGER,
            required=True,
            default_value=5,  # Temporary default for data flow testing
            min_value=1,
            max_value=50,
            description="Maximum number of symbols to scan during discovery - REQUIRED"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="DISCOVERY_HOURS_BACK",
            config_type=ConfigurationType.INTEGER,
            required=True,
            default_value=24,  # Temporary default for data flow testing
            min_value=1,
            max_value=168,
            description="Hours back to search for discovery news - REQUIRED"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="DISCOVERY_MAX_ARTICLES",
            config_type=ConfigurationType.INTEGER,
            required=True,
            default_value=8,  # Temporary default for data flow testing
            min_value=1,
            max_value=50,
            description="Maximum articles per source for discovery - REQUIRED"
        ))

        # MEMORY MANAGEMENT - CRITICAL
        self.register_schema(ConfigurationSchema(
            name="MEMORY_THRESHOLD_MB",
            config_type=ConfigurationType.INTEGER,
            required=True,
            default_value=2000 if self.environment == ConfigurationEnvironment.PRODUCTION else 8000,
            min_value=100,
            max_value=64000,
            description="Memory usage threshold in MB"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="MEMORY_RESUME_MB",
            config_type=ConfigurationType.INTEGER,
            required=True,
            default_value=1500 if self.environment == ConfigurationEnvironment.PRODUCTION else 6000,
            min_value=100,
            max_value=64000,
            description="Memory resume threshold in MB"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="MEMORY_EMERGENCY_MB",
            config_type=ConfigurationType.INTEGER,
            required=True,
            default_value=2500 if self.environment == ConfigurationEnvironment.PRODUCTION else 10000,
            min_value=100,
            max_value=64000,
            description="Emergency memory threshold in MB"
        ))
        
        # TRADING PARAMETERS - CRITICAL
        self.register_schema(ConfigurationSchema(
            name="TRADING_SYMBOLS",
            config_type=ConfigurationType.LIST,
            required=True,
            default_value=["AAPL", "MSFT", "GOOGL", "TSLA", "NVDA"] if self.environment != ConfigurationEnvironment.PRODUCTION else [],
            description="List of trading symbols (comma-separated)",
            validator=lambda x: all(isinstance(s, str) and len(s) <= 5 for s in x)
        ))
        
        self.register_schema(ConfigurationSchema(
            name="WATCHLIST_SIZE",
            config_type=ConfigurationType.INTEGER,
            required=True,
            default_value=50,
            min_value=1,
            max_value=500,
            description="Maximum watchlist size"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="MAX_POSITION_SIZE_PERCENT",
            config_type=ConfigurationType.FLOAT,
            required=True,
            default_value=10.0,
            min_value=0.1,
            max_value=50.0,
            description="Maximum position size as percentage of portfolio"
        ))
        
        # API CONFIGURATION
        self.register_schema(ConfigurationSchema(
            name="FMP_API_BASE_URL",
            config_type=ConfigurationType.URL,
            required=True,
            default_value="https://financialmodelingprep.com/api/v3",
            description="Financial Modeling Prep API base URL"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="FMP_API_V4_URL",
            config_type=ConfigurationType.URL,
            required=True,
            default_value="https://financialmodelingprep.com/api/v4",
            description="Financial Modeling Prep API v4 URL"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="FMP_MAX_DAILY_REQUESTS",
            config_type=ConfigurationType.INTEGER,
            required=True,
            default_value=250,
            min_value=1,
            max_value=10000,
            description="Maximum daily FMP API requests"
        ))
        
        # NEWS API CONFIGURATION
        self.register_schema(ConfigurationSchema(
            name="NEWS_API_KEY",
            config_type=ConfigurationType.STRING,
            required=False,
            default_value="",
            description="NewsAPI key for financial news collection",
            sensitive=True,
            hot_reloadable=False
        ))
        
        self.register_schema(ConfigurationSchema(
            name="NEWS_API_BASE_URL",
            config_type=ConfigurationType.URL,
            required=True,
            default_value="https://newsapi.org/v2",
            description="NewsAPI base URL"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="NEWS_API_MAX_DAILY_REQUESTS",
            config_type=ConfigurationType.INTEGER,
            required=True,
            default_value=500,
            min_value=1,
            max_value=10000,
            description="Maximum daily NewsAPI requests"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="NEWS_API_TIMEOUT_SECONDS",
            config_type=ConfigurationType.INTEGER,
            required=True,
            default_value=30,
            min_value=5,
            max_value=120,
            description="NewsAPI request timeout in seconds"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="NEWS_API_RATE_LIMIT_RESET_HOURS",
            config_type=ConfigurationType.INTEGER,
            required=True,
            default_value=1,
            min_value=1,
            max_value=24,
            description="Hours to wait after rate limit before retrying NewsAPI"
        ))
        
        # SYSTEM TIMING PARAMETERS
        self.register_schema(ConfigurationSchema(
            name="STARTUP_DELAY_SECONDS",
            config_type=ConfigurationType.INTEGER,
            required=True,
            default_value=3,
            min_value=0,
            max_value=60,
            description="System startup delay in seconds"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="RETRY_DELAY_SECONDS",
            config_type=ConfigurationType.INTEGER,
            required=True,
            default_value=5,
            min_value=1,
            max_value=300,
            description="Retry delay in seconds"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="HEALTH_CHECK_INTERVAL_SECONDS",
            config_type=ConfigurationType.INTEGER,
            required=True,
            default_value=60,
            min_value=5,
            max_value=3600,
            description="Health check interval in seconds"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="PROCESS_MONITOR_INTERVAL_SECONDS",
            config_type=ConfigurationType.INTEGER,
            required=True,
            default_value=10,
            min_value=1,
            max_value=300,
            description="Process monitoring interval in seconds"
        ))
        
        # WINDOWS HANDLE MONITOR CONFIGURATION
        self.register_schema(ConfigurationSchema(
            name="HANDLE_MONITOR_INTERVAL_SECONDS",
            config_type=ConfigurationType.INTEGER,
            required=True,
            default_value=30,
            min_value=5,
            max_value=300,
            description="Windows handle monitoring interval in seconds"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="HANDLE_WARNING_THRESHOLD",
            config_type=ConfigurationType.INTEGER,
            required=True,
            default_value=8000,
            min_value=1000,
            max_value=20000,
            description="Handle count warning threshold per process"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="HANDLE_CRITICAL_THRESHOLD",
            config_type=ConfigurationType.INTEGER,
            required=True,
            default_value=9500,
            min_value=2000,
            max_value=30000,
            description="Handle count critical threshold per process"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="HANDLE_LEAK_DETECTION_WINDOW_SECONDS",
            config_type=ConfigurationType.INTEGER,
            required=True,
            default_value=300,
            min_value=60,
            max_value=3600,
            description="Handle leak detection time window in seconds"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="HANDLE_MAX_GROWTH_RATE_PER_MINUTE",
            config_type=ConfigurationType.INTEGER,
            required=True,
            default_value=50,
            min_value=1,
            max_value=500,
            description="Maximum acceptable handle growth rate per minute"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="HANDLE_MAX_REDIS_CONNECTIONS",
            config_type=ConfigurationType.INTEGER,
            required=True,
            default_value=10,
            min_value=1,
            max_value=100,
            description="Maximum Redis connections in handle monitor pool"
        ))
        
        # FILE PATHS AND LOGGING
        self.register_schema(ConfigurationSchema(
            name="LOG_DIRECTORY",
            config_type=ConfigurationType.PATH,
            required=True,
            default_value="logs",
            description="Directory for log files"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="LOG_LEVEL",
            config_type=ConfigurationType.STRING,
            required=True,
            default_value="INFO",
            valid_values=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
            description="Logging level"
        ))
        
        
        # FINBERT CONFIGURATION
        self.register_schema(ConfigurationSchema(
            name="FINBERT_MODEL_NAME",
            config_type=ConfigurationType.STRING,
            required=True,
            default_value="ProsusAI/finbert",
            description="FinBERT model name from HuggingFace",
            hot_reloadable=False
        ))
        
        self.register_schema(ConfigurationSchema(
            name="FINBERT_MAX_LENGTH",
            config_type=ConfigurationType.INTEGER,
            required=True,
            default_value=512,
            min_value=64,
            max_value=2048,
            description="FinBERT maximum input length"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="FINBERT_BATCH_SIZE",
            config_type=ConfigurationType.INTEGER,
            required=True,
            default_value=8,
            min_value=1,
            max_value=32,
            description="FinBERT batch processing size"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="FINBERT_CONFIDENCE_THRESHOLD",
            config_type=ConfigurationType.FLOAT,
            required=True,
            default_value=0.5,
            min_value=0.0,
            max_value=1.0,
            description="FinBERT confidence threshold for reliable results"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="DEFAULT_SENTIMENT_SCORE",
            config_type=ConfigurationType.FLOAT,
            required=True,
            default_value=0.5,
            min_value=0.0,
            max_value=1.0,
            description="Default neutral sentiment score when analysis is not available"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="NEWS_HOURS_BACK",
            config_type=ConfigurationType.INTEGER,
            required=True,
            default_value=6,
            min_value=1,
            max_value=168,
            description="Hours back to search for news articles"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="MAX_ARTICLES_PER_SYMBOL",
            config_type=ConfigurationType.INTEGER,
            required=True,
            default_value=12,
            min_value=1,
            max_value=100,
            description="Maximum articles to collect per symbol"
        ))
        
        # NEWS PIPELINE CONFIGURATION - CRITICAL
        self.register_schema(ConfigurationSchema(
            name="NEWS_QUALITY_THRESHOLD",
            config_type=ConfigurationType.FLOAT,
            required=True,
            default_value=0.3,
            min_value=0.0,
            max_value=1.0,
            description="News quality threshold for filtering"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="NEWS_MIN_CONFIDENCE_THRESHOLD",
            config_type=ConfigurationType.FLOAT,
            required=True,
            default_value=0.3,
            min_value=0.0,
            max_value=1.0,
            description="Minimum confidence threshold for news analysis"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="NEWS_RECOMMENDATION_THRESHOLD",
            config_type=ConfigurationType.FLOAT,
            required=True,
            default_value=0.4,
            min_value=0.0,
            max_value=1.0,
            description="News recommendation generation threshold"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="NEWS_PIPELINE_VERSION",
            config_type=ConfigurationType.STRING,
            required=True,
            default_value="2.0",
            description="News pipeline version identifier"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="NEWS_CYCLE_INTERVAL_MINUTES",
            config_type=ConfigurationType.INTEGER,
            required=True,
            default_value=1,
            min_value=1,
            max_value=60,
            description="News collection cycle interval in minutes"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="NEWS_BATCH_SIZE",
            config_type=ConfigurationType.INTEGER,
            required=True,
            default_value=4,
            min_value=1,
            max_value=32,
            description="News processing batch size"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="NEWS_MAX_MEMORY_MB",
            config_type=ConfigurationType.INTEGER,
            required=True,
            default_value=4000,
            min_value=512,
            max_value=16000,
            description="Maximum memory usage for news processing in MB"
        ))
        
        # SCORING WEIGHTS CONFIGURATION
        self.register_schema(ConfigurationSchema(
            name="NEWS_MENTION_DIVISOR",
            config_type=ConfigurationType.FLOAT,
            required=True,
            default_value=3.0,
            min_value=1.0,
            max_value=10.0,
            description="Divisor for news mention scoring"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="NEWS_ARTICLE_DIVISOR",
            config_type=ConfigurationType.FLOAT,
            required=True,
            default_value=2.0,
            min_value=1.0,
            max_value=10.0,
            description="Divisor for article count scoring"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="NEWS_MENTION_WEIGHT",
            config_type=ConfigurationType.FLOAT,
            required=True,
            default_value=0.4,
            min_value=0.0,
            max_value=1.0,
            description="Weight for mention score in final calculation"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="NEWS_QUALITY_WEIGHT",
            config_type=ConfigurationType.FLOAT,
            required=True,
            default_value=0.3,
            min_value=0.0,
            max_value=1.0,
            description="Weight for quality score in final calculation"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="NEWS_SENTIMENT_WEIGHT",
            config_type=ConfigurationType.FLOAT,
            required=True,
            default_value=0.3,
            min_value=0.0,
            max_value=1.0,
            description="Weight for sentiment score in final calculation"
        ))
        
        # REDIS TIMEOUT CONFIGURATION
        self.register_schema(ConfigurationSchema(
            name="REDIS_TIMEOUT_FAST",
            config_type=ConfigurationType.FLOAT,
            required=True,
            default_value=0.1,
            min_value=0.01,
            max_value=5.0,
            description="Fast Redis operation timeout in seconds"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="REDIS_TIMEOUT_IMMEDIATE",
            config_type=ConfigurationType.FLOAT,
            required=True,
            default_value=0.01,
            min_value=0.001,
            max_value=1.0,
            description="Immediate Redis operation timeout in seconds"
        ))
        
        # CONFIDENCE THRESHOLDS
        self.register_schema(ConfigurationSchema(
            name="HIGH_CONFIDENCE_THRESHOLD",
            config_type=ConfigurationType.FLOAT,
            required=True,
            default_value=0.75,
            min_value=0.0,
            max_value=1.0,
            description="High confidence threshold for decisions"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="RECOMMENDATION_CONFIDENCE_THRESHOLD",
            config_type=ConfigurationType.FLOAT,
            required=True,
            default_value=0.65,
            min_value=0.0,
            max_value=1.0,
            description="Confidence threshold for generating recommendations"
        ))
        
        # FMP API CONFIGURATION
        self.register_schema(ConfigurationSchema(
            name="FMP_MAX_CONSECUTIVE_FAILURES",
            config_type=ConfigurationType.INTEGER,
            required=True,
            default_value=3,
            min_value=1,
            max_value=10,
            description="Maximum consecutive FMP API failures before circuit breaker"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="FMP_RATE_LIMIT_RESET_SECONDS",
            config_type=ConfigurationType.INTEGER,
            required=True,
            default_value=3600,
            min_value=300,
            max_value=86400,
            description="FMP API rate limit reset time in seconds"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="FMP_REQUEST_TIMEOUT",
            config_type=ConfigurationType.INTEGER,
            required=True,
            default_value=30,
            min_value=5,
            max_value=120,
            description="FMP API request timeout in seconds"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="FMP_FREE_TIER_LIMIT",
            config_type=ConfigurationType.INTEGER,
            required=True,
            default_value=20,
            min_value=1,
            max_value=100,
            description="FMP API free tier article limit per request"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="FMP_REQUEST_BUFFER",
            config_type=ConfigurationType.INTEGER,
            required=True,
            default_value=5,
            min_value=1,
            max_value=50,
            description="FMP API request buffer to avoid hitting limits"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="FMP_MAX_GAINERS",
            config_type=ConfigurationType.INTEGER,
            required=True,
            default_value=10,
            min_value=1,
            max_value=50,
            description="Maximum gainers to fetch from FMP API"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="FMP_MAX_LOSERS",
            config_type=ConfigurationType.INTEGER,
            required=True,
            default_value=10,
            min_value=1,
            max_value=50,
            description="Maximum losers to fetch from FMP API"
        ))
        
        # YAHOO FINANCE CONFIGURATION
        self.register_schema(ConfigurationSchema(
            name="YAHOO_QUOTE_URL_TEMPLATE",
            config_type=ConfigurationType.STRING,
            required=True,
            default_value="https://finance.yahoo.com/quote/{symbol}/",
            description="Yahoo Finance quote URL template"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="YAHOO_NEWS_URL_TEMPLATE",
            config_type=ConfigurationType.STRING,
            required=True,
            default_value="https://finance.yahoo.com/quote/{symbol}/news/",
            description="Yahoo Finance news URL template"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="YAHOO_GENERAL_NEWS_URL",
            config_type=ConfigurationType.STRING,
            required=True,
            default_value="https://finance.yahoo.com/news/",
            description="Yahoo Finance general news URL"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="YAHOO_RATE_LIMIT_DELAY",
            config_type=ConfigurationType.INTEGER,
            required=True,
            default_value=2,
            min_value=1,
            max_value=10,
            description="Yahoo Finance rate limiting delay in seconds"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="YAHOO_MAX_ARTICLES_PER_SECTION",
            config_type=ConfigurationType.INTEGER,
            required=True,
            default_value=50,
            min_value=1,
            max_value=200,
            description="Maximum articles to process per Yahoo section"
        ))
        
        # SYMBOL EXTRACTION PATTERNS
        self.register_schema(ConfigurationSchema(
            name="SYMBOL_PATTERN_DOLLAR",
            config_type=ConfigurationType.STRING,
            required=True,
            default_value=r'\$([A-Z]{1,5})\b',
            description="Regex pattern for dollar-prefixed symbols"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="SYMBOL_PATTERN_PARENTHESES",
            config_type=ConfigurationType.STRING,
            required=True,
            default_value=r'\(([A-Z]{1,5})\)',
            description="Regex pattern for parentheses-wrapped symbols"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="SYMBOL_PATTERN_PRICE",
            config_type=ConfigurationType.STRING,
            required=True,
            default_value=r'([A-Z]{2,5})[+-]?\s*\d+\.\d+%?',
            description="Regex pattern for symbols with price changes"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="MIN_TITLE_LENGTH",
            config_type=ConfigurationType.INTEGER,
            required=True,
            default_value=10,
            min_value=3,
            max_value=50,
            description="Minimum title length for article filtering"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="MIN_TEXT_LENGTH",
            config_type=ConfigurationType.INTEGER,
            required=True,
            default_value=10,
            min_value=3,
            max_value=50,
            description="Minimum text length for content filtering"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="MIN_SYMBOL_LENGTH",
            config_type=ConfigurationType.INTEGER,
            required=True,
            default_value=2,
            min_value=1,
            max_value=3,
            description="Minimum symbol length for extraction"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="MAX_SYMBOL_LENGTH",
            config_type=ConfigurationType.INTEGER,
            required=True,
            default_value=5,
            min_value=3,
            max_value=10,
            description="Maximum symbol length for extraction"
        ))
        
        # ALPHA VANTAGE CONFIGURATION
        self.register_schema(ConfigurationSchema(
            name="ALPHA_VANTAGE_DAILY_LIMIT",
            config_type=ConfigurationType.INTEGER,
            required=True,
            default_value=25,
            min_value=1,
            max_value=1000,
            description="Alpha Vantage daily request limit"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="ALPHA_VANTAGE_MAX_ARTICLES_PER_REQUEST",
            config_type=ConfigurationType.INTEGER,
            required=True,
            default_value=50,
            min_value=1,
            max_value=200,
            description="Alpha Vantage maximum articles per request"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="ALPHA_VANTAGE_DEFAULT_CONFIDENCE",
            config_type=ConfigurationType.FLOAT,
            required=True,
            default_value=0.8,
            min_value=0.0,
            max_value=1.0,
            description="Alpha Vantage default sentiment confidence"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="ALPHA_VANTAGE_REQUEST_TIMEOUT",
            config_type=ConfigurationType.INTEGER,
            required=True,
            default_value=30,
            min_value=5,
            max_value=120,
            description="Alpha Vantage request timeout in seconds"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="ALPHA_VANTAGE_TEST_TIMEOUT",
            config_type=ConfigurationType.INTEGER,
            required=True,
            default_value=10,
            min_value=1,
            max_value=60,
            description="Alpha Vantage test connection timeout in seconds"
        ))
        
        # NEWSAPI CONFIGURATION
        self.register_schema(ConfigurationSchema(
            name="NEWSAPI_MAX_CONSECUTIVE_FAILURES",
            config_type=ConfigurationType.INTEGER,
            required=True,
            default_value=3,
            min_value=1,
            max_value=10,
            description="NewsAPI maximum consecutive failures before circuit breaker"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="NEWSAPI_RATE_LIMIT_RESET_SECONDS",
            config_type=ConfigurationType.INTEGER,
            required=True,
            default_value=3600,
            min_value=300,
            max_value=86400,
            description="NewsAPI rate limit reset time in seconds"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="NEWSAPI_MAX_PAGE_SIZE",
            config_type=ConfigurationType.INTEGER,
            required=True,
            default_value=100,
            min_value=1,
            max_value=100,
            description="NewsAPI maximum page size per request"
        ))
        
        # XGBOOST FEATURE WEIGHTS
        self.register_schema(ConfigurationSchema(
            name="XGBOOST_SENTIMENT_WEIGHT",
            config_type=ConfigurationType.FLOAT,
            required=True,
            default_value=0.4,
            min_value=0.0,
            max_value=1.0,
            description="XGBoost sentiment score weight in feature calculation"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="XGBOOST_CONFIDENCE_WEIGHT",
            config_type=ConfigurationType.FLOAT,
            required=True,
            default_value=0.3,
            min_value=0.0,
            max_value=1.0,
            description="XGBoost confidence weight in feature calculation"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="XGBOOST_VOLUME_WEIGHT",
            config_type=ConfigurationType.FLOAT,
            required=True,
            default_value=0.2,
            min_value=0.0,
            max_value=1.0,
            description="XGBoost news volume weight in feature calculation"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="XGBOOST_QUALITY_WEIGHT",
            config_type=ConfigurationType.FLOAT,
            required=True,
            default_value=0.1,
            min_value=0.0,
            max_value=1.0,
            description="XGBoost data quality weight in feature calculation"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="XGBOOST_POSITIVE_SENTIMENT_THRESHOLD",
            config_type=ConfigurationType.FLOAT,
            required=True,
            default_value=0.3,
            min_value=0.0,
            max_value=1.0,
            description="XGBoost positive sentiment threshold for risk assessment"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="XGBOOST_NEGATIVE_SENTIMENT_THRESHOLD",
            config_type=ConfigurationType.FLOAT,
            required=True,
            default_value=-0.3,
            min_value=-1.0,
            max_value=0.0,
            description="XGBoost negative sentiment threshold for risk assessment"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="XGBOOST_HIGH_VOLUME_THRESHOLD",
            config_type=ConfigurationType.FLOAT,
            required=True,
            default_value=0.7,
            min_value=0.0,
            max_value=1.0,
            description="XGBoost high volume threshold for risk assessment"
        ))
        
        # FINBERT CACHE CONFIGURATION
        self.register_schema(ConfigurationSchema(
            name="FINBERT_CHARS_PER_TOKEN_RATIO",
            config_type=ConfigurationType.INTEGER,
            required=True,
            default_value=3,
            min_value=1,
            max_value=10,
            description="FinBERT estimated characters per token ratio"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="FINBERT_CACHE_SIZE_LIMIT",
            config_type=ConfigurationType.INTEGER,
            required=True,
            default_value=1000,
            min_value=100,
            max_value=10000,
            description="FinBERT cache size limit before cleanup"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="FINBERT_CACHE_CLEANUP_COUNT",
            config_type=ConfigurationType.INTEGER,
            required=True,
            default_value=250,
            min_value=10,
            max_value=1000,
            description="Number of cache entries to remove during cleanup"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="FINBERT_DEFAULT_MAX_LENGTH",
            config_type=ConfigurationType.INTEGER,
            required=True,
            default_value=256,
            min_value=64,
            max_value=2048,
            description="FinBERT default maximum input length"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="FINBERT_DEFAULT_BATCH_SIZE",
            config_type=ConfigurationType.INTEGER,
            required=True,
            default_value=4,
            min_value=1,
            max_value=32,
            description="FinBERT default batch processing size"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="FINBERT_MIN_CONFIDENCE_THRESHOLD",
            config_type=ConfigurationType.FLOAT,
            required=True,
            default_value=0.6,
            min_value=0.0,
            max_value=1.0,
            description="FinBERT minimum confidence threshold for reliable results"
        ))
        
        # SENTIMENT AGGREGATOR CONFIGURATION
        self.register_schema(ConfigurationSchema(
            name="SENTIMENT_MIN_CONFIDENCE_THRESHOLD",
            config_type=ConfigurationType.FLOAT,
            required=True,
            default_value=0.6,
            min_value=0.0,
            max_value=1.0,
            description="Sentiment aggregator minimum confidence threshold"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="SENTIMENT_HIGH_CONFIDENCE_THRESHOLD",
            config_type=ConfigurationType.FLOAT,
            required=True,
            default_value=0.75,
            min_value=0.0,
            max_value=1.0,
            description="Sentiment aggregator high confidence threshold"
        ))
        
        # SOURCE WEIGHTS CONFIGURATION
        self.register_schema(ConfigurationSchema(
            name="SOURCE_WEIGHT_YAHOO",
            config_type=ConfigurationType.FLOAT,
            required=True,
            default_value=0.25,
            min_value=0.0,
            max_value=1.0,
            description="Weight for Yahoo Finance news source"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="SOURCE_WEIGHT_ALPHA_VANTAGE",
            config_type=ConfigurationType.FLOAT,
            required=True,
            default_value=0.25,
            min_value=0.0,
            max_value=1.0,
            description="Weight for Alpha Vantage news source"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="SOURCE_WEIGHT_FMP",
            config_type=ConfigurationType.FLOAT,
            required=True,
            default_value=0.20,
            min_value=0.0,
            max_value=1.0,
            description="Weight for FMP news source"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="SOURCE_WEIGHT_REDDIT",
            config_type=ConfigurationType.FLOAT,
            required=True,
            default_value=0.15,
            min_value=0.0,
            max_value=1.0,
            description="Weight for Reddit news source"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="SOURCE_WEIGHT_NEWSAPI",
            config_type=ConfigurationType.FLOAT,
            required=True,
            default_value=0.10,
            min_value=0.0,
            max_value=1.0,
            description="Weight for NewsAPI news source"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="SOURCE_WEIGHT_MARKETAUX",
            config_type=ConfigurationType.FLOAT,
            required=True,
            default_value=0.05,
            min_value=0.0,
            max_value=1.0,
            description="Weight for Marketaux news source"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="SOURCE_INIT_TIMEOUT",
            config_type=ConfigurationType.INTEGER,
            required=True,
            default_value=15,
            min_value=1,
            max_value=60,
            description="Source initialization timeout in seconds"
        ))
        
        # NEWS COLLECTOR CONFIGURATION
        self.register_schema(ConfigurationSchema(
            name="NEWS_MIN_ARTICLES_FOR_CONFIDENCE",
            config_type=ConfigurationType.INTEGER,
            required=True,
            default_value=3,
            min_value=1,
            max_value=20,
            description="Minimum articles required for confidence calculation"
        ))
        
        self.register_schema(ConfigurationSchema(
            name="NEWS_MAX_KEY_ARTICLES",
            config_type=ConfigurationType.INTEGER,
            required=True,
            default_value=5,
            min_value=1,
            max_value=20,
            description="Maximum key articles to retain per symbol"
        ))
        
        # DEVELOPMENT/TESTING PARAMETERS
        if self.environment in [ConfigurationEnvironment.DEVELOPMENT, ConfigurationEnvironment.TESTING]:
            self.register_schema(ConfigurationSchema(
                name="TEST_SYMBOLS",
                config_type=ConfigurationType.LIST,
                required=False,
                default_value=["AAPL", "MSFT"],
                description="Symbols for testing purposes"
            ))
            
            self.register_schema(ConfigurationSchema(
                name="ENABLE_DEBUG_MODE",
                config_type=ConfigurationType.BOOLEAN,
                required=False,
                default_value=True,
                description="Enable debug mode features"
            ))
    
    def register_schema(self, schema: ConfigurationSchema):
        """Register a configuration schema"""
        with self._lock:
            self._config_schemas[schema.name] = schema
    
    def _load_configuration(self):
        """Load configuration values from environment variables"""
        with self._lock:
            for name, schema in self._config_schemas.items():
                # Get value from environment
                env_value = os.getenv(name)
                
                if env_value is not None:
                    # Parse the environment value
                    try:
                        parsed_value = self._parse_value(env_value, schema.config_type)
                        self._config_values[name] = parsed_value
                    except Exception as e:
                        if schema.required:
                            raise ConfigurationError(f"Failed to parse {name}: {e}")
                        else:
                            self.logger.warning(f"Failed to parse optional config {name}: {e}")
                            self._config_values[name] = schema.default_value
                elif schema.required:
                    if schema.default_value is not None:
                        self._config_values[name] = schema.default_value
                        self.logger.info(f"Using default value for {name}: {schema.default_value}")
                    else:
                        raise ConfigurationError(f"Required configuration {name} not found and no default value")
                else:
                    self._config_values[name] = schema.default_value
    
    def _parse_value(self, value: str, config_type: ConfigurationType) -> Any:
        """Parse string value based on configuration type"""
        if config_type == ConfigurationType.STRING:
            return value
        elif config_type == ConfigurationType.INTEGER:
            return int(value)
        elif config_type == ConfigurationType.FLOAT:
            return float(value)
        elif config_type == ConfigurationType.BOOLEAN:
            return value.lower() in ('true', '1', 'yes', 'on')
        elif config_type == ConfigurationType.LIST:
            return [item.strip() for item in value.split(',') if item.strip()]
        elif config_type == ConfigurationType.PATH:
            return Path(value)
        elif config_type == ConfigurationType.URL:
            if not value.startswith(('http://', 'https://')):
                raise ValueError(f"URL must start with http:// or https://")
            return value
        elif config_type == ConfigurationType.JSON:
            return json.loads(value)
        else:
            raise ValueError(f"Unknown configuration type: {config_type}")
    
    def _validate_configuration(self):
        """Validate all configuration values"""
        with self._lock:
            errors = []
            
            for name, value in self._config_values.items():
                schema = self._config_schemas[name]
                
                # Type validation
                if not self._validate_type(value, schema.config_type):
                    errors.append(f"{name}: Invalid type")
                    continue
                
                # Range validation
                if schema.min_value is not None and value < schema.min_value:
                    errors.append(f"{name}: Value {value} below minimum {schema.min_value}")
                
                if schema.max_value is not None and value > schema.max_value:
                    errors.append(f"{name}: Value {value} above maximum {schema.max_value}")
                
                # Valid values validation
                if schema.valid_values is not None and value not in schema.valid_values:
                    errors.append(f"{name}: Value {value} not in valid values {schema.valid_values}")
                
                # Custom validator
                if schema.validator is not None and not schema.validator(value):
                    errors.append(f"{name}: Failed custom validation")
                
                # Path existence validation
                if schema.config_type == ConfigurationType.PATH and schema.required:
                    path = Path(value)
                    if name.endswith('_DIRECTORY') and not path.parent.exists():
                        # For directories, ensure parent exists
                        errors.append(f"{name}: Parent directory does not exist: {path.parent}")
            
            if errors:
                error_msg = "Configuration validation failed:\n" + "\n".join(f"  - {error}" for error in errors)
                raise ConfigurationError(error_msg)
    
    def _validate_type(self, value: Any, config_type: ConfigurationType) -> bool:
        """Validate value type matches configuration type"""
        if config_type == ConfigurationType.STRING:
            return isinstance(value, str)
        elif config_type == ConfigurationType.INTEGER:
            return isinstance(value, int)
        elif config_type == ConfigurationType.FLOAT:
            return isinstance(value, (int, float))
        elif config_type == ConfigurationType.BOOLEAN:
            return isinstance(value, bool)
        elif config_type == ConfigurationType.LIST:
            return isinstance(value, list)
        elif config_type == ConfigurationType.PATH:
            return isinstance(value, (str, Path))
        elif config_type == ConfigurationType.URL:
            return isinstance(value, str)
        elif config_type == ConfigurationType.JSON:
            return True  # Any JSON-serializable type
        return False
    
    def get(self, name: str, default: Any = None) -> Any:
        """Get configuration value"""
        with self._lock:
            if name in self._config_values:
                return self._config_values[name]
            elif name in self._config_schemas:
                return self._config_schemas[name].default_value
            else:
                return default
    
    def set(self, name: str, value: Any, changed_by: str = "system") -> bool:
        """Set configuration value with validation"""
        with self._lock:
            if name not in self._config_schemas:
                raise ConfigurationError(f"Unknown configuration: {name}")
            
            schema = self._config_schemas[name]
            
            if not schema.hot_reloadable:
                raise ConfigurationError(f"Configuration {name} is not hot-reloadable")
            
            # Validate new value
            if not self._validate_type(value, schema.config_type):
                raise ConfigurationError(f"Invalid type for {name}")
            
            if schema.min_value is not None and value < schema.min_value:
                raise ConfigurationError(f"Value below minimum for {name}")
            
            if schema.max_value is not None and value > schema.max_value:
                raise ConfigurationError(f"Value above maximum for {name}")
            
            if schema.valid_values is not None and value not in schema.valid_values:
                raise ConfigurationError(f"Invalid value for {name}")
            
            if schema.validator is not None and not schema.validator(value):
                raise ConfigurationError(f"Failed validation for {name}")
            
            # Record change
            old_value = self._config_values.get(name)
            self._config_values[name] = value
            
            change = ConfigurationChange(
                timestamp=datetime.now(),
                config_name=name,
                old_value=old_value,
                new_value=value,
                environment=self.environment,
                changed_by=changed_by
            )
            
            self._change_history.append(change)
            
            # Notify listeners
            for listener in self._change_listeners:
                try:
                    listener(change)
                except Exception as e:
                    self.logger.error(f"Error notifying configuration change listener: {e}")
            
            self.logger.info(f"Configuration {name} changed from {old_value} to {value} by {changed_by}")
            return True
    
    def get_all(self) -> Dict[str, Any]:
        """Get all configuration values"""
        with self._lock:
            return self._config_values.copy()
    
    def get_schema(self, name: str) -> Optional[ConfigurationSchema]:
        """Get configuration schema"""
        return self._config_schemas.get(name)
    
    def get_all_schemas(self) -> Dict[str, ConfigurationSchema]:
        """Get all configuration schemas"""
        return self._config_schemas.copy()
    
    def add_change_listener(self, listener: Callable[[ConfigurationChange], None]):
        """Add configuration change listener"""
        with self._lock:
            self._change_listeners.append(listener)
    
    def remove_change_listener(self, listener: Callable[[ConfigurationChange], None]):
        """Remove configuration change listener"""
        with self._lock:
            if listener in self._change_listeners:
                self._change_listeners.remove(listener)
    
    def get_change_history(self, limit: Optional[int] = None) -> List[ConfigurationChange]:
        """Get configuration change history"""
        with self._lock:
            if limit is not None:
                return self._change_history[-limit:]
            return self._change_history.copy()
    
    def export_configuration(self, include_sensitive: bool = False) -> Dict[str, Any]:
        """Export configuration for backup or transfer"""
        with self._lock:
            export_data = {
                'environment': self.environment.value,
                'timestamp': datetime.now().isoformat(),
                'configuration': {}
            }
            
            for name, value in self._config_values.items():
                schema = self._config_schemas[name]
                if not schema.sensitive or include_sensitive:
                    export_data['configuration'][name] = value
                else:
                    export_data['configuration'][name] = "***REDACTED***"
            
            return export_data
    
    def validate_environment_readiness(self) -> Dict[str, Any]:
        """Validate system readiness for current environment"""
        issues = []
        warnings = []
        
        with self._lock:
            # Check critical paths exist
            critical_paths = ['XGBOOST_MODEL_PATH', 'DATABASE_PATH']
            for path_config in critical_paths:
                if path_config in self._config_values:
                    path = Path(self._config_values[path_config])
                    if not path.exists() and path_config == 'XGBOOST_MODEL_PATH':
                        issues.append(f"XGBoost model file not found: {path}")
                    elif not path.parent.exists():
                        issues.append(f"Parent directory does not exist: {path.parent}")
            
            # Check production-specific requirements
            if self.environment == ConfigurationEnvironment.PRODUCTION:
                if not self._config_values.get('TRADING_SYMBOLS'):
                    issues.append("TRADING_SYMBOLS must be specified for production")
                
                if self._config_values.get('MEMORY_THRESHOLD_MB', 0) > 3000:
                    warnings.append("Memory threshold above 3GB may not work on t3.micro")
                
                if self._config_values.get('ENABLE_DEBUG_MODE', False):
                    warnings.append("Debug mode enabled in production")
            
            # Check API configuration
            if not self._config_values.get('FMP_API_BASE_URL', '').startswith('https://'):
                warnings.append("FMP API URL should use HTTPS")
            
            # Check NewsAPI configuration
            if not self._config_values.get('NEWS_API_KEY'):
                warnings.append("NEWS_API_KEY not configured - NewsAPI features will be disabled")
            
            if not self._config_values.get('NEWS_API_BASE_URL', '').startswith('https://'):
                warnings.append("NewsAPI URL should use HTTPS")
            
            return {
                'environment': self.environment.value,
                'ready': len(issues) == 0,
                'issues': issues,
                'warnings': warnings,
                'config_count': len(self._config_values),
                'validation_timestamp': datetime.now().isoformat()
            }


# Global configuration manager instance
_config_manager: Optional[ConfigurationManager] = None


def get_config_manager() -> ConfigurationManager:
    """Get global configuration manager instance"""
    global _config_manager
    if _config_manager is None:
        _config_manager = ConfigurationManager()
    return _config_manager


def get_config(name: str, default: Any = None) -> Any:
    """Get configuration value using global manager"""
    return get_config_manager().get(name, default)


def set_config(name: str, value: Any, changed_by: str = "system") -> bool:
    """Set configuration value using global manager"""
    return get_config_manager().set(name, value, changed_by)


# Convenience functions for common configuration patterns
def get_redis_config() -> Dict[str, Any]:
    """Get Redis configuration parameters"""
    manager = get_config_manager()
    return {
        'host': manager.get('REDIS_HOST'),
        'port': manager.get('REDIS_PORT'),
        'db': manager.get('REDIS_DB'),
        'max_connections': manager.get('REDIS_MAX_CONNECTIONS'),
        'socket_connect_timeout': manager.get('REDIS_CONNECT_TIMEOUT'),
        'socket_timeout': manager.get('REDIS_SOCKET_TIMEOUT'),
        'health_check_interval': manager.get('REDIS_HEALTH_CHECK_INTERVAL')
    }


def get_xgboost_config() -> Dict[str, Any]:
    """Get XGBoost configuration parameters"""
    manager = get_config_manager()
    return {
        'model_path': manager.get('XGBOOST_MODEL_PATH'),
        'buy_threshold': manager.get('XGBOOST_BUY_THRESHOLD'),
        'sell_threshold': manager.get('XGBOOST_SELL_THRESHOLD'),
        'recommendation_threshold': manager.get('XGBOOST_RECOMMENDATION_THRESHOLD')
    }


def get_memory_config() -> Dict[str, Any]:
    """Get memory management configuration"""
    manager = get_config_manager()
    return {
        'threshold_mb': manager.get('MEMORY_THRESHOLD_MB'),
        'resume_mb': manager.get('MEMORY_RESUME_MB'),
        'emergency_mb': manager.get('MEMORY_EMERGENCY_MB')
    }


def get_trading_config() -> Dict[str, Any]:
    """Get trading configuration parameters"""
    manager = get_config_manager()
    return {
        'symbols': manager.get('TRADING_SYMBOLS'),
        'watchlist_size': manager.get('WATCHLIST_SIZE'),
        'max_position_size_percent': manager.get('MAX_POSITION_SIZE_PERCENT')
    }


def get_timing_config() -> Dict[str, Any]:
    """Get system timing configuration"""
    manager = get_config_manager()
    return {
        'startup_delay': manager.get('STARTUP_DELAY_SECONDS'),
        'retry_delay': manager.get('RETRY_DELAY_SECONDS'),
        'health_check_interval': manager.get('HEALTH_CHECK_INTERVAL_SECONDS'),
        'process_monitor_interval': manager.get('PROCESS_MONITOR_INTERVAL_SECONDS')
    }


def get_finbert_config() -> Dict[str, Any]:
    """Get FinBERT configuration parameters"""
    manager = get_config_manager()
    return {
        'model_name': manager.get('FINBERT_MODEL_NAME'),
        'max_length': manager.get('FINBERT_MAX_LENGTH'),
        'batch_size': manager.get('FINBERT_BATCH_SIZE'),
        'confidence_threshold': manager.get('FINBERT_CONFIDENCE_THRESHOLD'),
        'default_sentiment_score': manager.get('DEFAULT_SENTIMENT_SCORE'),
        'news_hours_back': manager.get('NEWS_HOURS_BACK'),
        'max_articles_per_symbol': manager.get('MAX_ARTICLES_PER_SYMBOL')
    }


def get_newsapi_config() -> Dict[str, Any]:
    """Get NewsAPI configuration parameters"""
    manager = get_config_manager()
    return {
        'api_key': manager.get('NEWS_API_KEY'),
        'base_url': manager.get('NEWS_API_BASE_URL'),
        'max_daily_requests': manager.get('NEWS_API_MAX_DAILY_REQUESTS'),
        'timeout_seconds': manager.get('NEWS_API_TIMEOUT_SECONDS'),
        'rate_limit_reset_hours': manager.get('NEWS_API_RATE_LIMIT_RESET_HOURS')
    }


def get_handle_monitor_config() -> Dict[str, Any]:
    """Get Windows handle monitor configuration parameters"""
    manager = get_config_manager()
    return {
        'monitoring_interval': manager.get('HANDLE_MONITOR_INTERVAL_SECONDS'),
        'warning_threshold': manager.get('HANDLE_WARNING_THRESHOLD'),
        'critical_threshold': manager.get('HANDLE_CRITICAL_THRESHOLD'),
        'leak_detection_window': manager.get('HANDLE_LEAK_DETECTION_WINDOW_SECONDS'),
        'max_growth_rate': manager.get('HANDLE_MAX_GROWTH_RATE_PER_MINUTE'),
        'max_redis_connections': manager.get('HANDLE_MAX_REDIS_CONNECTIONS')
    }


if __name__ == "__main__":
    # Example usage and testing
    config = ConfigurationManager()
    
    print("Configuration Manager Test")
    print("=" * 40)
    
    # Test getting values
    print(f"Redis Host: {config.get('REDIS_HOST')}")
    print(f"XGBoost Model Path: {config.get('XGBOOST_MODEL_PATH')}")
    print(f"Trading Symbols: {config.get('TRADING_SYMBOLS')}")
    
    # Test validation
    readiness = config.validate_environment_readiness()
    print(f"\nEnvironment Readiness: {readiness}")
    
    # Test convenience functions
    redis_config = get_redis_config()
    print(f"\nRedis Config: {redis_config}")
    
    # Export configuration
    exported = config.export_configuration()
    print(f"\nExported Configuration Keys: {list(exported['configuration'].keys())}")