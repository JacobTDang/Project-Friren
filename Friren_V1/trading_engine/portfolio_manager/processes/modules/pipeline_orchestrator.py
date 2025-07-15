"""
Pipeline Orchestrator Module for Enhanced News Pipeline
======================================================

Main orchestration module that coordinates news collection, FinBERT analysis,
and XGBoost recommendations. This module contains the main process class and
handles the complete pipeline workflow integration.

Extracted from enhanced_news_pipeline_process.py for better maintainability.
"""

import sys
import os
import time
import asyncio
import threading
import logging
import psutil
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from enum import Enum
from collections import deque, defaultdict

# Import infrastructure
from Friren_V1.multiprocess_infrastructure.redis_base_process import RedisBaseProcess, ProcessState
from Friren_V1.multiprocess_infrastructure.trading_redis_manager import (
    get_trading_redis_manager, create_process_message, MessagePriority, ProcessMessage
)

# Import output coordination
from Friren_V1.trading_engine.output.output_coordinator import OutputCoordinator

# Import the modular components
from .news_collection_module import NewsCollectionModule, NewsCollectionConfig, CollectionMetrics
from .finbert_analysis_module import FinBERTAnalysisModule, FinBERTConfig, AnalysisMetrics
from .xgboost_recommendation_module import XGBoostRecommendationModule, XGBoostConfig, RecommendationMetrics

# Import data types
from Friren_V1.trading_engine.data.news.base import NewsArticle
from Friren_V1.trading_engine.data.news.analysis import EnhancedSentimentResult
from Friren_V1.trading_engine.news.recommendations import TradingRecommendation


class MarketRegime(Enum):
    """Market regime for context"""
    BULLISH = "bullish"
    BEARISH = "bearish"
    SIDEWAYS = "sideways"
    HIGH_VOLATILITY = "high_volatility"


@dataclass
class PipelineConfig:
    """Configuration for the complete news pipeline process"""
    # Process settings
    cycle_interval_minutes: int = 1  # Ultra-fast cycle for responsive news collection
    batch_size: int = 4  # Max articles to process in one batch
    max_memory_mb: int = 1200  # Memory limit for subprocess context
    
    # Component configurations
    news_config: NewsCollectionConfig = field(default_factory=NewsCollectionConfig)
    finbert_config: FinBERTConfig = field(default_factory=FinBERTConfig)
    xgboost_config: XGBoostConfig = field(default_factory=XGBoostConfig)
    
    # Performance settings
    enable_caching: bool = True
    cache_ttl_minutes: int = 30
    max_processing_time_minutes: int = 10


@dataclass
class PipelineMetrics:
    """Comprehensive pipeline performance metrics"""
    # Overall metrics
    cycles_completed: int = 0
    total_processing_time_ms: float = 0.0
    average_cycle_time_ms: float = 0.0
    
    # Component metrics
    collection_metrics: Optional[CollectionMetrics] = None
    analysis_metrics: Optional[AnalysisMetrics] = None
    recommendation_metrics: Optional[RecommendationMetrics] = None
    
    # Resource metrics
    memory_usage_mb: float = 0.0
    cpu_usage_percent: float = 0.0
    
    # Error tracking
    error_count: int = 0
    last_error_time: Optional[datetime] = None
    
    # Idle state tracking
    idle_cycles: int = 0
    last_idle_time: Optional[datetime] = None
    
    # Timestamp
    timestamp: datetime = field(default_factory=datetime.now)


class EnhancedNewsPipelineOrchestrator(RedisBaseProcess):
    """
    Enhanced News Pipeline Orchestrator
    
    Main process that orchestrates the complete news-to-recommendation pipeline:
    1. News Collection - Gather news from multiple sources
    2. FinBERT Analysis - Sentiment analysis with impact calculation
    3. XGBoost Recommendations - ML-based trading recommendations
    4. Output Coordination - Standardized output formatting
    """
    
    def __init__(self,
                 process_id: str = "enhanced_news_pipeline",
                 watchlist_symbols: Optional[List[str]] = None,
                 config: Optional[PipelineConfig] = None):
        """
        Initialize pipeline orchestrator
        
        Args:
            process_id: Process identifier
            watchlist_symbols: List of symbols to monitor
            config: Pipeline configuration
        """
        # Handle config memory limit safely for subprocess context
        if config:
            if isinstance(config, dict):
                memory_limit = config.get('max_memory_mb', 1200)
            else:
                memory_limit = getattr(config, 'max_memory_mb', 1200)
        else:
            memory_limit = 1200
        super().__init__(process_id, memory_limit_mb=memory_limit)
        
        # Configuration
        if isinstance(config, dict):
            self.config = PipelineConfig(**config) if config else PipelineConfig()
        else:
            self.config = config or PipelineConfig()
        
        # Load symbols dynamically from database or use watchlist_symbols parameter
        self.watchlist_symbols = watchlist_symbols or []
        
        # Validate symbol list
        if not self.watchlist_symbols:
            self.logger.warning("No symbols provided to Enhanced News Pipeline - will attempt to load from database")
            try:
                from Friren_V1.trading_engine.portfolio_manager.tools.db_utils import load_dynamic_watchlist
                dynamic_symbols = load_dynamic_watchlist()
                if dynamic_symbols:
                    self.watchlist_symbols = dynamic_symbols
                    self.logger.info(f"Loaded {len(self.watchlist_symbols)} symbols from database: {self.watchlist_symbols}")
                else:
                    self.logger.error("No symbols available from database - Enhanced News Pipeline cannot operate")
                    raise ValueError("No symbols available for news pipeline operation")
            except Exception as e:
                self.logger.error(f"Failed to load symbols from database: {e}")
                raise ValueError(f"Enhanced News Pipeline requires symbols to operate: {e}")
        
        # Core modules
        self.news_collection: Optional[NewsCollectionModule] = None
        self.finbert_analysis: Optional[FinBERTAnalysisModule] = None
        self.xgboost_recommendation: Optional[XGBoostRecommendationModule] = None
        self.output_coordinator: Optional[OutputCoordinator] = None
        
        # State tracking
        self.last_run_time: Optional[datetime] = None
        self.pipeline_metrics = PipelineMetrics()
        self.symbol_tracking = {symbol: {
            'last_update': None,
            'recommendation_count': 0,
            'avg_confidence': 0.0,
            'last_recommendation': None
        } for symbol in self.watchlist_symbols}
        
        # Performance tracking
        self.processing_history = deque(maxlen=50)
        self.error_history = deque(maxlen=20)
        
        # Threading control
        self._pipeline_lock = threading.Lock()
        self._running_pipeline = False
        
        # Component availability tracking
        self.components_initialized = {
            'news_collection': False,
            'finbert_analysis': False,
            'xgboost_recommendation': False,
            'output_coordinator': False
        }
    
    def _initialize(self):
        """Initialize all pipeline components"""
        try:
            self.logger.info(f"=== ENHANCED NEWS PIPELINE ORCHESTRATOR INITIALIZATION START: {self.process_id} ===")
            self.logger.info(f"Initial state: {self.state.value}")
            self.logger.info(f"Watchlist symbols: {self.watchlist_symbols}")
            self.logger.info(f"Configuration: {self.config}")
            
            # Initialize OutputCoordinator first
            self.logger.info("Step 1: Initializing output coordinator...")
            self._initialize_output_coordinator()
            
            # Initialize News Collection Module
            self.logger.info("Step 2: Initializing news collection module...")
            self._initialize_news_collection()
            
            # Initialize FinBERT Analysis Module
            self.logger.info("Step 3: Initializing FinBERT analysis module...")
            self._initialize_finbert_analysis()
            
            # Initialize XGBoost Recommendation Module
            self.logger.info("Step 4: Initializing XGBoost recommendation module...")
            self._initialize_xgboost_recommendation()
            
            # Validate initialization
            self._validate_initialization()
            
            self.state = ProcessState.RUNNING
            self.logger.info("=== ENHANCED NEWS PIPELINE ORCHESTRATOR INITIALIZATION COMPLETE ===")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize Enhanced News Pipeline Orchestrator: {e}")
            self.state = ProcessState.ERROR
            raise
    
    def _initialize_output_coordinator(self):
        """Initialize output coordinator"""
        try:
            self.output_coordinator = OutputCoordinator(
                redis_client=self.redis_manager.redis_client if self.redis_manager else None,
                enable_terminal=True,
                enable_logging=True
            )
            self.components_initialized['output_coordinator'] = True
            self.logger.info("OutputCoordinator initialized successfully")
        except Exception as e:
            self.logger.error(f"Failed to initialize OutputCoordinator: {e}")
            self.output_coordinator = None
    
    def _initialize_news_collection(self):
        """Initialize news collection module"""
        try:
            self.news_collection = NewsCollectionModule(
                config=self.config.news_config,
                process_id=self.process_id
            )
            
            if self.news_collection.initialize():
                self.components_initialized['news_collection'] = True
                self.logger.info("News collection module initialized successfully")
            else:
                raise Exception("News collection module initialization failed")
                
        except Exception as e:
            self.logger.error(f"Failed to initialize news collection module: {e}")
            self.news_collection = None
    
    def _initialize_finbert_analysis(self):
        """Initialize FinBERT analysis module"""
        try:
            self.finbert_analysis = FinBERTAnalysisModule(
                config=self.config.finbert_config,
                output_coordinator=self.output_coordinator,
                process_id=self.process_id
            )
            
            if self.finbert_analysis.initialize():
                self.components_initialized['finbert_analysis'] = True
                self.logger.info("FinBERT analysis module initialized successfully")
            else:
                raise Exception("FinBERT analysis module initialization failed")
                
        except Exception as e:
            self.logger.error(f"Failed to initialize FinBERT analysis module: {e}")
            self.finbert_analysis = None
    
    def _initialize_xgboost_recommendation(self):
        """Initialize XGBoost recommendation module"""
        try:
            self.xgboost_recommendation = XGBoostRecommendationModule(
                config=self.config.xgboost_config,
                output_coordinator=self.output_coordinator,
                process_id=self.process_id
            )
            
            if self.xgboost_recommendation.initialize():
                self.components_initialized['xgboost_recommendation'] = True
                self.logger.info("XGBoost recommendation module initialized successfully")
            else:
                self.logger.warning("XGBoost recommendation module initialization failed - continuing without ML recommendations")
                
        except Exception as e:
            self.logger.warning(f"Failed to initialize XGBoost recommendation module: {e}")
            self.xgboost_recommendation = None
    
    def _validate_initialization(self):
        """Validate that critical components are initialized"""
        critical_components = ['news_collection', 'finbert_analysis']
        
        for component in critical_components:
            if not self.components_initialized.get(component, False):
                raise Exception(f"Critical component {component} failed to initialize")
        
        initialized_count = sum(self.components_initialized.values())
        total_count = len(self.components_initialized)
        
        self.logger.info(f"Pipeline initialization summary: {initialized_count}/{total_count} components initialized")
        
        if initialized_count < 2:  # Need at least news collection and analysis
            raise Exception("Insufficient components initialized for pipeline operation")
    
    def _execute(self):
        """Execute main pipeline logic"""
        self._process_cycle()
    
    def _process_cycle(self):
        """Main pipeline processing cycle"""
        try:
            # Check if we should run this cycle
            if not self._should_run_cycle():
                time.sleep(5)  # Short sleep before next check
                return
            
            self.logger.info(f"Starting enhanced news pipeline cycle #{self.pipeline_metrics.cycles_completed + 1}")
            cycle_start_time = time.time()
            
            # Step 1: Collect news for all symbols
            news_data = self._execute_news_collection()
            
            # Debug logging to identify the FinBERT issue
            self.logger.info(f"DEBUG: news_data structure: {type(news_data)}")
            self.logger.info(f"DEBUG: news_data keys: {list(news_data.keys()) if news_data else 'None'}")
            if news_data:
                for symbol, articles in news_data.items():
                    self.logger.info(f"DEBUG: {symbol} has {len(articles)} articles, type: {type(articles)}")
                    if articles:
                        self.logger.info(f"DEBUG: First article type for {symbol}: {type(articles[0])}")
            
            # Check the exact condition that's causing the problem
            has_data = bool(news_data)
            has_articles = any(articles for articles in news_data.values()) if news_data else False
            self.logger.info(f"DEBUG: has_data={has_data}, has_articles={has_articles}")
            
            if not news_data or not any(articles for articles in news_data.values()):
                self.logger.info("No news articles collected, marking as idle cycle")
                self._handle_idle_cycle()
                return
            
            # Step 2: Analyze sentiment using FinBERT
            sentiment_data = self._execute_sentiment_analysis(news_data)
            
            # Step 3: Generate XGBoost recommendations
            recommendations = self._execute_recommendation_generation(sentiment_data)
            
            # Step 4: Process and send results
            self._process_pipeline_results(news_data, sentiment_data, recommendations)
            
            # Update metrics and state
            cycle_time = (time.time() - cycle_start_time) * 1000
            self._update_pipeline_metrics(cycle_time)
            
            self.last_run_time = datetime.now()
            self.logger.info(f"Pipeline cycle completed in {cycle_time:.1f}ms")
            
        except Exception as e:
            self.logger.error(f"Error in pipeline cycle: {e}")
            self.pipeline_metrics.error_count += 1
            self.pipeline_metrics.last_error_time = datetime.now()
            self.error_history.append({
                'timestamp': datetime.now(),
                'error': str(e),
                'cycle': self.pipeline_metrics.cycles_completed
            })
            time.sleep(30)  # Wait before retry
    
    def _should_run_cycle(self) -> bool:
        """Check if pipeline should run this cycle"""
        if self.last_run_time is None:
            return True
        
        time_since_last = (datetime.now() - self.last_run_time).total_seconds() / 60
        return time_since_last >= self.config.cycle_interval_minutes
    
    def _execute_news_collection(self) -> Dict[str, List[NewsArticle]]:
        """Execute news collection step"""
        try:
            if not self.news_collection:
                self.logger.error("News collection module not available")
                return {}
            
            self.logger.info("Executing news collection step...")
            news_data = self.news_collection.collect_news_for_symbols(self.watchlist_symbols)
            
            # Update metrics
            self.pipeline_metrics.collection_metrics = self.news_collection.get_metrics()
            
            total_articles = sum(len(articles) for articles in news_data.values())
            self.logger.info(f"News collection completed: {total_articles} articles for {len(news_data)} symbols")
            
            return news_data
            
        except Exception as e:
            self.logger.error(f"News collection step failed: {e}")
            return {}
    
    def _execute_sentiment_analysis(self, news_data: Dict[str, List[NewsArticle]]) -> Dict[str, List[EnhancedSentimentResult]]:
        """Execute sentiment analysis step"""
        try:
            if not self.finbert_analysis:
                self.logger.error("FinBERT analysis module not available")
                return {}
            
            self.logger.info("DEBUG: About to execute sentiment analysis step...")
            self.logger.info(f"DEBUG: FinBERT module available: {self.finbert_analysis is not None}")
            self.logger.info("Executing sentiment analysis step...")
            sentiment_data = self.finbert_analysis.analyze_news_sentiment(news_data)
            
            # Update metrics
            self.pipeline_metrics.analysis_metrics = self.finbert_analysis.get_metrics()
            
            total_results = sum(len(results) for results in sentiment_data.values())
            self.logger.info(f"Sentiment analysis completed: {total_results} results for {len(sentiment_data)} symbols")
            
            return sentiment_data
            
        except Exception as e:
            self.logger.error(f"Sentiment analysis step failed: {e}")
            return {}
    
    def _execute_recommendation_generation(self, sentiment_data: Dict[str, List[EnhancedSentimentResult]]) -> Dict[str, List[TradingRecommendation]]:
        """Execute recommendation generation step"""
        try:
            if not self.xgboost_recommendation:
                self.logger.warning("XGBoost recommendation module not available, skipping recommendations")
                return {}
            
            self.logger.info("Executing recommendation generation step...")
            recommendations = self.xgboost_recommendation.generate_recommendations(sentiment_data)
            
            # Update metrics
            self.pipeline_metrics.recommendation_metrics = self.xgboost_recommendation.get_metrics()
            
            total_recommendations = sum(len(recs) for recs in recommendations.values())
            self.logger.info(f"Recommendation generation completed: {total_recommendations} recommendations for {len(recommendations)} symbols")
            
            return recommendations
            
        except Exception as e:
            self.logger.error(f"Recommendation generation step failed: {e}")
            return {}
    
    def _process_pipeline_results(self, news_data: Dict[str, List[NewsArticle]], 
                                sentiment_data: Dict[str, List[EnhancedSentimentResult]], 
                                recommendations: Dict[str, List[TradingRecommendation]]):
        """Process and send pipeline results"""
        try:
            self.logger.info("Processing pipeline results...")
            
            # Send high-confidence recommendations to decision engine
            self._send_recommendations_to_decision_engine(recommendations)
            
            # Update symbol tracking
            self._update_symbol_tracking(news_data, sentiment_data, recommendations)
            
            # Update shared state
            self._update_shared_state(news_data, sentiment_data, recommendations)
            
            self.logger.info("Pipeline results processing completed")
            
        except Exception as e:
            self.logger.error(f"Error processing pipeline results: {e}")
    
    def _send_recommendations_to_decision_engine(self, recommendations: Dict[str, List[TradingRecommendation]]):
        """Send high-confidence recommendations to decision engine"""
        try:
            if not self.redis_manager:
                self.logger.warning("Redis manager not available, cannot send recommendations")
                return
            
            recommendations_sent = 0
            
            for symbol, symbol_recommendations in recommendations.items():
                for recommendation in symbol_recommendations:
                    # Only send high-confidence recommendations
                    if recommendation.confidence >= 0.75:
                        message = create_process_message(
                            sender=self.process_id,
                            recipient="market_decision_engine",
                            message_type="TRADING_RECOMMENDATION",
                            data={
                                'symbol': symbol,
                                'action': recommendation.action,
                                'confidence': recommendation.confidence,
                                'prediction_score': recommendation.prediction_score,
                                'reasoning': recommendation.reasoning,
                                'risk_score': recommendation.risk_score,
                                'expected_return': recommendation.expected_return,
                                'time_horizon': recommendation.time_horizon,
                                'source': 'enhanced_news_pipeline',
                                'timestamp': datetime.now().isoformat()
                            },
                            priority=MessagePriority.HIGH if recommendation.confidence > 0.85 else MessagePriority.NORMAL
                        )
                        
                        if self.redis_manager.send_message(message):
                            recommendations_sent += 1
                            self.logger.info(f"Sent {recommendation.action} recommendation for {symbol} (confidence: {recommendation.confidence:.2f})")
            
            if recommendations_sent > 0:
                self.logger.info(f"Sent {recommendations_sent} high-confidence recommendations to decision engine")
            
        except Exception as e:
            self.logger.error(f"Error sending recommendations to decision engine: {e}")
    
    def _update_symbol_tracking(self, news_data: Dict[str, List[NewsArticle]], 
                              sentiment_data: Dict[str, List[EnhancedSentimentResult]], 
                              recommendations: Dict[str, List[TradingRecommendation]]):
        """Update symbol tracking information"""
        try:
            for symbol in self.watchlist_symbols:
                tracking = self.symbol_tracking[symbol]
                tracking['last_update'] = datetime.now()
                
                # Update recommendation stats
                symbol_recs = recommendations.get(symbol, [])
                tracking['recommendation_count'] = len(symbol_recs)
                
                if symbol_recs:
                    tracking['avg_confidence'] = sum(r.confidence for r in symbol_recs) / len(symbol_recs)
                    tracking['last_recommendation'] = symbol_recs[0].action
                else:
                    tracking['avg_confidence'] = 0.0
                    tracking['last_recommendation'] = None
                
        except Exception as e:
            self.logger.error(f"Error updating symbol tracking: {e}")
    
    def _update_shared_state(self, news_data: Dict[str, List[NewsArticle]], 
                           sentiment_data: Dict[str, List[EnhancedSentimentResult]], 
                           recommendations: Dict[str, List[TradingRecommendation]]):
        """Update Redis shared state with pipeline results"""
        try:
            if not self.redis_manager:
                return
            
            # Store latest pipeline results
            pipeline_state = {
                'last_run_time': self.last_run_time.isoformat() if self.last_run_time else None,
                'symbols_processed': len(self.watchlist_symbols),
                'articles_collected': sum(len(articles) for articles in news_data.values()),
                'sentiment_results': sum(len(results) for results in sentiment_data.values()),
                'recommendations_generated': sum(len(recs) for recs in recommendations.values()),
                'symbol_tracking': self.symbol_tracking,
                'metrics': {
                    'cycles_completed': self.pipeline_metrics.cycles_completed,
                    'average_cycle_time_ms': self.pipeline_metrics.average_cycle_time_ms,
                    'error_count': self.pipeline_metrics.error_count
                }
            }
            
            self.redis_manager.update_shared_state(
                {f'{self.process_id}_state': pipeline_state},
                namespace='pipeline_status'
            )
            
        except Exception as e:
            self.logger.warning(f"Failed to update shared state: {e}")
    
    def _update_pipeline_metrics(self, cycle_time_ms: float):
        """Update pipeline performance metrics"""
        try:
            self.pipeline_metrics.cycles_completed += 1
            self.pipeline_metrics.total_processing_time_ms += cycle_time_ms
            self.pipeline_metrics.average_cycle_time_ms = (
                self.pipeline_metrics.total_processing_time_ms / self.pipeline_metrics.cycles_completed
            )
            
            # Update resource metrics
            try:
                process = psutil.Process()
                self.pipeline_metrics.memory_usage_mb = process.memory_info().rss / 1024 / 1024
                self.pipeline_metrics.cpu_usage_percent = process.cpu_percent()
            except Exception:
                pass  # Resource metrics are optional
            
            self.pipeline_metrics.timestamp = datetime.now()
            
            # Add to processing history
            self.processing_history.append({
                'cycle': self.pipeline_metrics.cycles_completed,
                'time_ms': cycle_time_ms,
                'timestamp': datetime.now(),
                'memory_mb': self.pipeline_metrics.memory_usage_mb
            })
            
        except Exception as e:
            self.logger.error(f"Error updating pipeline metrics: {e}")
    
    def _handle_idle_cycle(self):
        """Handle idle cycles when no news is available"""
        self.pipeline_metrics.idle_cycles += 1
        self.pipeline_metrics.last_idle_time = datetime.now()
        
        # Still update last run time to prevent immediate re-run
        self.last_run_time = datetime.now()
        
        self.logger.debug(f"Idle cycle #{self.pipeline_metrics.idle_cycles} - no news available")
    
    def get_process_info(self) -> Dict[str, Any]:
        """Get comprehensive process information"""
        try:
            return {
                'process_type': 'enhanced_news_pipeline',
                'state': self.state.value,
                'watchlist_symbols': self.watchlist_symbols,
                'last_run_time': self.last_run_time.isoformat() if self.last_run_time else None,
                'components_initialized': self.components_initialized,
                'metrics': {
                    'cycles_completed': self.pipeline_metrics.cycles_completed,
                    'average_cycle_time_ms': self.pipeline_metrics.average_cycle_time_ms,
                    'idle_cycles': self.pipeline_metrics.idle_cycles,
                    'error_count': self.pipeline_metrics.error_count,
                    'memory_usage_mb': self.pipeline_metrics.memory_usage_mb,
                    'cpu_usage_percent': self.pipeline_metrics.cpu_usage_percent
                },
                'symbol_tracking': self.symbol_tracking,
                'configuration': {
                    'cycle_interval_minutes': self.config.cycle_interval_minutes,
                    'max_memory_mb': self.config.max_memory_mb,
                    'enable_caching': self.config.enable_caching
                }
            }
        except Exception as e:
            self.logger.error(f"Error getting process info: {e}")
            return {'error': str(e)}
    
    def force_pipeline_run(self) -> Dict[str, Any]:
        """Force an immediate pipeline run (for testing/debugging)"""
        try:
            self.logger.info("Forcing immediate pipeline run")
            
            # Temporarily reset last run time to force execution
            original_last_run = self.last_run_time
            self.last_run_time = None
            
            # Execute pipeline cycle
            self._process_cycle()
            
            # Restore original time if execution failed
            if self.last_run_time is None:
                self.last_run_time = original_last_run
                return {'success': False, 'error': 'Pipeline execution failed'}
            
            return {
                'success': True,
                'execution_time': self.last_run_time.isoformat(),
                'metrics': self.pipeline_metrics.__dict__
            }
            
        except Exception as e:
            self.logger.error(f"Error in forced pipeline run: {e}")
            return {'success': False, 'error': str(e)}
    
    def _cleanup(self):
        """Cleanup pipeline resources"""
        try:
            self.logger.info("Cleaning up Enhanced News Pipeline Orchestrator...")
            
            # Cleanup modules
            if self.news_collection:
                self.news_collection.cleanup()
            
            if self.finbert_analysis:
                self.finbert_analysis.cleanup()
            
            if self.xgboost_recommendation:
                self.xgboost_recommendation.cleanup()
            
            # Clear tracking data
            self.symbol_tracking.clear()
            self.processing_history.clear()
            self.error_history.clear()
            
            self.logger.info("Enhanced News Pipeline Orchestrator cleanup completed")
            
        except Exception as e:
            self.logger.error(f"Error during pipeline cleanup: {e}")


# Alias for backward compatibility
EnhancedNewsPipelineProcess = EnhancedNewsPipelineOrchestrator