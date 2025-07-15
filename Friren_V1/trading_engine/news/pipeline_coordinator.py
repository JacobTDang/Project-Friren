"""
Pipeline Coordinator for Friren Trading System
==============================================

Orchestrates all news pipeline components including news collection,
FinBERT sentiment analysis, and XGBoost recommendation generation.

This coordinator manages the complete pipeline flow:
1. News Collection (via collectors/)
2. Sentiment Analysis (via analysis/)  
3. Recommendation Generation (via recommendations/)

All components use real market data and OutputCoordinator for standardized output.
"""

import logging
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from collections import defaultdict

# Import modular components
from .collectors.news_collector import EnhancedNewsCollector
from .collectors.source_manager import SourceManager
from .collectors.article_deduplicator import ArticleDeduplicator

from .analysis.finbert_processor import FinBERTProcessor
from .analysis.sentiment_aggregator import SentimentAggregator
from .analysis.impact_calculator import ImpactCalculator

from .recommendations.xgboost_engine import XGBoostRecommendationEngine
from .recommendations.feature_engineer import FeatureEngineer
from .recommendations.recommendation_validator import RecommendationValidator

# Import output coordination
from ..output.output_coordinator import OutputCoordinator

# Import base data structures
from ..data.news.base import NewsArticle


@dataclass
class PipelineResult:
    """Complete pipeline result for a symbol"""
    symbol: str
    articles_collected: int
    sentiment_results: List[Any]
    recommendations: List[Dict[str, Any]]
    processing_time_ms: float
    success: bool
    error_message: Optional[str] = None


@dataclass
class PipelineMetrics:
    """Pipeline performance metrics"""
    total_symbols_processed: int = 0
    total_articles_collected: int = 0
    total_recommendations_generated: int = 0
    average_processing_time_ms: float = 0.0
    success_rate: float = 0.0
    error_count: int = 0


class PipelineCoordinator:
    """Coordinates all news pipeline components"""
    
    def __init__(self, 
                 output_coordinator: Optional[OutputCoordinator] = None,
                 config: Optional[Dict[str, Any]] = None):
        """
        Initialize pipeline coordinator
        
        Args:
            output_coordinator: OutputCoordinator for standardized output
            config: Configuration dictionary
        """
        self.logger = logging.getLogger(__name__)
        self.output_coordinator = output_coordinator
        self.config = config or {}
        
        # Initialize components
        self.news_collector: Optional[EnhancedNewsCollector] = None
        self.source_manager: Optional[SourceManager] = None
        self.article_deduplicator: Optional[ArticleDeduplicator] = None
        
        self.finbert_processor: Optional[FinBERTProcessor] = None
        self.sentiment_aggregator: Optional[SentimentAggregator] = None
        self.impact_calculator: Optional[ImpactCalculator] = None
        
        self.xgboost_engine: Optional[XGBoostRecommendationEngine] = None
        self.feature_engineer: Optional[FeatureEngineer] = None
        self.recommendation_validator: Optional[RecommendationValidator] = None
        
        # Performance tracking
        self.metrics = PipelineMetrics()
        self._last_results: Dict[str, PipelineResult] = {}
    
    def initialize_components(self) -> bool:
        """
        Initialize all pipeline components
        
        Returns:
            True if all components initialized successfully
        """
        try:
            self.logger.info("Initializing news pipeline coordinator components...")
            
            # Initialize news collection components
            self.logger.info("Initializing news collection components...")
            self.news_collector = EnhancedNewsCollector()
            self.source_manager = SourceManager()
            self.article_deduplicator = ArticleDeduplicator()
            
            # Initialize analysis components
            self.logger.info("Initializing analysis components...")
            self.finbert_processor = FinBERTProcessor(output_coordinator=self.output_coordinator)
            self.sentiment_aggregator = SentimentAggregator()
            self.impact_calculator = ImpactCalculator()
            
            # Initialize recommendation components
            self.logger.info("Initializing recommendation components...")
            self.xgboost_engine = XGBoostRecommendationEngine(output_coordinator=self.output_coordinator)
            self.feature_engineer = FeatureEngineer()
            self.recommendation_validator = RecommendationValidator()
            
            self.logger.info("All pipeline coordinator components initialized successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to initialize pipeline coordinator components: {e}")
            return False
    
    async def process_symbol(self, symbol: str) -> PipelineResult:
        """
        Process complete news pipeline for a single symbol
        
        Args:
            symbol: Stock symbol to process
            
        Returns:
            PipelineResult with all processing results
        """
        start_time = datetime.now()
        
        try:
            # Stage 1: News Collection
            self.logger.info(f"Starting news collection for {symbol}")
            articles = await self._collect_news(symbol)
            
            if not articles:
                return PipelineResult(
                    symbol=symbol,
                    articles_collected=0,
                    sentiment_results=[],
                    recommendations=[],
                    processing_time_ms=0.0,
                    success=False,
                    error_message="No articles collected"
                )
            
            # Stage 2: Sentiment Analysis
            self.logger.info(f"Starting sentiment analysis for {symbol} ({len(articles)} articles)")
            sentiment_results = await self._analyze_sentiment(symbol, articles)
            
            # Stage 3: Recommendation Generation
            self.logger.info(f"Starting recommendation generation for {symbol}")
            recommendations = await self._generate_recommendations(symbol, articles, sentiment_results)
            
            # Calculate processing time
            processing_time = (datetime.now() - start_time).total_seconds() * 1000
            
            # Create result
            result = PipelineResult(
                symbol=symbol,
                articles_collected=len(articles),
                sentiment_results=sentiment_results,
                recommendations=recommendations,
                processing_time_ms=processing_time,
                success=True
            )
            
            # Update metrics
            self._update_metrics(result)
            
            # Store result
            self._last_results[symbol] = result
            
            self.logger.info(f"Pipeline processing complete for {symbol}: {len(articles)} articles, "
                           f"{len(sentiment_results)} sentiment results, {len(recommendations)} recommendations "
                           f"in {processing_time:.1f}ms")
            
            return result
            
        except Exception as e:
            processing_time = (datetime.now() - start_time).total_seconds() * 1000
            self.logger.error(f"Pipeline processing failed for {symbol}: {e}")
            
            return PipelineResult(
                symbol=symbol,
                articles_collected=0,
                sentiment_results=[],
                recommendations=[],
                processing_time_ms=processing_time,
                success=False,
                error_message=str(e)
            )
    
    async def process_watchlist(self, symbols: List[str]) -> Dict[str, PipelineResult]:
        """
        Process complete news pipeline for multiple symbols
        
        Args:
            symbols: List of stock symbols to process
            
        Returns:
            Dictionary mapping symbols to PipelineResult
        """
        results = {}
        
        self.logger.info(f"Processing news pipeline for {len(symbols)} symbols: {symbols}")
        
        # Process symbols concurrently but with controlled concurrency
        max_concurrent = self.config.get('max_concurrent_symbols', 3)
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def process_with_semaphore(symbol: str) -> Tuple[str, PipelineResult]:
            async with semaphore:
                result = await self.process_symbol(symbol)
                return symbol, result
        
        # Execute processing
        tasks = [process_with_semaphore(symbol) for symbol in symbols]
        completed_results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Collect results
        for result in completed_results:
            if isinstance(result, Exception):
                self.logger.error(f"Symbol processing failed: {result}")
                continue
            
            symbol, pipeline_result = result
            results[symbol] = pipeline_result
        
        self.logger.info(f"Watchlist processing complete: {len(results)} symbols processed")
        
        return results
    
    async def _collect_news(self, symbol: str) -> List[NewsArticle]:
        """Collect news articles for symbol"""
        if not self.news_collector:
            raise RuntimeError("News collector not initialized")
        
        # Collect articles
        articles = await self.news_collector.collect_symbol_news(symbol)
        
        # Deduplicate articles
        if self.article_deduplicator and articles:
            articles = self.article_deduplicator.deduplicate_articles(articles)
        
        return articles
    
    async def _analyze_sentiment(self, symbol: str, articles: List[NewsArticle]) -> List[Any]:
        """Analyze sentiment for articles"""
        if not self.finbert_processor:
            raise RuntimeError("FinBERT processor not initialized")
        
        # Process articles through FinBERT
        sentiment_results = await self.finbert_processor.analyze_articles_batch(symbol, articles)
        
        # Aggregate sentiment if aggregator available
        if self.sentiment_aggregator and sentiment_results:
            aggregated = self.sentiment_aggregator.aggregate_sentiment(sentiment_results)
            # Output aggregated sentiment if OutputCoordinator available
            if self.output_coordinator and aggregated:
                self.output_coordinator.output_finbert_analysis(
                    symbol=symbol,
                    sentiment=aggregated.get('overall_sentiment', 'NEUTRAL'),
                    confidence=aggregated.get('confidence', 0.0),
                    article_snippet=f"Aggregated from {len(sentiment_results)} articles",
                    impact=aggregated.get('impact_score', 0.0)
                )
        
        return sentiment_results
    
    async def _generate_recommendations(self, symbol: str, articles: List[NewsArticle], 
                                      sentiment_results: List[Any]) -> List[Dict[str, Any]]:
        """Generate trading recommendations"""
        if not self.xgboost_engine or not self.feature_engineer:
            raise RuntimeError("Recommendation components not initialized")
        
        # Engineer features from news and sentiment
        features = self.feature_engineer.engineer_features(symbol, articles, sentiment_results)
        
        # Generate recommendations
        recommendations = await self.xgboost_engine.generate_recommendations(symbol, features)
        
        # Validate recommendations
        if self.recommendation_validator and recommendations:
            validated_recommendations = []
            for rec in recommendations:
                validated = self.recommendation_validator.validate_recommendation(symbol, rec, features)
                validated_recommendations.append(validated)
            recommendations = validated_recommendations
        
        return recommendations
    
    def _update_metrics(self, result: PipelineResult) -> None:
        """Update pipeline performance metrics"""
        self.metrics.total_symbols_processed += 1
        self.metrics.total_articles_collected += result.articles_collected
        self.metrics.total_recommendations_generated += len(result.recommendations)
        
        # Update average processing time
        total_time = (self.metrics.average_processing_time_ms * (self.metrics.total_symbols_processed - 1) + 
                     result.processing_time_ms)
        self.metrics.average_processing_time_ms = total_time / self.metrics.total_symbols_processed
        
        # Update success rate
        if not result.success:
            self.metrics.error_count += 1
        
        self.metrics.success_rate = (
            (self.metrics.total_symbols_processed - self.metrics.error_count) / 
            self.metrics.total_symbols_processed * 100
        )
    
    def get_metrics(self) -> PipelineMetrics:
        """Get current pipeline metrics"""
        return self.metrics
    
    def get_last_results(self) -> Dict[str, PipelineResult]:
        """Get last processing results for all symbols"""
        return self._last_results.copy()
    
    def reset_metrics(self) -> None:
        """Reset pipeline metrics"""
        self.metrics = PipelineMetrics()
        self._last_results.clear()