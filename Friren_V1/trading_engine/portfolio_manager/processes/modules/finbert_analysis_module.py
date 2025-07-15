"""
FinBERT Analysis Module for Enhanced News Pipeline
=================================================

Handles FinBERT sentiment analysis, impact calculation, and sentiment aggregation
for the enhanced news pipeline process. This module provides comprehensive
sentiment analysis with market impact assessment.

Extracted from enhanced_news_pipeline_process.py for better maintainability.
"""

import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from enum import Enum

# Import FinBERT components
from Friren_V1.trading_engine.sentiment.finBERT_analysis import SentimentResult, EnhancedFinBERT as FinBERTAnalyzer
from Friren_V1.trading_engine.data.news.analysis import (
    FinBERTProcessor,
    SentimentAggregator,
    ImpactCalculator,
    EnhancedSentimentResult,
    AggregatedSentimentResult,
    MarketImpactResult,
    SentimentLabel,
    RiskLevel,
    create_finbert_processor,
    create_sentiment_aggregator,
    create_impact_calculator
)
from Friren_V1.trading_engine.data.news.base import NewsArticle
from Friren_V1.trading_engine.output.output_coordinator import OutputCoordinator


@dataclass
class FinBERTConfig:
    """Configuration for FinBERT analysis"""
    batch_size: int = 4
    min_confidence_threshold: float = 0.6
    enable_impact_calculation: bool = True
    enable_aggregation: bool = True
    max_articles_per_batch: int = 10
    timeout_seconds: int = 30


@dataclass
class AnalysisMetrics:
    """FinBERT analysis metrics"""
    articles_analyzed: int = 0
    symbols_processed: int = 0
    analysis_time_ms: float = 0.0
    average_confidence: float = 0.0
    sentiment_distribution: Dict[str, int] = field(default_factory=dict)
    errors: int = 0
    timeout_count: int = 0
    timestamp: datetime = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()
        if not self.sentiment_distribution:
            self.sentiment_distribution = {"positive": 0, "negative": 0, "neutral": 0}


class FinBERTAnalysisModule:
    """
    FinBERT Analysis Module
    
    Handles comprehensive sentiment analysis using FinBERT, including
    sentiment classification, confidence scoring, impact calculation,
    and sentiment aggregation for trading decisions.
    """
    
    def __init__(self, config: Optional[FinBERTConfig] = None,
                 output_coordinator: Optional[OutputCoordinator] = None,
                 process_id: str = "finbert_analysis"):
        """
        Initialize FinBERT analysis module
        
        Args:
            config: FinBERT analysis configuration
            output_coordinator: Output coordinator for standardized output
            process_id: Process identifier for output
        """
        self.config = config or FinBERTConfig()
        self.output_coordinator = output_coordinator
        self.process_id = process_id
        self.logger = logging.getLogger(__name__)
        
        # Core components
        self.finbert_analyzer: Optional[FinBERTAnalyzer] = None
        self.finbert_processor: Optional[FinBERTProcessor] = None
        self.sentiment_aggregator: Optional[SentimentAggregator] = None
        self.impact_calculator: Optional[ImpactCalculator] = None
        
        # State tracking
        self.last_analysis_time: Optional[datetime] = None
        self.analysis_cache: Dict[str, List[EnhancedSentimentResult]] = {}
        self.metrics = AnalysisMetrics()
        
        # Performance tracking
        self.processing_history = []
        self.error_count = 0
        self.last_error_time: Optional[datetime] = None
    
    def initialize(self) -> bool:
        """
        Initialize FinBERT analysis components
        
        Returns:
            True if initialization successful
        """
        try:
            self.logger.info("Initializing FinBERT analysis module...")
            
            # Initialize main FinBERT analyzer
            self.finbert_analyzer = FinBERTAnalyzer()
            if hasattr(self.finbert_analyzer, 'initialize'):
                self.finbert_analyzer.initialize()
            
            # Initialize modular analysis components
            self.finbert_processor = create_finbert_processor()
            self.sentiment_aggregator = create_sentiment_aggregator()
            self.impact_calculator = create_impact_calculator()
            
            self.logger.info("FinBERT analysis module initialized successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to initialize FinBERT analysis module: {e}")
            self.error_count += 1
            self.last_error_time = datetime.now()
            return False
    
    def analyze_news_sentiment(self, news_data: Dict[str, List[NewsArticle]]) -> Dict[str, List[EnhancedSentimentResult]]:
        """
        Analyze sentiment for collected news articles
        
        Args:
            news_data: Dictionary mapping symbols to news articles
            
        Returns:
            Dictionary mapping symbols to sentiment analysis results
        """
        start_time = time.time()
        sentiment_results = {}
        
        try:
            self.logger.info(f"DEBUG: FinBERT analyze_news_sentiment called with {len(news_data)} symbols")
            self.logger.info(f"Starting FinBERT analysis for {len(news_data)} symbols")
            
            # Process each symbol's news
            for symbol, articles in news_data.items():
                try:
                    if not articles:
                        sentiment_results[symbol] = []
                        continue
                    
                    # Analyze articles for this symbol
                    symbol_results = self._analyze_symbol_articles(symbol, articles)
                    sentiment_results[symbol] = symbol_results
                    
                    # Output analysis results using OutputCoordinator
                    if symbol_results and self.output_coordinator:
                        for result in symbol_results:
                            self._output_analysis_result(symbol, result)
                    
                except Exception as e:
                    self.logger.error(f"Error analyzing sentiment for {symbol}: {e}")
                    self.metrics.errors += 1
                    sentiment_results[symbol] = []
            
            # Update metrics
            analysis_time = (time.time() - start_time) * 1000
            self.metrics.analysis_time_ms = analysis_time
            self.metrics.symbols_processed = len(news_data)
            self.metrics.articles_analyzed = sum(len(results) for results in sentiment_results.values())
            self.last_analysis_time = datetime.now()
            
            # Calculate average confidence
            all_results = [result for results in sentiment_results.values() for result in results]
            if all_results:
                self.metrics.average_confidence = sum(r.confidence for r in all_results) / len(all_results)
                
                # Update sentiment distribution
                for result in all_results:
                    sentiment_str = result.sentiment_label.value.lower()
                    self.metrics.sentiment_distribution[sentiment_str] = self.metrics.sentiment_distribution.get(sentiment_str, 0) + 1
            
            self.logger.info(f"FinBERT analysis completed: {self.metrics.articles_analyzed} articles in {analysis_time:.1f}ms")
            
            return sentiment_results
            
        except Exception as e:
            self.logger.error(f"FinBERT analysis failed: {e}")
            self.error_count += 1
            self.last_error_time = datetime.now()
            return {}
    
    def _analyze_symbol_articles(self, symbol: str, articles: List[NewsArticle]) -> List[EnhancedSentimentResult]:
        """
        Analyze sentiment for articles of a specific symbol
        
        Args:
            symbol: Stock symbol
            articles: List of news articles
            
        Returns:
            List of sentiment analysis results
        """
        try:
            if not self.finbert_analyzer:
                self.logger.error("FinBERT analyzer not initialized")
                return []
            
            results = []
            
            # Process articles in batches
            for i in range(0, len(articles), self.config.batch_size):
                batch = articles[i:i + self.config.batch_size]
                batch_results = self._process_article_batch(symbol, batch)
                results.extend(batch_results)
            
            # Filter by confidence threshold
            filtered_results = [
                result for result in results
                if result.confidence >= self.config.min_confidence_threshold
            ]
            
            self.logger.debug(f"Analyzed {len(articles)} articles for {symbol}, {len(filtered_results)} passed confidence threshold")
            
            return filtered_results
            
        except Exception as e:
            self.logger.error(f"Error analyzing articles for {symbol}: {e}")
            return []
    
    def _process_article_batch(self, symbol: str, articles: List[NewsArticle]) -> List[EnhancedSentimentResult]:
        """
        Process a batch of articles for sentiment analysis
        
        Args:
            symbol: Stock symbol
            articles: Batch of articles to process
            
        Returns:
            List of enhanced sentiment results
        """
        try:
            results = []
            
            for article in articles:
                try:
                    # Prepare text for analysis (title + content)
                    text = self._prepare_article_text(article)
                    if not text:
                        continue
                    
                    # Run FinBERT analysis with proper article_id for symbol extraction
                    article_id = f"{symbol}_{hash(text) % 10000}"
                    sentiment_result = self.finbert_analyzer.analyze_text(text, article_id)
                    
                    if sentiment_result:
                        # Calculate impact score
                        impact_score = self._calculate_market_impact(article, sentiment_result)
                        
                        # Create enhanced result
                        enhanced_result = EnhancedSentimentResult(
                            symbol=symbol,
                            sentiment_label=sentiment_result.sentiment_label,
                            confidence=sentiment_result.confidence,
                            raw_scores=sentiment_result.raw_scores,
                            text_snippet=text[:100] + "..." if len(text) > 100 else text,
                            article_title=getattr(article, 'title', ''),
                            article_source=getattr(article, 'source', ''),
                            published_date=getattr(article, 'published_date', datetime.now()),
                            impact_score=impact_score,
                            risk_level=self._determine_risk_level(sentiment_result, impact_score),
                            processing_time=time.time(),
                            metadata={
                                'article_length': len(text),
                                'title_length': len(getattr(article, 'title', '')),
                                'source_reliability': self._assess_source_reliability(getattr(article, 'source', ''))
                            }
                        )
                        
                        results.append(enhanced_result)
                        
                except Exception as e:
                    self.logger.debug(f"Error processing individual article: {e}")
                    continue
            
            return results
            
        except Exception as e:
            self.logger.error(f"Error processing article batch: {e}")
            return []
    
    def _prepare_article_text(self, article: NewsArticle) -> str:
        """
        Prepare article text for FinBERT analysis
        
        Args:
            article: News article
            
        Returns:
            Prepared text string
        """
        try:
            text_parts = []
            
            # Add title if available
            if hasattr(article, 'title') and article.title:
                text_parts.append(article.title)
            
            # Add content if available
            if hasattr(article, 'content') and article.content:
                # Limit content length for FinBERT processing
                content = article.content
                if len(content) > 500:
                    content = content[:500] + "..."
                text_parts.append(content)
            
            # Join with space
            return " ".join(text_parts).strip()
            
        except Exception as e:
            self.logger.debug(f"Error preparing article text: {e}")
            return ""
    
    def _calculate_market_impact(self, article: NewsArticle, sentiment_result: SentimentResult) -> float:
        """
        Calculate market impact score for the article
        
        Args:
            article: News article
            sentiment_result: Sentiment analysis result
            
        Returns:
            Impact score between 0.0 and 1.0
        """
        try:
            if not self.impact_calculator:
                # Fallback calculation
                base_impact = sentiment_result.confidence
                
                # Adjust based on sentiment strength
                if sentiment_result.sentiment_label == SentimentLabel.POSITIVE:
                    return min(1.0, base_impact * 1.2)
                elif sentiment_result.sentiment_label == SentimentLabel.NEGATIVE:
                    return min(1.0, base_impact * 1.3)  # Negative news often has higher impact
                else:
                    return base_impact * 0.7
            
            # Use impact calculator if available
            impact_result = self.impact_calculator.calculate_impact(article, sentiment_result)
            return impact_result.impact_score if impact_result else 0.5
            
        except Exception as e:
            self.logger.debug(f"Error calculating market impact: {e}")
            return 0.5
    
    def _determine_risk_level(self, sentiment_result: SentimentResult, impact_score: float) -> RiskLevel:
        """
        Determine risk level based on sentiment and impact
        
        Args:
            sentiment_result: Sentiment analysis result
            impact_score: Market impact score
            
        Returns:
            Risk level classification
        """
        try:
            # High confidence negative sentiment with high impact = high risk
            if (sentiment_result.sentiment_label == SentimentLabel.NEGATIVE and
                sentiment_result.confidence > 0.8 and impact_score > 0.7):
                return RiskLevel.HIGH
            
            # Moderate risk conditions
            if (sentiment_result.confidence > 0.7 and impact_score > 0.6) or \
               (sentiment_result.sentiment_label == SentimentLabel.NEGATIVE and impact_score > 0.5):
                return RiskLevel.MEDIUM
            
            # Low risk for positive or neutral sentiment with low impact
            return RiskLevel.LOW
            
        except Exception:
            return RiskLevel.MEDIUM
    
    def _assess_source_reliability(self, source: str) -> float:
        """
        Assess the reliability of a news source
        
        Args:
            source: News source name
            
        Returns:
            Reliability score between 0.0 and 1.0
        """
        try:
            if not source:
                return 0.5
            
            source_lower = source.lower()
            
            # Tier 1 sources (highest reliability)
            tier1_sources = ['reuters', 'bloomberg', 'wall street journal', 'financial times']
            if any(s in source_lower for s in tier1_sources):
                return 0.9
            
            # Tier 2 sources (high reliability)
            tier2_sources = ['cnbc', 'marketwatch', 'yahoo finance', 'seeking alpha']
            if any(s in source_lower for s in tier2_sources):
                return 0.8
            
            # Tier 3 sources (moderate reliability)
            tier3_sources = ['investing.com', 'benzinga', 'fool', 'zacks']
            if any(s in source_lower for s in tier3_sources):
                return 0.7
            
            # Unknown sources
            return 0.6
            
        except Exception:
            return 0.5
    
    def _output_analysis_result(self, symbol: str, result: EnhancedSentimentResult):
        """
        Output analysis result using OutputCoordinator
        
        Args:
            symbol: Stock symbol
            result: Enhanced sentiment result
        """
        try:
            if self.output_coordinator:
                # Convert confidence to percentage
                confidence_pct = result.confidence * 100
                
                # Prepare article snippet (first 50 characters)
                article_snippet = result.text_snippet[:50] + "..." if len(result.text_snippet) > 50 else result.text_snippet
                
                # Output using standardized format
                self.output_coordinator.output_finbert_analysis(
                    symbol=symbol,
                    sentiment=result.sentiment_label.value,
                    confidence=confidence_pct,
                    article_snippet=article_snippet,
                    impact=result.impact_score
                )
            
        except Exception as e:
            self.logger.warning(f"Error outputting analysis result for {symbol}: {e}")
    
    def aggregate_symbol_sentiment(self, symbol: str, results: List[EnhancedSentimentResult]) -> Optional[AggregatedSentimentResult]:
        """
        Aggregate sentiment results for a symbol
        
        Args:
            symbol: Stock symbol
            results: List of sentiment results
            
        Returns:
            Aggregated sentiment result
        """
        try:
            if not results or not self.sentiment_aggregator:
                return None
            
            return self.sentiment_aggregator.aggregate_sentiment(symbol, results)
            
        except Exception as e:
            self.logger.error(f"Error aggregating sentiment for {symbol}: {e}")
            return None
    
    def get_metrics(self) -> AnalysisMetrics:
        """
        Get current analysis metrics
        
        Returns:
            Current metrics snapshot
        """
        return self.metrics
    
    def clear_cache(self):
        """Clear the analysis cache"""
        self.analysis_cache.clear()
        self.logger.info("FinBERT analysis cache cleared")
    
    def cleanup(self):
        """Cleanup module resources"""
        try:
            self.clear_cache()
            if self.finbert_analyzer and hasattr(self.finbert_analyzer, 'cleanup'):
                self.finbert_analyzer.cleanup()
            self.logger.info("FinBERT analysis module cleanup completed")
        except Exception as e:
            self.logger.error(f"Error during FinBERT analysis cleanup: {e}")