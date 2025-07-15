"""
FinBERT Processor - Core FinBERT sentiment analysis component

Extracted from enhanced_news_pipeline_process.py to provide modular
FinBERT sentiment analysis capabilities with lazy loading and memory optimization.

Key Features:
- Lazy loading of FinBERT analyzer to save memory
- Batch processing for memory efficiency
- Individual article sentiment analysis
- Enhanced sentiment results with market impact
- OutputCoordinator integration for standardized output
- Comprehensive error handling
"""

import logging
import time
from datetime import datetime
from typing import List, Optional, Sequence, Dict, Any
from dataclasses import dataclass
from enum import Enum

# Import FinBERT components
from Friren_V1.trading_engine.sentiment.finBERT_analysis import SentimentResult, EnhancedFinBERT as FinBERTAnalyzer
from Friren_V1.trading_engine.data.news.base import NewsArticle
from Friren_V1.trading_engine.output.output_coordinator import OutputCoordinator


class SentimentLabel(Enum):
    """FinBERT sentiment labels"""
    POSITIVE = "POSITIVE"
    NEGATIVE = "NEGATIVE"
    NEUTRAL = "NEUTRAL"


@dataclass
class EnhancedSentimentResult:
    """Enhanced sentiment result with additional metrics"""
    article_id: str
    title: str
    sentiment_label: SentimentLabel
    confidence: float
    positive_score: float
    negative_score: float
    neutral_score: float
    market_impact_score: float
    processing_time_ms: float
    finbert_version: str = "1.0"


class FinBERTProcessor:
    """
    Core FinBERT sentiment analysis processor
    
    Handles lazy loading, batch processing, and individual article analysis
    with memory optimization and comprehensive error handling.
    """
    
    def __init__(self, 
                 batch_size: int = 4,
                 max_length: int = 256,
                 min_confidence_threshold: float = 0.6,
                 output_coordinator: Optional[OutputCoordinator] = None):
        """
        Initialize FinBERT processor
        
        Args:
            batch_size: Number of articles to process in each batch
            max_length: Maximum token length for FinBERT analysis
            min_confidence_threshold: Minimum confidence threshold for results
            output_coordinator: Optional output coordinator for standardized output
        """
        self.batch_size = batch_size
        self.max_length = max_length
        self.min_confidence_threshold = min_confidence_threshold
        self.output_coordinator = output_coordinator
        
        # Lazy loading - analyzer will be initialized when first needed
        self.finbert_analyzer: Optional[FinBERTAnalyzer] = None
        
        # Setup logging
        self.logger = logging.getLogger(__name__)
        
    def get_finbert_analyzer(self) -> FinBERTAnalyzer:
        """
        Lazy load FinBERT analyzer only when needed to save memory
        
        Returns:
            FinBERTAnalyzer instance
            
        Raises:
            RuntimeError: If FinBERT initialization fails
        """
        if self.finbert_analyzer is None:
            try:
                self.logger.info("Initializing FinBERT analyzer with lazy loading...")
                self.finbert_analyzer = FinBERTAnalyzer(
                    max_length=self.max_length,
                    batch_size=self.batch_size,
                    device="cpu"  # Force CPU to avoid GPU memory issues
                )
                
                # Initialize the model
                success = self.finbert_analyzer.initialize()
                if not success:
                    raise RuntimeError("FinBERT initialization returned False")
                    
                self.logger.info("SUCCESS: FinBERT analyzer lazy loaded and initialized successfully")
                
                # Test with a sample to ensure it works
                test_result = self.finbert_analyzer.analyze_text(
                    "Apple reported strong quarterly earnings with record revenue growth.",
                    article_id="init_test"
                )
                self.logger.info(f"FinBERT test analysis: {test_result.classification} (confidence: {test_result.confidence:.3f})")
                
            except ImportError as e:
                self.logger.error(f"CRITICAL: FinBERT dependencies missing: {e}")
                self.logger.error("SOLUTION: Install required packages: pip install torch transformers numpy")
                raise RuntimeError(f"FinBERT dependencies not installed: {e}")
            except Exception as e:
                self.logger.error(f"CRITICAL: FinBERT initialization failed: {e}")
                self.logger.error(f"Full error details:", exc_info=True)
                raise RuntimeError(f"FinBERT initialization failed: {e}")
                
        return self.finbert_analyzer
    
    def analyze_single_article(self, article: NewsArticle, article_id: Optional[str] = None) -> EnhancedSentimentResult:
        """
        Analyze sentiment for a single article
        
        Args:
            article: NewsArticle to analyze
            article_id: Optional custom article ID
            
        Returns:
            EnhancedSentimentResult with sentiment analysis
            
        Raises:
            RuntimeError: If sentiment analysis fails
        """
        try:
            # Get FinBERT analyzer (force lazy loading)
            finbert = self.get_finbert_analyzer()
            
            # Prepare text for analysis
            article_text = article.title
            if hasattr(article, 'content') and article.content:
                article_text += " " + article.content
            
            # Generate article ID if not provided
            if article_id is None:
                article_id = f"{article.source}_{hash(article.title) % 10000}"
            
            # Analyze sentiment
            analysis_start = datetime.now()
            sentiment_result = finbert.analyze_text(
                article_text,
                article_id=article_id
            )
            analysis_time = (datetime.now() - analysis_start).total_seconds() * 1000

            # Create enhanced sentiment result
            enhanced_result = EnhancedSentimentResult(
                article_id=article_id,
                title=article.title,
                sentiment_label=SentimentLabel(sentiment_result.classification.upper()),
                confidence=sentiment_result.confidence,
                positive_score=sentiment_result.raw_scores.get('positive', 0.0),
                negative_score=sentiment_result.raw_scores.get('negative', 0.0),
                neutral_score=sentiment_result.raw_scores.get('neutral', 0.0),
                market_impact_score=sentiment_result.sentiment_score,
                processing_time_ms=analysis_time
            )
            
            # Output through coordinator if available
            if self.output_coordinator:
                self._output_analysis_result(enhanced_result, article)
                
            return enhanced_result
            
        except Exception as e:
            self.logger.error(f"Error analyzing sentiment for article '{article.title[:50]}...': {e}")
            raise RuntimeError(f"Sentiment analysis failed for article: {e}")
    
    def analyze_articles_batch(self, articles: Sequence[NewsArticle], symbol_hint: Optional[str] = None) -> List[EnhancedSentimentResult]:
        """
        Analyze sentiment for multiple articles using batch processing
        
        Args:
            articles: Sequence of NewsArticle objects to analyze
            symbol_hint: Optional symbol for context in output
            
        Returns:
            List of EnhancedSentimentResult objects
            
        Raises:
            RuntimeError: If batch analysis fails completely
        """
        sentiment_results = []
        
        try:
            # Get FinBERT analyzer (force lazy loading)
            finbert = self.get_finbert_analyzer()
            
            # Process in batches to manage memory
            for i in range(0, len(articles), self.batch_size):
                batch = articles[i:i + self.batch_size]
                
                for idx, article in enumerate(batch):
                    try:
                        # Analyze single article
                        enhanced_result = self.analyze_single_article(
                            article, 
                            article_id=f"{symbol_hint}_{i}_{idx}" if symbol_hint else f"batch_{i}_{idx}"
                        )
                        sentiment_results.append(enhanced_result)
                        
                    except Exception as e:
                        self.logger.error(f"Error analyzing article in batch: {e}")
                        # Continue with other articles rather than failing completely
                        continue
                        
        except Exception as e:
            self.logger.error(f"CRITICAL: FinBERT batch analysis completely failed: {e}")
            raise RuntimeError(f"FinBERT batch analysis failed: {e}")
            
        return sentiment_results
    
    def analyze_articles_with_symbol(self, articles: Sequence[NewsArticle], symbol: str) -> List[EnhancedSentimentResult]:
        """
        Analyze sentiment for articles with symbol context for better output
        
        Args:
            articles: Sequence of NewsArticle objects to analyze
            symbol: Symbol for context in analysis and output
            
        Returns:
            List of EnhancedSentimentResult objects
        """
        self.logger.info(f"Analyzing sentiment for {len(articles)} articles for symbol {symbol}")
        
        sentiment_results = self.analyze_articles_batch(articles, symbol_hint=symbol)
        
        # Log summary of results
        if sentiment_results:
            self.logger.info(f"SENTIMENT ANALYSIS RESULTS for {symbol}:")
            for i, result in enumerate(sentiment_results):
                self.logger.info(f"  {i+1}. {result.title[:60]}... -> {result.sentiment_label.value} (Confidence: {result.confidence:.3f})")
        else:
            self.logger.info(f"No sentiment results generated for {symbol}")
            
        return sentiment_results
    
    def _output_analysis_result(self, result: EnhancedSentimentResult, article: NewsArticle):
        """
        Output analysis result through OutputCoordinator
        
        Args:
            result: EnhancedSentimentResult to output
            article: Original NewsArticle
        """
        if not self.output_coordinator:
            return
            
        try:
            # Extract symbol hint from article source or use default
            symbol_hint = article.source.split('-')[0] if '-' in article.source else 'UNKNOWN'
            sentiment = result.sentiment_label.value
            confidence = result.confidence * 100  # Convert to percentage
            article_snippet = result.title[:50] if result.title else "No title available"
            impact_score = result.market_impact_score
            
            self.output_coordinator.output_finbert_analysis(
                symbol_hint, sentiment, confidence, article_snippet, impact_score
            )
            
        except Exception as e:
            self.logger.error(f"Error outputting analysis result: {e}")
    
    def get_analyzer_status(self) -> Dict[str, Any]:
        """
        Get current status of the FinBERT analyzer
        
        Returns:
            Dictionary containing analyzer status information
        """
        return {
            'analyzer_loaded': self.finbert_analyzer is not None,
            'batch_size': self.batch_size,
            'max_length': self.max_length,
            'min_confidence_threshold': self.min_confidence_threshold,
            'output_coordinator_available': self.output_coordinator is not None
        }
    
    def verify_dependencies(self) -> bool:
        """
        Verify that all FinBERT dependencies are available
        
        Returns:
            True if all dependencies are available, False otherwise
        """
        try:
            # Test import to verify dependencies
            from Friren_V1.trading_engine.sentiment.finBERT_analysis import EnhancedFinBERT
            import torch
            import transformers
            import numpy as np
            
            self.logger.info("SUCCESS: All FinBERT dependencies available")
            return True
            
        except ImportError as e:
            self.logger.error(f"CRITICAL: FinBERT dependencies missing: {e}")
            return False
    
    def cleanup(self):
        """
        Clean up resources and reset analyzer
        """
        try:
            if self.finbert_analyzer:
                # Reset analyzer to free memory
                self.finbert_analyzer = None
                self.logger.info("FinBERT analyzer cleaned up")
                
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")


def create_finbert_processor(batch_size: int = 4, 
                           max_length: int = 256,
                           min_confidence_threshold: float = 0.6,
                           output_coordinator: Optional[OutputCoordinator] = None) -> FinBERTProcessor:
    """
    Factory function to create FinBERT processor
    
    Args:
        batch_size: Number of articles to process in each batch
        max_length: Maximum token length for FinBERT analysis
        min_confidence_threshold: Minimum confidence threshold for results
        output_coordinator: Optional output coordinator for standardized output
        
    Returns:
        Configured FinBERTProcessor instance
    """
    return FinBERTProcessor(
        batch_size=batch_size,
        max_length=max_length,
        min_confidence_threshold=min_confidence_threshold,
        output_coordinator=output_coordinator
    )