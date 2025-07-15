"""
Sentiment Aggregator - Multi-article sentiment processing and aggregation

Extracted from enhanced_news_pipeline_process.py to provide modular
sentiment aggregation capabilities for combining multiple article sentiments.

Key Features:
- Multi-article sentiment aggregation
- Confidence calculation from multiple sources
- Consensus-based sentiment scoring
- Weighted averaging of sentiment scores
- Statistical analysis of sentiment distributions
"""

import logging
import statistics
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass
from enum import Enum
from datetime import datetime

# Import configuration manager to eliminate ALL hardcoded values
from Friren_V1.infrastructure.configuration_manager import get_config

from .finbert_processor import EnhancedSentimentResult, SentimentLabel


@dataclass
class AggregatedSentimentResult:
    """Aggregated sentiment result from multiple articles"""
    symbol: str
    total_articles: int
    sentiment_distribution: Dict[str, int]  # Count of each sentiment type
    overall_sentiment: SentimentLabel
    confidence_score: float
    
    # Score breakdowns
    average_positive_score: float
    average_negative_score: float
    average_neutral_score: float
    weighted_market_impact: float
    
    # Statistical metrics
    sentiment_consistency: float  # How consistent are the sentiments
    confidence_variance: float    # Variance in confidence scores
    
    # Processing metadata
    processing_time_ms: float
    timestamp: datetime
    
    # Supporting data
    high_confidence_articles: int  # Articles with confidence > 0.75
    articles_processed: List[str]  # Article IDs processed


class SentimentAggregator:
    """
    Aggregates sentiment results from multiple articles
    
    Provides sophisticated sentiment aggregation with confidence calculation,
    consensus analysis, and weighted scoring for comprehensive sentiment assessment.
    """
    
    def __init__(self, 
                 min_confidence_threshold: float = None,
                 high_confidence_threshold: float = None):
        """
        Initialize sentiment aggregator
        
        Args:
            min_confidence_threshold: Minimum confidence for including results
            high_confidence_threshold: Threshold for high-confidence articles
        """
        # Load thresholds from configuration manager - ELIMINATE HARDCODED VALUES
        self.min_confidence_threshold = (
            min_confidence_threshold if min_confidence_threshold is not None 
            else get_config('SENTIMENT_MIN_CONFIDENCE_THRESHOLD', 0.6)
        )
        self.high_confidence_threshold = (
            high_confidence_threshold if high_confidence_threshold is not None 
            else get_config('SENTIMENT_HIGH_CONFIDENCE_THRESHOLD', 0.75)
        )
        
        # Setup logging
        self.logger = logging.getLogger(__name__)
    
    def aggregate_sentiments(self, sentiment_results: List[EnhancedSentimentResult], 
                           symbol: str) -> AggregatedSentimentResult:
        """
        Aggregate multiple sentiment results into a single comprehensive result
        
        Args:
            sentiment_results: List of EnhancedSentimentResult objects
            symbol: Symbol for context
            
        Returns:
            AggregatedSentimentResult with aggregated analysis
        """
        start_time = datetime.now()
        
        if not sentiment_results:
            return self._create_empty_result(symbol, start_time)
        
        # Filter by confidence threshold
        filtered_results = [
            result for result in sentiment_results 
            if result.confidence >= self.min_confidence_threshold
        ]
        
        if not filtered_results:
            self.logger.warning(f"No sentiment results meet confidence threshold {self.min_confidence_threshold} for {symbol}")
            return self._create_empty_result(symbol, start_time)
        
        # Calculate sentiment distribution
        sentiment_distribution = self._calculate_sentiment_distribution(filtered_results)
        
        # Determine overall sentiment
        overall_sentiment = self._determine_overall_sentiment(sentiment_distribution)
        
        # Calculate confidence score
        confidence_score = self._calculate_aggregated_confidence(filtered_results)
        
        # Calculate average scores
        avg_positive = statistics.mean([r.positive_score for r in filtered_results])
        avg_negative = statistics.mean([r.negative_score for r in filtered_results])
        avg_neutral = statistics.mean([r.neutral_score for r in filtered_results])
        
        # Calculate weighted market impact
        weighted_impact = self._calculate_weighted_market_impact(filtered_results)
        
        # Calculate statistical metrics
        sentiment_consistency = self._calculate_sentiment_consistency(filtered_results)
        confidence_variance = self._calculate_confidence_variance(filtered_results)
        
        # Count high-confidence articles
        high_confidence_count = sum(
            1 for result in filtered_results 
            if result.confidence >= self.high_confidence_threshold
        )
        
        processing_time = (datetime.now() - start_time).total_seconds() * 1000
        
        return AggregatedSentimentResult(
            symbol=symbol,
            total_articles=len(filtered_results),
            sentiment_distribution=sentiment_distribution,
            overall_sentiment=overall_sentiment,
            confidence_score=confidence_score,
            average_positive_score=avg_positive,
            average_negative_score=avg_negative,
            average_neutral_score=avg_neutral,
            weighted_market_impact=weighted_impact,
            sentiment_consistency=sentiment_consistency,
            confidence_variance=confidence_variance,
            processing_time_ms=processing_time,
            timestamp=datetime.now(),
            high_confidence_articles=high_confidence_count,
            articles_processed=[result.article_id for result in filtered_results]
        )
    
    def calculate_sentiment_confidence(self, positive_count: int, total_articles: int, 
                                     sentiment_results: Optional[List[EnhancedSentimentResult]] = None) -> float:
        """
        Calculate sentiment-based confidence using actual FinBERT scores
        
        Args:
            positive_count: Number of positive sentiment articles
            total_articles: Total number of articles analyzed
            sentiment_results: Optional list of sentiment results for detailed analysis
            
        Returns:
            Confidence score between 0.0 and 1.0
        """
        try:
            # If we have detailed results, use their confidence scores
            if sentiment_results:
                # Extract confidence scores from FinBERT analysis
                confidence_scores = [result.confidence for result in sentiment_results]
                
                if confidence_scores:
                    # Use weighted average of FinBERT confidence scores
                    avg_confidence = statistics.mean(confidence_scores)
                    # Boost confidence based on positive consensus
                    consensus_boost = min(1.0, positive_count / max(1, total_articles))
                    return avg_confidence * (0.7 + 0.3 * consensus_boost)
            
            # Fallback to count-based confidence
            if total_articles == 0:
                return 0.0
                
            positive_ratio = positive_count / total_articles
            
            # Note: Consensus ratios could be configurable but are less critical for now
            if positive_ratio >= 0.8:  # Strong positive consensus
                return 0.85
            elif positive_ratio >= 0.6:  # Good positive consensus
                return 0.70
            elif positive_ratio >= 0.4:  # Weak positive consensus
                return 0.55
            else:  # No positive consensus
                return 0.30
                
        except Exception as e:
            self.logger.error(f"Error calculating sentiment confidence: {e}")
            return 0.30  # Conservative fallback
    
    def analyze_sentiment_consensus(self, sentiment_results: List[EnhancedSentimentResult]) -> Dict[str, Any]:
        """
        Analyze consensus among sentiment results
        
        Args:
            sentiment_results: List of sentiment results to analyze
            
        Returns:
            Dictionary with consensus analysis
        """
        if not sentiment_results:
            return {
                'consensus_strength': 0.0,
                'dominant_sentiment': SentimentLabel.NEUTRAL,
                'agreement_percentage': 0.0,
                'conflicting_signals': 0
            }
        
        # Count sentiments
        sentiment_counts = {}
        for result in sentiment_results:
            sentiment = result.sentiment_label.value
            sentiment_counts[sentiment] = sentiment_counts.get(sentiment, 0) + 1
        
        # Find dominant sentiment
        dominant_sentiment = max(sentiment_counts, key=sentiment_counts.get)
        dominant_count = sentiment_counts[dominant_sentiment]
        
        # Calculate agreement percentage
        agreement_percentage = (dominant_count / len(sentiment_results)) * 100
        
        # Calculate consensus strength (0-1 scale)
        consensus_strength = self._calculate_consensus_strength(sentiment_counts, len(sentiment_results))
        
        # Count conflicting signals
        conflicting_signals = len(sentiment_counts) - 1 if len(sentiment_counts) > 1 else 0
        
        return {
            'consensus_strength': consensus_strength,
            'dominant_sentiment': SentimentLabel(dominant_sentiment),
            'agreement_percentage': agreement_percentage,
            'conflicting_signals': conflicting_signals,
            'sentiment_distribution': sentiment_counts,
            'total_articles': len(sentiment_results)
        }
    
    def _calculate_sentiment_distribution(self, results: List[EnhancedSentimentResult]) -> Dict[str, int]:
        """Calculate distribution of sentiment labels"""
        distribution = {}
        for result in results:
            sentiment = result.sentiment_label.value
            distribution[sentiment] = distribution.get(sentiment, 0) + 1
        return distribution
    
    def _determine_overall_sentiment(self, distribution: Dict[str, int]) -> SentimentLabel:
        """Determine overall sentiment from distribution"""
        if not distribution:
            return SentimentLabel.NEUTRAL
        
        # Find the most common sentiment
        dominant_sentiment = max(distribution, key=distribution.get)
        return SentimentLabel(dominant_sentiment)
    
    def _calculate_aggregated_confidence(self, results: List[EnhancedSentimentResult]) -> float:
        """Calculate aggregated confidence score"""
        if not results:
            return 0.0
        
        # Weight confidence by market impact
        weighted_confidences = []
        for result in results:
            # Higher market impact increases weight
            weight = 1.0 + (result.market_impact_score * 0.5)
            weighted_confidences.append(result.confidence * weight)
        
        return statistics.mean(weighted_confidences)
    
    def _calculate_weighted_market_impact(self, results: List[EnhancedSentimentResult]) -> float:
        """Calculate weighted market impact score"""
        if not results:
            return 0.0
        
        # Weight market impact by confidence
        weighted_impacts = []
        for result in results:
            weight = result.confidence
            weighted_impacts.append(result.market_impact_score * weight)
        
        return statistics.mean(weighted_impacts)
    
    def _calculate_sentiment_consistency(self, results: List[EnhancedSentimentResult]) -> float:
        """Calculate how consistent the sentiments are (0-1 scale)"""
        if len(results) <= 1:
            return 1.0
        
        # Count different sentiments
        sentiment_types = set(result.sentiment_label.value for result in results)
        
        # More consistent = fewer different types
        consistency = 1.0 - ((len(sentiment_types) - 1) / 2.0)  # Normalized to 0-1
        return max(0.0, consistency)
    
    def _calculate_confidence_variance(self, results: List[EnhancedSentimentResult]) -> float:
        """Calculate variance in confidence scores"""
        if len(results) <= 1:
            return 0.0
        
        confidences = [result.confidence for result in results]
        return statistics.variance(confidences)
    
    def _calculate_consensus_strength(self, sentiment_counts: Dict[str, int], total_articles: int) -> float:
        """Calculate consensus strength based on sentiment distribution"""
        if total_articles == 0:
            return 0.0
        
        # Calculate normalized entropy (measure of distribution)
        probabilities = [count / total_articles for count in sentiment_counts.values()]
        entropy = -sum(p * (p.bit_length() - 1) for p in probabilities if p > 0)
        
        # Normalize entropy to 0-1 scale (lower entropy = higher consensus)
        max_entropy = (3.0).bit_length() - 1  # Maximum entropy for 3 sentiment types
        normalized_entropy = entropy / max_entropy if max_entropy > 0 else 0
        
        # Consensus strength is inverse of entropy
        return 1.0 - normalized_entropy
    
    def _create_empty_result(self, symbol: str, start_time: datetime) -> AggregatedSentimentResult:
        """Create empty aggregated result for cases with no valid data"""
        processing_time = (datetime.now() - start_time).total_seconds() * 1000
        
        return AggregatedSentimentResult(
            symbol=symbol,
            total_articles=0,
            sentiment_distribution={},
            overall_sentiment=SentimentLabel.NEUTRAL,
            confidence_score=0.0,
            average_positive_score=0.0,
            average_negative_score=0.0,
            average_neutral_score=0.0,
            weighted_market_impact=0.0,
            sentiment_consistency=0.0,
            confidence_variance=0.0,
            processing_time_ms=processing_time,
            timestamp=datetime.now(),
            high_confidence_articles=0,
            articles_processed=[]
        )


def create_sentiment_aggregator(min_confidence_threshold: float = None,
                               high_confidence_threshold: float = None) -> SentimentAggregator:
    """
    Factory function to create sentiment aggregator
    
    Args:
        min_confidence_threshold: Minimum confidence for including results
        high_confidence_threshold: Threshold for high-confidence articles
        
    Returns:
        Configured SentimentAggregator instance
    """
    return SentimentAggregator(
        min_confidence_threshold=min_confidence_threshold,
        high_confidence_threshold=high_confidence_threshold
    )