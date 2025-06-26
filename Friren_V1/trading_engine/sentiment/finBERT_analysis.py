"""
trading_engine/sentiment/finBERT_analysis.py

FinBERT Analysis Utility Tool - Pure Business Logic

Handles FinBERT model loading, text preprocessing, and sentiment analysis.
No process logic - just sentiment analysis utilities.
"""

import time
import logging
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass
import numpy as np
from datetime import datetime


@dataclass
class SentimentResult:
    """Individual article sentiment analysis result"""
    article_id: str
    text: str
    sentiment_score: float  # -1.0 to +1.0 (negative to positive)
    confidence: float      # 0.0 to 1.0
    classification: str    # POSITIVE, NEGATIVE, NEUTRAL
    raw_scores: Dict[str, float]  # Raw model outputs
    processing_time: float
    model_version: str
    is_reliable: bool      # True if confidence > threshold


@dataclass
class BatchSentimentResult:
    """Batch sentiment analysis result"""
    results: List[SentimentResult]
    batch_processing_time: float
    success_count: int
    error_count: int
    average_confidence: float
    sentiment_distribution: Dict[str, int]  # Count by classification


class EnhancedFinBERT:
    """
    Enhanced FinBERT Sentiment Analysis Tool

    Pure utility class for financial sentiment analysis using FinBERT.
    Optimized for trading system integration with proper error handling,
    batching, and performance monitoring.

    Features:
    - FinBERT model loading with fallback options
    - Batch processing for efficiency
    - Text preprocessing and cleaning
    - Confidence scoring and classification
    - Performance monitoring and caching
    - Memory-aware processing for t3.micro
    """

    def __init__(self, model_name: str = "ProsusAI/finbert",
                 max_length: int = 512,
                 batch_size: int = 8,
                 device: str = "cpu"):
        self.model_name = model_name
        self.max_length = max_length
        self.batch_size = batch_size
        self.device = device

        # Model components
        self.model = None
        self.tokenizer = None
        self.initialized = False

        # Performance tracking
        self.total_texts_processed = 0
        self.total_processing_time = 0.0
        self.error_count = 0
        self.cache = {}  # Simple text -> result cache

        # Configuration
        self.enable_caching = True
        self.cache_max_size = 1000
        self.confidence_threshold = 0.5  # Minimum confidence for reliable results

        self.logger = logging.getLogger("enhanced_finbert")
        self.logger.info(f"Enhanced FinBERT initialized - model: {model_name}, device: {device}")

    def initialize(self) -> bool:
        """
        Initialize FinBERT model with proper error handling

        Returns:
            bool: True if successful, False if fallback to mock
        """
        try:
            self.logger.info("Loading FinBERT model...")

            # Import here to avoid loading unless needed
            import torch
            from transformers import AutoTokenizer, AutoModelForSequenceClassification

            # Load tokenizer
            self.tokenizer = AutoTokenizer.from_pretrained(self.model_name)

            # Load model
            self.model = AutoModelForSequenceClassification.from_pretrained(self.model_name)

            # Move to device and set evaluation mode
            self.model.to(self.device)
            self.model.eval()

            self.initialized = True
            self.logger.info(f"FinBERT model loaded successfully on {self.device}")

            # Test with a sample text to ensure everything works
            test_result = self.analyze_text("The market is performing well today.")
            self.logger.info(f"Model test successful: {test_result.classification}")

            return True

        except ImportError as e:
            self.logger.warning(f"Required libraries not available: {e}")
            self.logger.info("Falling back to mock sentiment analysis")
            self.initialized = False
            return False

        except Exception as e:
            self.logger.error(f"Failed to initialize FinBERT model: {e}")
            self.logger.info("Falling back to mock sentiment analysis")
            self.initialized = False
            return False

    def analyze_text(self, text: str, article_id: str = None) -> SentimentResult:
        """
        Analyze sentiment of a single text

        Args:
            text: Text to analyze
            article_id: Optional identifier for the text

        Returns:
            SentimentResult with sentiment analysis
        """
        start_time = time.time()

        # Generate article ID if not provided
        if article_id is None:
            article_id = f"text_{hash(text) % 10000}"

        # Check cache first
        if self.enable_caching and text in self.cache:
            cached_result = self.cache[text]
            self.logger.debug(f"Cache hit for article {article_id}")

            # Update article_id and return cached result
            return SentimentResult(
                article_id=article_id,
                text=text,
                sentiment_score=cached_result['sentiment_score'],
                confidence=cached_result['confidence'],
                classification=cached_result['classification'],
                raw_scores=cached_result['raw_scores'],
                processing_time=0.001,  # Cache access time
                model_version=self.model_name,
                is_reliable=cached_result['is_reliable']
            )

        try:
            if self.initialized:
                result = self._analyze_with_finbert(text, article_id)
            else:
                result = self._analyze_with_mock(text, article_id)

            # Cache the result
            if self.enable_caching:
                self._cache_result(text, result)

            # Update statistics
            processing_time = time.time() - start_time
            self.total_texts_processed += 1
            self.total_processing_time += processing_time

            return result

        except Exception as e:
            self.logger.error(f"Error analyzing text: {e}")
            self.error_count += 1

            # Return fallback result
            return self._create_fallback_result(text, article_id, time.time() - start_time)

    def analyze_batch(self, texts: List[str],
                      article_ids: Optional[List[str]] = None) -> BatchSentimentResult:
        """
        Analyze sentiment of multiple texts in batch for efficiency

        Args:
            texts: List of texts to analyze
            article_ids: Optional list of identifiers for each text

        Returns:
            BatchSentimentResult with all analysis results
        """
        start_time = time.time()

        if not texts:
            return BatchSentimentResult(
                results=[],
                batch_processing_time=0.0,
                success_count=0,
                error_count=0,
                average_confidence=0.0,
                sentiment_distribution={'POSITIVE': 0, 'NEGATIVE': 0, 'NEUTRAL': 0}
            )

        # Generate article IDs if not provided
        if article_ids is None:
            article_ids = [f"batch_{i}_{hash(text) % 10000}" for i, text in enumerate(texts)]

        self.logger.info(f"Analyzing batch of {len(texts)} texts")

        try:
            if self.initialized and len(texts) > 1:
                # Use efficient batch processing
                results = self._analyze_batch_with_finbert(texts, article_ids)
            else:
                # Process individually (for mock or single items)
                results = []
                for text, article_id in zip(texts, article_ids):
                    result = self.analyze_text(text, article_id)
                    results.append(result)

            # Calculate batch statistics
            batch_time = time.time() - start_time
            success_count = len([r for r in results if r.sentiment_score is not None])
            error_count = len(results) - success_count

            confidences = [r.confidence for r in results if r.confidence is not None]
            avg_confidence = np.mean(confidences) if confidences else 0.0

            # Calculate sentiment distribution
            distribution = {'POSITIVE': 0, 'NEGATIVE': 0, 'NEUTRAL': 0}
            for result in results:
                if result.classification in distribution:
                    distribution[result.classification] += 1

            self.logger.info(f"Batch analysis complete - {batch_time:.2f}s, "
                           f"{success_count}/{len(texts)} successful")

            return BatchSentimentResult(
                results=results,
                batch_processing_time=batch_time,
                success_count=success_count,
                error_count=error_count,
                average_confidence=avg_confidence,
                sentiment_distribution=distribution
            )

        except Exception as e:
            self.logger.error(f"Error in batch analysis: {e}")
            self.error_count += len(texts)

            # Return fallback results
            fallback_results = []
            for text, article_id in zip(texts, article_ids):
                fallback_results.append(self._create_fallback_result(text, article_id, 0.0))

            return BatchSentimentResult(
                results=fallback_results,
                batch_processing_time=time.time() - start_time,
                success_count=0,
                error_count=len(texts),
                average_confidence=0.0,
                sentiment_distribution={'POSITIVE': 0, 'NEGATIVE': 0, 'NEUTRAL': len(texts)}
            )

    def _analyze_with_finbert(self, text: str, article_id: str) -> SentimentResult:
        """Analyze text using actual FinBERT model"""
        import torch

        start_time = time.time()

        # Clean and prepare text
        cleaned_text = self._clean_text(text)

        # Tokenize
        inputs = self.tokenizer(
            cleaned_text,
            return_tensors="pt",
            truncation=True,
            padding=True,
            max_length=self.max_length
        )

        # Move to device
        inputs = {k: v.to(self.device) for k, v in inputs.items()}

        # Run inference
        with torch.no_grad():
            outputs = self.model(**inputs)
            predictions = torch.nn.functional.softmax(outputs.logits, dim=-1)

        # Calculate processing time
        processing_time = time.time() - start_time

        # Extract scores
        scores = predictions[0].cpu().numpy()

        # FinBERT outputs: [negative, neutral, positive]
        negative_score = float(scores[0])
        neutral_score = float(scores[1])
        positive_score = float(scores[2])

        # Calculate overall sentiment (-1 to +1)
        sentiment_score = positive_score - negative_score

        # Classification based on highest score
        max_idx = np.argmax(scores)
        classifications = ['NEGATIVE', 'NEUTRAL', 'POSITIVE']
        classification = classifications[max_idx]

        # Confidence is the max probability
        confidence = float(np.max(scores))

        # Check reliability based on confidence threshold
        is_reliable = confidence >= self.confidence_threshold

        # Raw scores for debugging/analysis
        raw_scores = {
            'negative': negative_score,
            'neutral': neutral_score,
            'positive': positive_score,
            'max_score': confidence,
            'prediction_index': int(max_idx)
        }

        return SentimentResult(
            article_id=article_id,
            text=text,
            sentiment_score=sentiment_score,
            confidence=confidence,
            classification=classification,
            raw_scores=raw_scores,
            processing_time=processing_time,
            model_version=self.model_name,
            is_reliable=is_reliable
        )

    def _analyze_batch_with_finbert(self, texts: List[str], article_ids: List[str]) -> List[SentimentResult]:
        """Efficiently analyze multiple texts using FinBERT"""
        import torch

        start_time = time.time()

        # Clean all texts
        cleaned_texts = [self._clean_text(text) for text in texts]

        # Tokenize batch
        inputs = self.tokenizer(
            cleaned_texts,
            return_tensors="pt",
            truncation=True,
            padding=True,
            max_length=self.max_length
        )

        # Move to device
        inputs = {k: v.to(self.device) for k, v in inputs.items()}

        # Run batch inference
        with torch.no_grad():
            outputs = self.model(**inputs)
            predictions = torch.nn.functional.softmax(outputs.logits, dim=-1)

        # Calculate batch processing time
        batch_processing_time = time.time() - start_time
        individual_processing_time = batch_processing_time / len(texts)

        # Process results
        results = []
        for i, (text, article_id) in enumerate(zip(texts, article_ids)):
            scores = predictions[i].cpu().numpy()

            # FinBERT outputs: [negative, neutral, positive]
            negative_score = float(scores[0])
            neutral_score = float(scores[1])
            positive_score = float(scores[2])

            # Calculate overall sentiment (-1 to +1)
            sentiment_score = positive_score - negative_score

            # Classification based on highest score
            max_idx = np.argmax(scores)
            classifications = ['NEGATIVE', 'NEUTRAL', 'POSITIVE']
            classification = classifications[max_idx]

            # Confidence is the max probability
            confidence = float(np.max(scores))

            # Check reliability based on confidence threshold
            is_reliable = confidence >= self.confidence_threshold

            # Raw scores
            raw_scores = {
                'negative': negative_score,
                'neutral': neutral_score,
                'positive': positive_score,
                'max_score': confidence,
                'prediction_index': int(max_idx)
            }

            result = SentimentResult(
                article_id=article_id,
                text=text,
                sentiment_score=sentiment_score,
                confidence=confidence,
                classification=classification,
                raw_scores=raw_scores,
                processing_time=individual_processing_time,
                model_version=self.model_name,
                is_reliable=is_reliable
            )

            results.append(result)

        return results

    def _analyze_with_mock(self, text: str, article_id: str) -> SentimentResult:
        """Mock sentiment analysis for development/fallback"""
        import random

        start_time = time.time()

        # Simple keyword-based sentiment for more realistic mock results
        text_lower = text.lower()

        positive_keywords = ['good', 'great', 'excellent', 'strong', 'up', 'gain', 'profit', 'bull', 'buy',
                           'growth', 'beat', 'exceeds', 'outperform', 'surge', 'rally', 'positive']
        negative_keywords = ['bad', 'poor', 'weak', 'down', 'loss', 'bear', 'sell', 'crash', 'decline',
                           'drop', 'fall', 'disappointing', 'miss', 'concern', 'risk', 'negative']

        positive_count = sum(1 for word in positive_keywords if word in text_lower)
        negative_count = sum(1 for word in negative_keywords if word in text_lower)

        # Base sentiment on keyword analysis with some randomness
        if positive_count > negative_count:
            base_sentiment = 0.3 + random.uniform(0.0, 0.5)
            classification = 'POSITIVE'
        elif negative_count > positive_count:
            base_sentiment = -0.3 - random.uniform(0.0, 0.5)
            classification = 'NEGATIVE'
        else:
            base_sentiment = random.uniform(-0.2, 0.2)
            classification = 'NEUTRAL'

        # Add some noise but keep realistic
        sentiment_score = base_sentiment + random.uniform(-0.1, 0.1)
        sentiment_score = max(-1.0, min(1.0, sentiment_score))

        # Generate realistic confidence based on keyword strength
        keyword_strength = abs(positive_count - negative_count)
        base_confidence = 0.5 + (keyword_strength * 0.1)
        confidence = min(0.95, base_confidence + random.uniform(-0.1, 0.1))

        # Check reliability
        is_reliable = confidence >= self.confidence_threshold

        # Create mock raw scores that are consistent with classification
        if classification == 'POSITIVE':
            positive_raw = 0.5 + abs(sentiment_score) / 2
            negative_raw = 0.5 - abs(sentiment_score) / 2
        elif classification == 'NEGATIVE':
            positive_raw = 0.5 - abs(sentiment_score) / 2
            negative_raw = 0.5 + abs(sentiment_score) / 2
        else:
            positive_raw = 0.4 + random.uniform(-0.1, 0.1)
            negative_raw = 0.4 + random.uniform(-0.1, 0.1)

        neutral_raw = max(0.05, 1.0 - positive_raw - negative_raw)

        # Normalize to ensure they sum to 1
        total = positive_raw + negative_raw + neutral_raw
        positive_raw /= total
        negative_raw /= total
        neutral_raw /= total

        raw_scores = {
            'negative': negative_raw,
            'neutral': neutral_raw,
            'positive': positive_raw,
            'max_score': confidence,
            'prediction_index': 0 if classification == 'NEGATIVE' else (1 if classification == 'NEUTRAL' else 2)
        }

        # Simulate realistic processing time
        processing_time = time.time() - start_time

        return SentimentResult(
            article_id=article_id,
            text=text,
            sentiment_score=sentiment_score,
            confidence=confidence,
            classification=classification,
            raw_scores=raw_scores,
            processing_time=processing_time,
            model_version="mock_finbert_v1.0",
            is_reliable=is_reliable
        )

    def _clean_text(self, text: str) -> str:
        """Clean and prepare text for analysis"""
        if not text:
            return ""

        # Remove excessive whitespace
        text = " ".join(text.split())

        # Remove very short texts
        if len(text.strip()) < 10:
            return text

        # Truncate if too long (leave room for tokenization)
        max_chars = self.max_length * 3  # Rough estimate: ~3 chars per token
        if len(text) > max_chars:
            text = text[:max_chars] + "..."

        return text.strip()

    def _cache_result(self, text: str, result: SentimentResult):
        """Cache result for future use"""
        if len(self.cache) >= self.cache_max_size:
            # Simple cache eviction - remove oldest entries
            keys_to_remove = list(self.cache.keys())[:self.cache_max_size // 4]
            for key in keys_to_remove:
                del self.cache[key]

        self.cache[text] = {
            'sentiment_score': result.sentiment_score,
            'confidence': result.confidence,
            'classification': result.classification,
            'raw_scores': result.raw_scores,
            'is_reliable': result.is_reliable
        }

    def _create_fallback_result(self, text: str, article_id: str, processing_time: float) -> SentimentResult:
        """Create neutral fallback result for errors"""
        return SentimentResult(
            article_id=article_id,
            text=text,
            sentiment_score=0.0,
            confidence=0.0,
            classification='NEUTRAL',
            raw_scores={'negative': 0.33, 'neutral': 0.34, 'positive': 0.33, 'max_score': 0.34, 'prediction_index': 1},
            processing_time=processing_time,
            model_version="fallback",
            is_reliable=False  # Fallback results are never reliable
        )

    def get_statistics(self) -> Dict[str, Any]:
        """Get performance and usage statistics"""
        avg_processing_time = (self.total_processing_time / self.total_texts_processed
                             if self.total_texts_processed > 0 else 0.0)

        return {
            'initialized': self.initialized,
            'model_name': self.model_name,
            'device': self.device,
            'total_texts_processed': self.total_texts_processed,
            'total_processing_time': self.total_processing_time,
            'average_processing_time': avg_processing_time,
            'error_count': self.error_count,
            'error_rate': self.error_count / max(1, self.total_texts_processed),
            'cache_size': len(self.cache),
            'cache_enabled': self.enable_caching,
            'confidence_threshold': self.confidence_threshold,
            'reliability_enforcement': True,  # New feature indicator
            'processing_time_tracking': True  # New feature indicator
        }

    def clear_cache(self):
        """Clear the result cache"""
        self.cache.clear()
        self.logger.info("FinBERT cache cleared")

    def set_confidence_threshold(self, threshold: float):
        """Set minimum confidence threshold for reliable results"""
        self.confidence_threshold = max(0.0, min(1.0, threshold))
        self.logger.info(f"Confidence threshold set to {self.confidence_threshold}")


# Example usage and testing
if __name__ == "__main__":
    # Test the FinBERT analyzer
    finbert = EnhancedFinBERT()

    # Initialize (will fall back to mock if FinBERT not available)
    success = finbert.initialize()
    print(f"FinBERT initialization: {'Success' if success else 'Mock mode'}")

    # Test single text analysis
    test_text = "Apple reported strong quarterly earnings, beating analyst expectations with robust iPhone sales driving revenue growth."
    result = finbert.analyze_text(test_text)

    print(f"\nSingle Text Analysis:")
    print(f"Text: {test_text[:100]}...")
    print(f"Sentiment: {result.classification} (score: {result.sentiment_score:.3f})")
    print(f"Confidence: {result.confidence:.3f}")
    print(f"Processing time: {result.processing_time:.4f}s")

    # Test batch analysis
    test_texts = [
        "Tesla stock surges on strong delivery numbers and positive guidance",
        "Market volatility continues amid economic uncertainty and inflation concerns",
        "Microsoft announces disappointing quarterly results missing revenue expectations",
        "Federal Reserve signals potential interest rate cuts supporting market sentiment"
    ]

    batch_result = finbert.analyze_batch(test_texts)

    print(f"\nBatch Analysis ({len(test_texts)} texts):")
    print(f"Processing time: {batch_result.batch_processing_time:.3f}s")
    print(f"Success rate: {batch_result.success_count}/{len(test_texts)}")
    print(f"Average confidence: {batch_result.average_confidence:.3f}")
    print(f"Distribution: {batch_result.sentiment_distribution}")

    for i, result in enumerate(batch_result.results):
        print(f"  {i+1}. {result.classification} ({result.sentiment_score:+.3f}) "
              f"[{result.confidence:.3f}] {'' if result.is_reliable else ''} - {test_texts[i][:60]}...")

    # Show statistics
    stats = finbert.get_statistics()
    print(f"\nStatistics:")
    for key, value in stats.items():
        if isinstance(value, float):
            print(f"  {key}: {value:.4f}")
        else:
            print(f"  {key}: {value}")

    # Test confidence threshold
    print(f"\nConfidence Threshold Analysis:")
    reliable_count = sum(1 for r in batch_result.results if r.is_reliable)
    print(f"Reliable results: {reliable_count}/{len(batch_result.results)} "
          f"(threshold: {finbert.confidence_threshold:.2f})")

    # Show processing time breakdown
    total_time = sum(r.processing_time for r in batch_result.results)
    print(f"Total processing time: {total_time:.4f}s")
    print(f"Average per text: {total_time/len(batch_result.results):.4f}s")
